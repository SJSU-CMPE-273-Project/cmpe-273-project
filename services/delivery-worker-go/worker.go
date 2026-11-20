// Package delivery implements a durable, serial notification mailbox per account.
package delivery

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sort"
	"time"

	pb "github.com/SJSU-CMPE-273-Project/cmpe-273-project/gen/go/relayguard/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	TaskQueue          = "delivery"
	SignalName         = "notification"
	AttemptActivity    = "delivery.attempt"
	DeadLetterActivity = "delivery.dead-letter"
	ExpireActivity     = "delivery.expire"
	LoadReplayActivity = "delivery.load-replay"
)

// Mailbox survives ContinueAsNew. Sequence is a high-water mark, not a count:
// producer sequences may have gaps after ingest rejection.
type Mailbox struct {
	AccountID    string
	LastSequence uint64
	Pending      []*pb.NotificationRequested
	Processed    int
}

type Signaler interface {
	SignalWithStartWorkflow(context.Context, string, string, interface{}, client.StartWorkflowOptions, interface{}, ...interface{}) (client.WorkflowRun, error)
}

// Submit returns only after Temporal has durably accepted the signal. Callers
// must not acknowledge Kafka before this succeeds.
func Submit(ctx context.Context, c Signaler, event *pb.NotificationRequested) error {
	if event.GetAccountId() == "" || event.GetEventId() == "" || event.GetSequence() == 0 {
		return fmt.Errorf("invalid_delivery_identity")
	}
	_, err := c.SignalWithStartWorkflow(ctx, event.AccountId, SignalName, event,
		client.StartWorkflowOptions{ID: event.AccountId, TaskQueue: TaskQueue}, AccountWorkflow, Mailbox{AccountID: event.AccountId})
	return err
}

func AccountWorkflow(ctx workflow.Context, state Mailbox) error {
	signals := workflow.GetSignalChannel(ctx, SignalName)
	enqueue := func(e *pb.NotificationRequested) {
		if e == nil || e.AccountId != state.AccountID || e.Sequence <= state.LastSequence {
			return
		}
		for _, p := range state.Pending {
			if p.EventId == e.EventId || p.Sequence == e.Sequence {
				return
			}
		}
		state.Pending = append(state.Pending, e)
	}
	drain := func() {
		for {
			var e *pb.NotificationRequested
			if !signals.ReceiveAsync(&e) {
				break
			}
			enqueue(e)
		}
		sort.SliceStable(state.Pending, func(i, j int) bool { return state.Pending[i].Sequence < state.Pending[j].Sequence })
	}
	for {
		drain()
		if state.Processed >= 1000 || workflow.GetInfo(ctx).GetContinueAsNewSuggested() {
			state.Processed = 0
			return workflow.NewContinueAsNewError(ctx, AccountWorkflow, state)
		}
		if len(state.Pending) == 0 {
			var e *pb.NotificationRequested
			signals.Receive(ctx, &e)
			enqueue(e)
			continue
		}
		e := state.Pending[0]
		if err := deliver(ctx, e); err != nil {
			return err
		}
		state.LastSequence = e.Sequence
		state.Pending = state.Pending[1:]
		state.Processed++
	}
}

type Target struct {
	Provider string
	Channel  pb.Channel
}

func targets(e *pb.NotificationRequested) []Target {
	var result []Target
	for _, ch := range e.Channels {
		switch ch {
		case pb.Channel_CHANNEL_SMS:
			result = append(result, Target{"sms_primary", ch}, Target{"sms_secondary", ch})
		case pb.Channel_CHANNEL_PUSH:
			result = append(result, Target{"push", ch})
		case pb.Channel_CHANNEL_EMAIL:
			result = append(result, Target{"email", ch})
		}
	}
	return result
}

type Attempt struct {
	Event  *pb.NotificationRequested
	Target Target
}
type Result struct {
	Accepted    bool
	Invalid     bool
	Expired     bool
	Ambiguous   bool
	Retryable   bool
	CircuitOpen bool
	RateLimited bool
	RetryAfter  time.Duration
}

func deliver(ctx workflow.Context, e *pb.NotificationRequested) error {
	persistExpiry := workflow.GetVersion(ctx, "delivery-persist-expiry", workflow.DefaultVersion, 1) != workflow.DefaultVersion
	jitter := workflow.GetVersion(ctx, "delivery-retry-jitter", workflow.DefaultVersion, 1) != workflow.DefaultVersion
	throttleWithoutPenalty := workflow.GetVersion(ctx, "delivery-rate-limit", workflow.DefaultVersion, 1) != workflow.DefaultVersion
	// HTTP retries are controlled here so timers, TTL, and failover are durable.
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 20 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{InitialInterval: time.Second, MaximumInterval: 30 * time.Second},
	})
	reason := "channels_exhausted"
	for _, target := range targets(e) {
		uncertain := false
		failures := 0
		for attempt := 1; ; attempt++ {
			if e.ExpiresAt != nil && !workflow.Now(ctx).Before(e.ExpiresAt.AsTime()) {
				if persistExpiry {
					return workflow.ExecuteActivity(ctx, ExpireActivity, e).Get(ctx, nil)
				}
				return nil
			}
			var r Result
			callCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				ActivityID:          fmt.Sprintf("%s/%s/%d", e.EventId, target.Provider, attempt),
				StartToCloseTimeout: 20 * time.Second,
				// A worker crash may happen after provider acceptance: retry the same
				// target and event ID, never automatically fail over on an activity error.
				RetryPolicy: &temporal.RetryPolicy{InitialInterval: time.Second, MaximumInterval: 30 * time.Second},
			})
			if err := workflow.ExecuteActivity(callCtx, AttemptActivity, Attempt{e, target}).Get(ctx, &r); err != nil {
				return err
			}
			if r.Accepted || r.Expired {
				return nil
			}
			uncertain = uncertain || r.Ambiguous
			throttled := throttleWithoutPenalty && r.RateLimited
			if !throttled {
				failures++
			}
			if !uncertain && r.Invalid {
				reason = "invalid_request"
				break
			}
			if !uncertain && !throttled && (!r.Retryable || failures >= 5) {
				break
			}
			// Request ordinals remain unique for activity IDs, while only
			// failures consume the retry budget or increase backoff.
			delay := time.Second << min(max(failures-1, 0), 5)
			if throttled {
				delay = r.RetryAfter
			} else if jitter {
				// Record randomness in history so replay uses the exact same timer.
				if err := workflow.SideEffect(ctx, func(workflow.Context) interface{} {
					return delay/2 + time.Duration(rand.Int64N(int64(delay/2)+1))
				}).Get(&delay); err != nil {
					return err
				}
			}
			if r.RetryAfter > delay {
				delay = r.RetryAfter
			}
			if e.ExpiresAt != nil {
				remaining := e.ExpiresAt.AsTime().Sub(workflow.Now(ctx))
				if delay > remaining {
					delay = remaining
				}
			}
			if err := workflow.Sleep(ctx, delay); err != nil {
				return err
			}
		}
		if reason == "invalid_request" {
			break
		}
	}
	return workflow.ExecuteActivity(ctx, DeadLetterActivity, e, reason).Get(ctx, nil)
}

// ReplayWorkflow bypasses the account high-water mark only for an audited DLQ
// event. Its stable workflow ID and RejectDuplicate policy allow one operator
// replay, including concurrent submissions and requests after completion.
func ReplayWorkflow(ctx workflow.Context, eventID, operator string) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 20 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{InitialInterval: time.Second, MaximumInterval: 30 * time.Second},
	})
	var e *pb.NotificationRequested
	if err := workflow.ExecuteActivity(ctx, LoadReplayActivity, eventID, operator).Get(ctx, &e); err != nil {
		return err
	}
	return deliver(ctx, e)
}
