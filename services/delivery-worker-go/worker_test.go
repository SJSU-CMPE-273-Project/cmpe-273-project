package delivery

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	pb "github.com/SJSU-CMPE-273-Project/cmpe-273-project/gen/go/relayguard/v1"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func event(seq uint64) *pb.NotificationRequested {
	return &pb.NotificationRequested{EventId: fmt.Sprintf("event-%d", seq), AccountId: "account-a", Sequence: seq, Recipient: "+14085550123", Body: "test", Channels: []pb.Channel{pb.Channel_CHANNEL_SMS, pb.Channel_CHANNEL_PUSH}, Traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", OccurredAt: timestamppb.Now()}
}

func TestAdapterRoutesAndHeaders(t *testing.T) {
	for provider, endpoint := range proxyURLs {
		t.Run(provider, func(t *testing.T) {
			e := event(42)
			a := Adapters{Client: &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
				require.Equal(t, endpoint, r.URL.String())
				require.Equal(t, "POST", r.Method)
				require.Equal(t, e.EventId, r.Header.Get("Idempotency-Key"))
				require.Equal(t, e.Traceparent, r.Header.Get("traceparent"))
				var body map[string]any
				require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
				require.Equal(t, e.EventId, body["event_id"])
				require.Equal(t, e.AccountId, body["account_id"])
				require.Equal(t, float64(42), body["sequence"])
				require.Equal(t, e.Body, body["body"])
				require.Equal(t, e.Recipient, body["recipient"])
				require.NotEmpty(t, body["occurred_at"])
				if provider == "email" || provider == "push" {
					require.Equal(t, provider, body["channel"])
				}
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(fmt.Sprintf(`{"event_id":%q,"status":"accepted","provider_message_id":"p-1"}`, e.EventId)))}, nil
			})}}
			r, err := a.Send(context.Background(), Attempt{e, Target{Provider: provider}})
			require.NoError(t, err)
			require.True(t, r.Accepted)
		})
	}
}

func TestAdapterResponses(t *testing.T) {
	for _, tc := range []struct {
		code int
		body string
		want Result
	}{
		{400, `{"event_id":"event-1","status":"invalid"}`, Result{Invalid: true}},
		{422, `{"event_id":"event-1","status":"rejected"}`, Result{}},
		{429, `{"event_id":"event-1","status":"busy","retryable":true}`, Result{Retryable: true, RateLimited: true, RetryAfter: 7 * time.Second}},
		{503, `{"event_id":"event-1","status":"unavailable","retryable":true}`, Result{Retryable: true}},
		{504, `{"event_id":"event-1","status":"timeout","retryable":true}`, Result{Retryable: true, Ambiguous: true}},
		{200, `garbage`, Result{Retryable: true, Ambiguous: true}},
		{200, `{"event_id":"wrong","status":"accepted"}`, Result{Retryable: true, Ambiguous: true}},
	} {
		t.Run(fmt.Sprint(tc.code, tc.body), func(t *testing.T) {
			a := Adapters{Client: &http.Client{Transport: transport(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: tc.code, Header: http.Header{"Retry-After": []string{"7"}}, Body: io.NopCloser(strings.NewReader(tc.body))}, nil
			})}}
			r, err := a.Send(context.Background(), Attempt{event(1), Target{Provider: "sms_primary"}})
			require.NoError(t, err)
			require.Equal(t, tc.want, r)
		})
	}
	a := Adapters{Client: &http.Client{Transport: transport(func(*http.Request) (*http.Response, error) { return nil, fmt.Errorf("connection reset") })}}
	r, err := a.Send(context.Background(), Attempt{event(1), Target{Provider: "sms_primary"}})
	require.NoError(t, err)
	require.True(t, r.Ambiguous)
	e := event(1)
	e.ExpiresAt = timestamppb.New(time.Now().Add(-time.Second))
	r, err = a.Send(context.Background(), Attempt{e, Target{Provider: "sms_primary"}})
	require.NoError(t, err)
	require.True(t, r.Expired)
}

func newEnv(t *testing.T) *testsuite.TestWorkflowEnvironment {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivityWithOptions(func(context.Context, Attempt) (Result, error) { return Result{}, nil }, activity.RegisterOptions{Name: AttemptActivity})
	env.RegisterActivityWithOptions(func(context.Context, *pb.NotificationRequested, string) error { return nil }, activity.RegisterOptions{Name: DeadLetterActivity})
	return env
}

func TestMailboxOrderingDedupAndContinueAsNew(t *testing.T) {
	env := newEnv(t)
	var sent []uint64
	env.OnActivity(AttemptActivity, mock.Anything, mock.Anything).Return(func(_ context.Context, a Attempt) (Result, error) {
		sent = append(sent, a.Event.Sequence)
		return Result{Accepted: true}, nil
	})
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalName, event(2))
		env.SignalWorkflow(SignalName, event(1))
		env.SignalWorkflow(SignalName, event(1))
	}, 0)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(SignalName, event(1)); env.SignalWorkflow(SignalName, event(3)) }, time.Second)
	// A successful event triggers continuation; delayed signals arriving during
	// activities are drained into the carried mailbox first.
	env.ExecuteWorkflow(AccountWorkflow, Mailbox{AccountID: "account-a", Processed: 998})
	var next *workflow.ContinueAsNewError
	require.ErrorAs(t, env.GetWorkflowError(), &next)
	require.Equal(t, []uint64{1, 2}, sent)
	var carried Mailbox
	require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(next.Input, &carried))
	require.Equal(t, uint64(2), carried.LastSequence)
	// A replay after continuation uses the saved high-water mark.
	env = newEnv(t)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(SignalName, event(1)); env.SignalWorkflow(SignalName, event(3)) }, 0)
	env.OnActivity(AttemptActivity, mock.Anything, mock.Anything).Return(func(_ context.Context, a Attempt) (Result, error) {
		require.Equal(t, uint64(3), a.Event.Sequence)
		return Result{Accepted: true}, nil
	})
	env.ExecuteWorkflow(AccountWorkflow, Mailbox{AccountID: "account-a", LastSequence: 2, Processed: 999})
	require.ErrorAs(t, env.GetWorkflowError(), &next)
	env.AssertExpectations(t)
}

func TestFailoverAndAmbiguousRetry(t *testing.T) {
	for _, ambiguous := range []bool{false, true} {
		t.Run(fmt.Sprint(ambiguous), func(t *testing.T) {
			env := newEnv(t)
			var providers []string
			env.OnActivity(AttemptActivity, mock.Anything, mock.Anything).Return(func(_ context.Context, a Attempt) (Result, error) {
				providers = append(providers, a.Target.Provider)
				if len(providers) == 1 {
					return Result{Ambiguous: ambiguous, Retryable: ambiguous}, nil
				}
				return Result{Accepted: true}, nil
			})
			env.RegisterDelayedCallback(func() { env.SignalWorkflow(SignalName, event(1)) }, 0)
			env.ExecuteWorkflow(AccountWorkflow, Mailbox{AccountID: "account-a", Processed: 999})
			var next *workflow.ContinueAsNewError
			require.ErrorAs(t, env.GetWorkflowError(), &next)
			expected := "sms_secondary"
			if ambiguous {
				expected = "sms_primary"
			}
			require.Equal(t, []string{"sms_primary", expected}, providers)
		})
	}
}

func TestInvalidRequestDeadLettersWithoutFailover(t *testing.T) {
	env := newEnv(t)
	env.OnActivity(AttemptActivity, mock.Anything, mock.Anything).Return(Result{Invalid: true}, nil).Once()
	env.OnActivity(DeadLetterActivity, mock.Anything, mock.Anything, "invalid_request").Return(nil).Once()
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(SignalName, event(1)) }, 0)
	env.ExecuteWorkflow(AccountWorkflow, Mailbox{AccountID: "account-a", Processed: 999})
	var next *workflow.ContinueAsNewError
	require.ErrorAs(t, env.GetWorkflowError(), &next)
	env.AssertExpectations(t)
}

type fakeSignaler struct{ ids []string }

func (f *fakeSignaler) SignalWithStartWorkflow(_ context.Context, id, name string, arg interface{}, options client.StartWorkflowOptions, w interface{}, args ...interface{}) (client.WorkflowRun, error) {
	f.ids = append(f.ids, id)
	if name != SignalName || options.TaskQueue != TaskQueue {
		return nil, fmt.Errorf("wrong routing")
	}
	return nil, nil
}
func TestSubmitUsesAccountIdentity(t *testing.T) {
	f := new(fakeSignaler)
	e := event(1)
	require.NoError(t, Submit(context.Background(), f, e))
	e = event(2)
	require.NoError(t, Submit(context.Background(), f, e))
	e.AccountId = "account-b"
	require.NoError(t, Submit(context.Background(), f, e))
	require.Equal(t, []string{"account-a", "account-a", "account-b"}, f.ids)
}

func TestDuplicateDuringSend(t *testing.T) {
	env := newEnv(t)
	e := event(1)
	env.OnActivity(AttemptActivity, mock.Anything, Attempt{e, Target{"sms_primary", pb.Channel_CHANNEL_SMS}}).After(2*time.Second).Return(Result{Accepted: true}, nil).Once()
	env.OnActivity(AttemptActivity, mock.Anything, mock.MatchedBy(func(a Attempt) bool { return a.Event.Sequence == 2 })).Return(Result{Accepted: true}, nil).Once()
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(SignalName, e) }, 0)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(SignalName, e); env.SignalWorkflow(SignalName, event(2)) }, time.Second)
	env.ExecuteWorkflow(AccountWorkflow, Mailbox{AccountID: "account-a", Processed: 998})
	var next *workflow.ContinueAsNewError
	require.ErrorAs(t, env.GetWorkflowError(), &next)
	env.AssertExpectations(t)
}

func TestExhaustionAndBackpressure(t *testing.T) {
	env := newEnv(t)
	var calls int
	start := env.Now()
	env.OnActivity(AttemptActivity, mock.Anything, mock.Anything).Return(func(_ context.Context, a Attempt) (Result, error) {
		calls++
		return Result{Retryable: true, RetryAfter: 7 * time.Second}, nil
	})
	env.OnActivity(DeadLetterActivity, mock.Anything, mock.Anything, "channels_exhausted").Return(nil).Once()
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(SignalName, event(1)) }, 0)
	env.ExecuteWorkflow(AccountWorkflow, Mailbox{AccountID: "account-a", Processed: 999})
	var next *workflow.ContinueAsNewError
	require.ErrorAs(t, env.GetWorkflowError(), &next)
	require.Equal(t, 15, calls)
	require.GreaterOrEqual(t, env.Now().Sub(start), 84*time.Second)
	env.AssertExpectations(t)
}

func TestAccountsRunInParallel(t *testing.T) {
	env := newEnv(t)
	env.RegisterWorkflow(AccountWorkflow)
	slow := event(1)
	fast := event(1)
	fast.AccountId = "account-b"
	env.OnActivity(AttemptActivity, mock.Anything, mock.MatchedBy(func(a Attempt) bool { return a.Event.AccountId == "account-a" })).After(time.Minute).Return(Result{Accepted: true}, nil).Once()
	env.OnActivity(AttemptActivity, mock.Anything, mock.MatchedBy(func(a Attempt) bool { return a.Event.AccountId == "account-b" })).Return(Result{Accepted: true}, nil).Once()
	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		for _, e := range []*pb.NotificationRequested{slow, fast} {
			child := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{WorkflowID: e.AccountId}), AccountWorkflow, Mailbox{AccountID: e.AccountId})
			if err := child.SignalChildWorkflow(ctx, SignalName, e).Get(ctx, nil); err != nil {
				return err
			}
		}
		return workflow.Sleep(ctx, 2*time.Second)
	})
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}
