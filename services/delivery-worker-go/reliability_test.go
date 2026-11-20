package delivery

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	pb "github.com/SJSU-CMPE-273-Project/cmpe-273-project/gen/go/relayguard/v1"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestProviderCircuitLifecycle(t *testing.T) {
	var b providerBreakers
	now := time.Now()
	for i := 0; i < breakerThreshold; i++ {
		allowed, generation, _ := b.allow("sms_primary", now)
		require.True(t, allowed)
		b.record("sms_primary", generation, true, now)
	}
	allowed, _, wait := b.allow("sms_primary", now)
	require.False(t, allowed)
	require.Equal(t, breakerCooldown, wait)
	allowed, _, _ = b.allow("sms_secondary", now)
	require.True(t, allowed, "circuits must be independent")
	now = now.Add(breakerCooldown)
	allowed, generation, _ := b.allow("sms_primary", now)
	require.True(t, allowed, "one half-open probe")
	allowed, _, _ = b.allow("sms_primary", now)
	require.False(t, allowed, "concurrent probes must be blocked")
	b.record("sms_primary", generation, true, now)
	allowed, _, _ = b.allow("sms_primary", now)
	require.False(t, allowed, "failed probe reopens circuit")
	now = now.Add(breakerCooldown)
	allowed, generation, _ = b.allow("sms_primary", now)
	require.True(t, allowed)
	b.record("sms_primary", generation, false, now)
	allowed, _, _ = b.allow("sms_primary", now)
	require.True(t, allowed, "successful probe closes circuit")
	b.record("sms_primary", generation, true, now) // Stale probe result.
	require.Equal(t, 0, b.states["sms_primary"].failures)
}

func TestAdapterStatusWithoutJSONAndDialFailure(t *testing.T) {
	for _, code := range []int{422, 503, 504} {
		a := &Adapters{Client: &http.Client{Transport: transport(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader("upstream error"))}, nil
		})}}
		r, err := a.Send(context.Background(), Attempt{event(1), Target{Provider: "sms_primary"}})
		require.NoError(t, err)
		require.Equal(t, code != 422, r.Retryable)
		require.Equal(t, code == 504, r.Ambiguous)
	}
	a := &Adapters{Client: &http.Client{Transport: transport(func(*http.Request) (*http.Response, error) {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: fmt.Errorf("connection refused")}
	})}}
	r, err := a.Send(context.Background(), Attempt{event(1), Target{Provider: "sms_primary"}})
	require.NoError(t, err)
	require.True(t, r.Retryable)
	require.False(t, r.Ambiguous)
}

// T6 exercises the real adapter and circuit through durable workflow timers.
// No external services or wall-clock backoff are needed.
func TestT6ProviderOutageFailover(t *testing.T) {
	for _, recoverAt := range []string{"sms_secondary", "push", "email"} {
		t.Run(recoverAt, func(t *testing.T) {
			env := newEnv(t)
			calls := map[string]int{}
			a := &Adapters{Client: &http.Client{Transport: transport(func(req *http.Request) (*http.Response, error) {
				provider := "sms_primary"
				if req.URL.Port() == "21002" {
					provider = "sms_secondary"
				}
				if req.URL.Port() == "21003" {
					data, _ := io.ReadAll(req.Body)
					provider = "push"
					if strings.Contains(string(data), `"channel":"email"`) {
						provider = "email"
					}
				}
				calls[provider]++
				code, body := 503, "provider unavailable"
				if provider == recoverAt {
					code, body = 200, `{"event_id":"event-1","status":"accepted","provider_message_id":"ok"}`
				}
				return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader(body))}, nil
			})}}
			env.OnActivity(AttemptActivity, mock.Anything, mock.Anything).Return(a.Send)
			e := event(1)
			e.Channels = append(e.Channels, pb.Channel_CHANNEL_EMAIL)
			start := env.Now()
			env.ExecuteWorkflow(func(ctx workflow.Context) error { return deliver(ctx, e) })
			require.NoError(t, env.GetWorkflowError())
			require.Equal(t, breakerThreshold, calls["sms_primary"])
			require.Equal(t, 1, calls[recoverAt])
			require.GreaterOrEqual(t, env.Now().Sub(start), 3500*time.Millisecond)
			t.Logf("T6 failover to %s in %s", recoverAt, env.Now().Sub(start))
		})
	}
}

func TestRetryJitterBoundsAndAmbiguousPinning(t *testing.T) {
	env := newEnv(t)
	var times []time.Time
	env.OnActivity(AttemptActivity, mock.Anything, mock.Anything).Return(func(_ context.Context, a Attempt) (Result, error) {
		require.Equal(t, "sms_primary", a.Target.Provider)
		times = append(times, env.Now())
		if len(times) == 7 {
			return Result{Accepted: true}, nil
		}
		if len(times) == 4 {
			return Result{CircuitOpen: true, RetryAfter: 30 * time.Second}, nil
		}
		return Result{Retryable: true, Ambiguous: true}, nil
	})
	env.ExecuteWorkflow(func(ctx workflow.Context) error { return deliver(ctx, event(1)) })
	require.NoError(t, env.GetWorkflowError())
	for i := 1; i < len(times); i++ {
		delay := time.Second << min(i-1, 5)
		actual := times[i].Sub(times[i-1])
		if i == 4 {
			require.GreaterOrEqual(t, actual, 30*time.Second)
			continue
		}
		require.GreaterOrEqual(t, actual, delay/2)
		require.LessOrEqual(t, actual, delay)
	}
}

func TestPermanentRejectionSkipsRetries(t *testing.T) {
	env := newEnv(t)
	var providers []string
	a := &Adapters{Client: &http.Client{Transport: transport(func(req *http.Request) (*http.Response, error) {
		providers = append(providers, req.URL.Port())
		if req.URL.Port() == "21001" {
			return &http.Response{StatusCode: 422, Body: io.NopCloser(strings.NewReader("permanent"))}, nil
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"event_id":"event-1","status":"accepted","provider_message_id":"ok"}`))}, nil
	})}}
	env.OnActivity(AttemptActivity, mock.Anything, mock.Anything).Return(a.Send)
	start := env.Now()
	env.ExecuteWorkflow(func(ctx workflow.Context) error { return deliver(ctx, event(1)) })
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, []string{"21001", "21002"}, providers)
	require.Equal(t, start, env.Now(), "422 must not schedule a retry timer")
	require.Zero(t, a.breakers.states["sms_primary"].failures)
}

func TestRateLimitDoesNotConsumeFailureBudget(t *testing.T) {
	for _, exhaust := range []bool{false, true} {
		t.Run(fmt.Sprintf("exhaust=%v", exhaust), func(t *testing.T) {
			env := newEnv(t)
			primaryCalls := 0
			secondaryCalls := 0
			var times []time.Time
			env.OnActivity(AttemptActivity, mock.Anything, mock.Anything).Return(func(_ context.Context, a Attempt) (Result, error) {
				if a.Target.Provider == "sms_secondary" {
					secondaryCalls++
					return Result{Accepted: true}, nil
				}
				require.Equal(t, "sms_primary", a.Target.Provider)
				primaryCalls++
				times = append(times, env.Now())
				if primaryCalls <= 4 || (exhaust && primaryCalls == 11) {
					return Result{Retryable: true}, nil
				}
				if primaryCalls <= 10 {
					return Result{Retryable: true, RateLimited: true, RetryAfter: 100 * time.Millisecond}, nil
				}
				return Result{Accepted: true}, nil
			})
			env.ExecuteWorkflow(func(ctx workflow.Context) error { return deliver(ctx, event(1)) })
			require.NoError(t, env.GetWorkflowError())
			require.Equal(t, 11, primaryCalls, "six 429s must not exhaust the remaining attempt")
			if exhaust {
				require.Equal(t, 1, secondaryCalls)
			} else {
				require.Zero(t, secondaryCalls)
			}
			for i := 5; i < len(times); i++ {
				require.Equal(t, 100*time.Millisecond, times[i].Sub(times[i-1]), "429 waits Retry-After without adding backoff")
			}
		})
	}
}

func TestRateLimitRespectsExpiry(t *testing.T) {
	env := newEnv(t)
	e := event(1)
	e.ExpiresAt = timestamppb.New(env.Now().Add(2 * time.Second))
	env.OnActivity(AttemptActivity, mock.Anything, mock.Anything).Return(Result{Retryable: true, RateLimited: true, RetryAfter: time.Minute}, nil).Once()
	start := env.Now()
	env.ExecuteWorkflow(func(ctx workflow.Context) error { return deliver(ctx, e) })
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, 2*time.Second, env.Now().Sub(start))
	env.AssertExpectations(t)
}

func TestT8BackPressureRetriesSameEvent(t *testing.T) {
	env := newEnv(t)
	var times []time.Time
	a := &Adapters{Client: &http.Client{Transport: transport(func(req *http.Request) (*http.Response, error) {
		require.Equal(t, "21001", req.URL.Port())
		require.Equal(t, "event-1", req.Header.Get("Idempotency-Key"))
		var payload map[string]any
		require.NoError(t, json.NewDecoder(req.Body).Decode(&payload))
		require.Equal(t, "event-1", payload["event_id"])
		times = append(times, env.Now())
		code, body := 429, `{"status":"busy","retryable":true}`
		if len(times) == 7 {
			code, body = 200, `{"event_id":"event-1","status":"accepted","provider_message_id":"ok"}`
		}
		return &http.Response{StatusCode: code, Header: http.Header{"Retry-After": []string{"1"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
	})}}
	env.OnActivity(AttemptActivity, mock.Anything, mock.Anything).Return(a.Send)
	env.ExecuteWorkflow(func(ctx workflow.Context) error { return deliver(ctx, event(1)) })
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, times, 7, "back pressure must not exhaust the failure budget or open the circuit")
	for i := 1; i < len(times); i++ {
		require.Equal(t, time.Second, times[i].Sub(times[i-1]))
	}
}

func TestRateLimitPreservesProviderFailures(t *testing.T) {
	code := 503
	a := &Adapters{Client: &http.Client{Transport: transport(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader("busy"))}, nil
	})}}
	send := func() Result {
		r, err := a.Send(context.Background(), Attempt{event(1), Target{Provider: "sms_primary"}})
		require.NoError(t, err)
		return r
	}
	for i := 0; i < breakerThreshold-1; i++ {
		require.True(t, send().Retryable)
	}
	code = 429
	for i := 0; i < 6; i++ {
		require.True(t, send().RateLimited)
		require.Equal(t, breakerThreshold-1, a.breakers.states["sms_primary"].failures)
	}
	code = 503
	require.True(t, send().Retryable)
	require.True(t, send().CircuitOpen)

	// Simulate cooldown without sleeping. A busy probe must release its slot
	// while preserving the circuit until a later probe proves recovery.
	s := a.breakers.states["sms_primary"]
	s.openUntil = time.Now().Add(-time.Second)
	code = 429
	for i := 0; i < 2; i++ {
		require.True(t, send().RateLimited)
		require.False(t, s.probing)
		require.Equal(t, breakerThreshold, s.failures)
		require.False(t, s.openUntil.IsZero())
	}
	code = 503
	require.True(t, send().Retryable)
	require.True(t, send().CircuitOpen)
}

func TestRateLimitWithoutJSONDoesNotTripCircuit(t *testing.T) {
	for _, header := range []string{"2", "", "invalid", "0", time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat), time.Now().Add(time.Hour).UTC().Format(http.TimeFormat)} {
		t.Run(header, func(t *testing.T) {
			a := &Adapters{Client: &http.Client{Transport: transport(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": []string{header}}, Body: io.NopCloser(strings.NewReader("busy"))}, nil
			})}}
			for i := 0; i < 6; i++ {
				r, err := a.Send(context.Background(), Attempt{event(1), Target{Provider: "sms_primary"}})
				require.NoError(t, err)
				require.True(t, r.RateLimited)
				require.False(t, r.Ambiguous)
				require.False(t, r.CircuitOpen)
				switch header {
				case "2":
					require.Equal(t, 2*time.Second, r.RetryAfter)
				case "", "invalid":
					require.Equal(t, time.Second, r.RetryAfter)
				case "0":
					require.Zero(t, r.RetryAfter)
				default:
					if strings.Contains(header, time.Now().Add(-time.Hour).UTC().Format("15:04")) {
						require.Zero(t, r.RetryAfter)
					} else {
						require.Greater(t, r.RetryAfter, 59*time.Minute)
						require.LessOrEqual(t, r.RetryAfter, time.Hour)
					}
				}
			}
			require.Zero(t, a.breakers.states["sms_primary"].failures)
		})
	}
}
