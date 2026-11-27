package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"go.temporal.io/sdk/activity"
)

// Adapter addresses are fixed deliberately: delivery traffic must traverse the
// chaos boundary, including when the worker runs on the host.
var proxyURLs = map[string]string{
	"sms_primary":   "http://toxiproxy:21001/send",
	"sms_secondary": "http://toxiproxy:21002/send",
	"push":          "http://toxiproxy:21003/send",
	"email":         "http://toxiproxy:21003/send",
}

type Adapters struct{ Client *http.Client }

func (a *Adapters) Send(ctx context.Context, attempt Attempt) (result Result, sendErr error) {
	// A prior activity execution may have reached the provider before a crash.
	// Even a later definitive failure cannot prove that earlier send did not land.
	defer func() {
		if activity.IsActivity(ctx) && activity.GetInfo(ctx).Attempt > 1 && !result.Accepted && !result.Expired {
			result.Ambiguous = true
			result.Retryable = true
		}
	}()
	e := attempt.Event
	if e == nil {
		return Result{}, errors.New("missing_event")
	}
	if e.ExpiresAt != nil && !time.Now().Before(e.ExpiresAt.AsTime()) {
		return Result{Expired: true}, nil
	}
	endpoint, ok := proxyURLs[attempt.Target.Provider]
	if !ok {
		return Result{}, errors.New("unknown_provider")
	}
	payload := map[string]any{"event_id": e.EventId, "account_id": e.AccountId, "sequence": e.Sequence, "recipient": e.Recipient, "body": e.Body}
	if e.OccurredAt != nil {
		payload["occurred_at"] = e.OccurredAt.AsTime().Format(time.RFC3339Nano)
	}
	if attempt.Target.Provider == "push" || attempt.Target.Provider == "email" {
		payload["channel"] = attempt.Target.Provider
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return Result{}, errors.New("encode_request")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(data))
	if err != nil {
		return Result{}, errors.New("build_request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", e.EventId)
	req.Header.Set("traceparent", e.Traceparent)
	c := a.Client
	if c == nil {
		c = &http.Client{Timeout: 10 * time.Second}
	}
	// Never follow redirects around Toxiproxy or resend PII to another host.
	bounded := *c
	bounded.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := bounded.Do(req)
	if err != nil {
		return Result{Retryable: true, Ambiguous: true}, nil
	}
	defer resp.Body.Close()
	var body struct {
		EventID           string `json:"event_id"`
		Status            string `json:"status"`
		Retryable         bool   `json:"retryable"`
		ProviderMessageID string `json:"provider_message_id"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&body) != nil || body.EventID != e.EventId {
		return Result{Retryable: true, Ambiguous: true}, nil
	}
	switch resp.StatusCode {
	case 200:
		if body.Status == "accepted" && !body.Retryable && body.ProviderMessageID != "" {
			return Result{Accepted: true}, nil
		}
	case 400:
		if body.Status == "invalid" {
			return Result{Invalid: true}, nil
		}
	case 422:
		if body.Status == "rejected" {
			return Result{}, nil
		}
	case 429:
		delay := time.Second
		if seconds, err := strconv.ParseUint(resp.Header.Get("Retry-After"), 10, 32); err == nil {
			delay = time.Duration(seconds) * time.Second
		} else if date, err := http.ParseTime(resp.Header.Get("Retry-After")); err == nil && time.Until(date) > 0 {
			delay = time.Until(date)
		}
		return Result{Retryable: true, RetryAfter: delay}, nil
	case 503:
		return Result{Retryable: true}, nil
	case 504:
		return Result{Retryable: true, Ambiguous: true}, nil
	}
	return Result{Retryable: true, Ambiguous: true}, nil
}
