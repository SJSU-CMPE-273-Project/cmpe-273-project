package delivery

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/SJSU-CMPE-273-Project/cmpe-273-project/gen/go/relayguard/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func testLedger(t *testing.T) (*Ledger, *pgx.Conn) {
	t.Helper()
	dsn := os.Getenv("DELIVERY_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set DELIVERY_TEST_DATABASE_URL for isolated Postgres tests")
	}
	ctx := t.Context()
	admin, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	schema := "delivery_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = admin.Exec(ctx, "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		_ = admin.Close(context.Background())
	})
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	db, err := pgx.Connect(ctx, u.String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close(context.Background()) })
	for _, name := range []string{"001_schema.sql", "003_ingest_recovery.sql", "004_ingest_rejection_identity.sql", "005_delivery_replay.sql", "005_delivery_replay.sql"} {
		data, err := os.ReadFile(filepath.Join("../../infra/postgres/init", name))
		require.NoError(t, err)
		_, err = db.Exec(ctx, string(data))
		require.NoError(t, err)
	}
	return &Ledger{DSN: u.String(), Adapters: &Adapters{}, Publish: func(context.Context, *pb.DeadLetter) error { return nil }}, db
}

func seedDelivery(t *testing.T, db *pgx.Conn, e *pb.NotificationRequested) {
	t.Helper()
	_, err := db.Exec(t.Context(), `INSERT INTO ingested_events(event_id,account_id,sequence,producer,payload_sha256,publish_state) VALUES($1,$2,$3,$4,$5,'published')`, e.EventId, e.AccountId, int64(e.Sequence), e.Producer, make([]byte, 32))
	require.NoError(t, err)
}
func stateOf(t *testing.T, db *pgx.Conn, id string) string {
	t.Helper()
	var state string
	require.NoError(t, db.QueryRow(t.Context(), `SELECT state::text FROM deliveries WHERE event_id=$1`, id).Scan(&state))
	return state
}
func ledgerEnv(l *Ledger) *testsuite.TestWorkflowEnvironment {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivityWithOptions(l.Send, activity.RegisterOptions{Name: AttemptActivity})
	env.RegisterActivityWithOptions(l.DeadLetter, activity.RegisterOptions{Name: DeadLetterActivity})
	env.RegisterActivityWithOptions(l.Expire, activity.RegisterOptions{Name: ExpireActivity})
	env.RegisterActivityWithOptions(l.LoadReplay, activity.RegisterOptions{Name: LoadReplayActivity})
	return env
}

func TestPostgresDLQHistoryAndReplay(t *testing.T) {
	l, db := testLedger(t)
	e := event(1)
	e.EventId = uuid.NewString()
	e.Channels = []pb.Channel{pb.Channel_CHANNEL_EMAIL}
	seedDelivery(t, db, e)
	// Isolate the five-failure workflow budget from the separately tested circuit.
	var calls atomic.Int32
	reject := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		require.Equal(t, e.EventId, r.Header.Get("Idempotency-Key"))
		calls.Add(1)
		return &http.Response{StatusCode: 503, Body: io.NopCloser(strings.NewReader(""))}, nil
	})}
	var envelopes []*pb.DeadLetter
	l.Publish = func(_ context.Context, dl *pb.DeadLetter) error {
		envelopes = append(envelopes, proto.Clone(dl).(*pb.DeadLetter))
		if len(envelopes) == 1 {
			return fmt.Errorf("lost_publish_ack")
		}
		return nil
	}
	env := ledgerEnv(l)
	env.RegisterActivityWithOptions(func(ctx context.Context, a Attempt) (Result, error) {
		l.Adapters = &Adapters{Client: reject}
		return l.Send(ctx, a)
	}, activity.RegisterOptions{Name: AttemptActivity, DisableAlreadyRegisteredCheck: true})
	env.ExecuteWorkflow(func(ctx workflow.Context) error { return deliver(ctx, e) })
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, int32(5), calls.Load())
	require.Equal(t, "dead_lettered", stateOf(t, db, e.EventId))
	require.Len(t, envelopes, 2)
	require.True(t, proto.Equal(envelopes[0], envelopes[1]))
	require.Len(t, envelopes[0].Attempts, 5)
	for i, a := range envelopes[0].Attempts {
		require.Equal(t, uint32(i+1), a.Attempt)
		require.Equal(t, "email", a.Provider)
		require.Equal(t, "unavailable", a.Error)
		require.NotNil(t, a.At)
	}
	require.Equal(t, e.EventId, envelopes[0].Original.EventId)
	// Replay is independent of the account sequence high-water mark.
	var delivered atomic.Int32
	l.Adapters = &Adapters{Client: &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		require.Equal(t, e.EventId, r.Header.Get("Idempotency-Key"))
		delivered.Add(1)
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(fmt.Sprintf(`{"event_id":%q,"status":"accepted","provider_message_id":"accepted"}`, e.EventId)))}, nil
	})}}
	env = ledgerEnv(l)
	env.ExecuteWorkflow(ReplayWorkflow, e.EventId, "operator-test")
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, "delivered", stateOf(t, db, e.EventId))
	require.Equal(t, int32(1), delivered.Load())
	var by string
	require.NoError(t, db.QueryRow(t.Context(), `SELECT replayed_by FROM dead_letters WHERE event_id=$1`, e.EventId).Scan(&by))
	require.Equal(t, "operator-test", by)
	// A different workflow cannot claim another replay, even after completion.
	_, err := l.claimReplay(t.Context(), e.EventId, "other-operator", "other-workflow/run")
	require.Error(t, err)
	// Lost activity acknowledgements and overlapping duplicate activities cannot
	// cross providers or send again once the ledger has accepted the message.
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := l.Send(t.Context(), Attempt{e, Target{"sms_secondary", pb.Channel_CHANNEL_SMS}})
			require.NoError(t, err)
			require.True(t, r.Accepted)
		}()
	}
	wg.Wait()
	require.Equal(t, int32(1), delivered.Load())
	var attempts int
	require.NoError(t, db.QueryRow(t.Context(), `SELECT count(*) FROM delivery_attempts WHERE event_id=$1`, e.EventId).Scan(&attempts))
	require.Equal(t, 6, attempts)
}

func TestPostgresExpiryAtWorkflowAndBackoff(t *testing.T) {
	for _, backoff := range []bool{false, true} {
		t.Run(fmt.Sprint(backoff), func(t *testing.T) {
			l, db := testLedger(t)
			e := event(1)
			e.EventId = uuid.NewString()
			seedDelivery(t, db, e)
			env := ledgerEnv(l)
			ttl := -time.Second
			if backoff {
				ttl = 500 * time.Millisecond
			}
			e.ExpiresAt = timestamppb.New(env.Now().Add(ttl))
			var calls atomic.Int32
			l.Adapters.Client = &http.Client{Transport: transport(func(*http.Request) (*http.Response, error) {
				calls.Add(1)
				return &http.Response{StatusCode: 503, Body: io.NopCloser(strings.NewReader(""))}, nil
			})}
			env.ExecuteWorkflow(func(ctx workflow.Context) error { return deliver(ctx, e) })
			require.NoError(t, env.GetWorkflowError())
			require.Equal(t, "expired", stateOf(t, db, e.EventId))
			want := int32(0)
			if backoff {
				want = 1
			}
			require.Equal(t, want, calls.Load())
		})
	}
}

func TestPostgresConcurrentReplayClaim(t *testing.T) {
	l, db := testLedger(t)
	e := event(1)
	e.EventId = uuid.NewString()
	seedDelivery(t, db, e)
	require.NoError(t, l.DeadLetter(t.Context(), e, "poison"))
	var successes atomic.Int32
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := l.claimReplay(t.Context(), e.EventId, "operator", fmt.Sprint(i))
			if err == nil {
				successes.Add(1)
			}
		}()
	}
	wg.Wait()
	require.Equal(t, int32(1), successes.Load())
	var token string
	require.NoError(t, db.QueryRow(t.Context(), `SELECT replay_token FROM dead_letters WHERE event_id=$1`, e.EventId).Scan(&token))
	// A retry of the winning LoadReplay activity must remain safe.
	got, err := l.claimReplay(t.Context(), e.EventId, "operator", token)
	require.NoError(t, err)
	require.Equal(t, e.EventId, got.EventId)
}

func TestPostgresPoisonHistory(t *testing.T) {
	l, db := testLedger(t)
	e := event(1)
	e.EventId = strings.ToUpper(uuid.NewString())
	seedDelivery(t, db, e)
	l.Adapters.Client = &http.Client{Transport: transport(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 422, Body: io.NopCloser(strings.NewReader(""))}, nil
	})}
	var dl *pb.DeadLetter
	l.Publish = func(_ context.Context, d *pb.DeadLetter) error { dl = d; return nil }
	env := ledgerEnv(l)
	env.ExecuteWorkflow(func(ctx workflow.Context) error { return deliver(ctx, e) })
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, "dead_lettered", stateOf(t, db, e.EventId))
	require.Len(t, dl.Attempts, 3)
	for i, provider := range []string{"sms_primary", "sms_secondary", "push"} {
		require.Equal(t, provider, dl.Attempts[i].Provider)
		require.Equal(t, "rejected", dl.Attempts[i].Error)
	}
	// A delayed original DLQ retry cannot consume the operator's claimed replay.
	_, err := l.claimReplay(t.Context(), strings.ToLower(e.EventId), "operator", "replay/run")
	require.NoError(t, err)
	require.NoError(t, l.DeadLetter(t.Context(), e, "channels_exhausted"))
	require.Equal(t, "pending", stateOf(t, db, e.EventId))
}

func TestPostgresLostSendAcknowledgement(t *testing.T) {
	l, db := testLedger(t)
	e := event(1)
	e.EventId = uuid.NewString()
	seedDelivery(t, db, e)
	interrupted, cancel := context.WithCancel(t.Context())
	defer cancel()
	keys := map[string]bool{}
	calls := 0
	l.Adapters.Client = &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		calls++
		keys[r.Header.Get("Idempotency-Key")] = true
		if calls == 1 {
			cancel()
		} // Provider accepted; ledger completion cannot commit.
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(fmt.Sprintf(`{"event_id":%q,"status":"accepted","provider_message_id":"same-receipt"}`, e.EventId)))}, nil
	})}
	a := Attempt{e, Target{"sms_primary", pb.Channel_CHANNEL_SMS}}
	_, err := l.Send(interrupted, a)
	require.Error(t, err)
	require.Equal(t, "retrying", stateOf(t, db, e.EventId))
	var audit string
	require.NoError(t, db.QueryRow(t.Context(), `SELECT error FROM delivery_attempts WHERE event_id=$1 AND attempt=1`, e.EventId).Scan(&audit))
	require.Equal(t, "interrupted_or_pending", audit)
	r, err := l.Send(t.Context(), a)
	require.NoError(t, err)
	require.True(t, r.Accepted)
	require.Equal(t, 2, calls)
	require.Len(t, keys, 1)
	require.True(t, keys[e.EventId])
	require.Equal(t, "delivered", stateOf(t, db, e.EventId))
}
