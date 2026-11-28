package delivery

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	pb "github.com/SJSU-CMPE-273-Project/cmpe-273-project/gen/go/relayguard/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type dispatchTemporal struct {
	mu                sync.Mutex
	exists            bool
	calls, starts     int
	workflowID        string
	args              []interface{}
	describeErr       error
	startErr          error
	acceptedWithError bool
	hideExisting      bool
	entered           chan struct{}
	release           chan struct{}
}

func (f *dispatchTemporal) DescribeWorkflowExecution(_ context.Context, id, _ string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.workflowID = id
	if f.describeErr != nil {
		return nil, f.describeErr
	}
	if f.exists && !f.hideExisting {
		return &workflowservice.DescribeWorkflowExecutionResponse{}, nil
	}
	return nil, serviceerror.NewNotFound("not found")
}
func (f *dispatchTemporal) ExecuteWorkflow(ctx context.Context, o client.StartWorkflowOptions, _ interface{}, args ...interface{}) (client.WorkflowRun, error) {
	f.mu.Lock()
	f.calls++
	f.args = args
	f.workflowID = o.ID
	if f.exists {
		f.mu.Unlock()
		return nil, serviceerror.NewWorkflowExecutionAlreadyStarted("already started", "", "run")
	}
	if f.startErr != nil && !f.acceptedWithError {
		err := f.startErr
		f.mu.Unlock()
		return nil, err
	}
	f.exists = true
	f.starts++
	err := f.startErr
	f.mu.Unlock()
	if f.entered != nil {
		close(f.entered)
		select {
		case <-f.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, err
}
func dispatcherFixture(t *testing.T) (*ReplayDispatcher, *pgx.Conn, *pb.NotificationRequested, *dispatchTemporal) {
	t.Helper()
	ledger, db := testLedger(t)
	for _, name := range []string{"005_ops_console.sql", "007_replay_dispatch.sql", "007_replay_dispatch.sql"} {
		data, err := os.ReadFile(filepath.Join("../../infra/postgres/init", name))
		require.NoError(t, err)
		_, err = db.Exec(t.Context(), string(data))
		require.NoError(t, err)
	}
	e := event(1)
	e.EventId = uuid.NewString()
	e.Type = pb.NotificationType_NOTIFICATION_TYPE_DEPOSIT_CONFIRMED
	e.Priority = pb.Priority_PRIORITY_NORMAL
	e.Producer = "core-banking-java"
	e.ExpiresAt = timestamppb.New(time.Now().Add(time.Hour))
	seedDelivery(t, db, e)
	_, err := db.Exec(t.Context(), `INSERT INTO deliveries(event_id,account_id,sequence,priority,state,expires_at)
  VALUES($1,$2,1,2,'dead_lettered',$3)`, e.EventId, e.AccountId, e.ExpiresAt.AsTime())
	require.NoError(t, err)
	data, err := proto.Marshal(e)
	require.NoError(t, err)
	_, err = db.Exec(t.Context(), `INSERT INTO dead_letters(event_id,reason,original) VALUES($1,'poison',$2)`, e.EventId, data)
	require.NoError(t, err)
	f := new(dispatchTemporal)
	d := &ReplayDispatcher{DSN: ledger.DSN, Temporal: f}
	t.Cleanup(d.Close)
	return d, db, e, f
}
func approveDispatch(t *testing.T, db *pgx.Conn, id string) {
	t.Helper()
	_, err := db.Exec(t.Context(), `INSERT INTO replay_requests(event_id,operator,status,prompt_version)
  VALUES($1,'human-operator','approved','dlq-v2')`, id)
	require.NoError(t, err)
}
func dispatchStatus(t *testing.T, db *pgx.Conn, id string) string {
	t.Helper()
	var status string
	require.NoError(t, db.QueryRow(t.Context(), `SELECT status FROM replay_requests WHERE event_id=$1`, id).Scan(&status))
	return status
}
func TestDispatcherRequiresApprovalAndPreservesIdentity(t *testing.T) {
	d, db, e, f := dispatcherFixture(t)
	// An undecided model proposal does not grant authority to dispatch.
	_, err := db.Exec(t.Context(), `INSERT INTO agent_proposals(event_id,action,rationale,prompt_version)
  VALUES($1,'replay','proposal only','dlq-v2')`, e.EventId)
	require.NoError(t, err)
	done, err := d.DispatchOne(t.Context())
	require.NoError(t, err)
	require.False(t, done)
	require.Zero(t, f.starts)
	approveDispatch(t, db, e.EventId)
	done, err = d.DispatchOne(t.Context())
	require.NoError(t, err)
	require.True(t, done)
	require.Equal(t, "started", dispatchStatus(t, db, e.EventId))
	require.Equal(t, 1, f.starts)
	require.Equal(t, "delivery-replay/"+e.EventId, f.workflowID)
	require.Equal(t, []interface{}{e.EventId, "human-operator"}, f.args)
	var workflowID, operator string
	var at time.Time
	require.NoError(t, db.QueryRow(t.Context(), `SELECT workflow_id,operator,started_at FROM replay_requests WHERE event_id=$1`, e.EventId).Scan(&workflowID, &operator, &at))
	require.Equal(t, f.workflowID, workflowID)
	require.Equal(t, "human-operator", operator)
	done, err = d.DispatchOne(t.Context())
	require.NoError(t, err)
	require.False(t, done)
	require.Equal(t, 1, f.starts)
}
func TestDispatcherRefusesInvalidAndExpiredRequests(t *testing.T) {
	for _, tc := range []struct{ name, sql, want string }{
		{"expiry after approval", `UPDATE deliveries SET expires_at=now()-interval '1 second' WHERE event_id=$1`, "expired"},
		{"missing payload", `UPDATE dead_letters SET original=NULL WHERE event_id=$1`, "missing_original_payload"},
		{"malformed payload", `UPDATE dead_letters SET original='bad'::bytea WHERE event_id=$1`, "invalid_original_payload"},
		{"wrong identity", `UPDATE deliveries SET account_id='other' WHERE event_id=$1`, "invalid_original_payload"},
		{"already claimed", `UPDATE dead_letters SET replayed_at=now() WHERE event_id=$1`, "already_replayed"},
		{"not in dlq", `UPDATE deliveries SET state='delivered' WHERE event_id=$1`, "not_in_dlq"},
		{"empty operator", `UPDATE replay_requests SET operator=' ' WHERE event_id=$1`, "invalid_operator"},
		{"missing dlq", `DELETE FROM dead_letters WHERE event_id=$1`, "dead_letter_not_found"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, db, e, f := dispatcherFixture(t)
			approveDispatch(t, db, e.EventId)
			_, err := db.Exec(t.Context(), tc.sql, e.EventId)
			require.NoError(t, err)
			done, err := d.DispatchOne(t.Context())
			require.NoError(t, err)
			require.True(t, done)
			var reason string
			require.NoError(t, db.QueryRow(t.Context(), `SELECT reason FROM replay_requests WHERE event_id=$1`, e.EventId).Scan(&reason))
			require.Equal(t, "refused", dispatchStatus(t, db, e.EventId))
			require.Equal(t, tc.want, reason)
			require.Zero(t, f.starts)
		})
	}
}
func TestDispatcherTwoWorkersSkipLockedApproval(t *testing.T) {
	d, db, e, f := dispatcherFixture(t)
	approveDispatch(t, db, e.EventId)
	f.entered = make(chan struct{})
	f.release = make(chan struct{})
	result := make(chan error, 1)
	go func() { _, err := d.DispatchOne(t.Context()); result <- err }()
	select {
	case <-f.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("dispatcher never submitted")
	}
	done, err := d.DispatchOne(t.Context())
	require.NoError(t, err)
	require.False(t, done)
	close(f.release)
	require.NoError(t, <-result)
	require.Equal(t, 1, f.starts)
}
func TestDispatcherReconcilesAcceptedReplayAfterDatabaseFailure(t *testing.T) {
	d, db, e, f := dispatcherFixture(t)
	approveDispatch(t, db, e.EventId)
	_, err := db.Exec(t.Context(), `CREATE FUNCTION fail_dispatch() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN RAISE EXCEPTION 'simulated commit gap'; END $$;
 CREATE TRIGGER fail_dispatch BEFORE UPDATE ON replay_requests FOR EACH ROW EXECUTE FUNCTION fail_dispatch()`)
	require.NoError(t, err)
	_, err = d.DispatchOne(t.Context())
	require.Error(t, err)
	require.Equal(t, "approved", dispatchStatus(t, db, e.EventId))
	require.Equal(t, 1, f.starts)
	_, err = db.Exec(t.Context(), `DROP TRIGGER fail_dispatch ON replay_requests;
 UPDATE deliveries SET expires_at=now()-interval '1 second'; UPDATE dead_letters SET replayed_at=now()`)
	require.NoError(t, err)
	done, err := d.DispatchOne(t.Context())
	require.NoError(t, err)
	require.True(t, done)
	require.Equal(t, "started", dispatchStatus(t, db, e.EventId))
	require.Equal(t, 1, f.starts)
	require.Equal(t, 1, f.calls)
	var audits int
	require.NoError(t, db.QueryRow(t.Context(), `SELECT count(*) FROM ops_audit WHERE action='replay_dispatch_started'`).Scan(&audits))
	require.Equal(t, 1, audits)
}
func TestDispatcherTemporaryErrorsRemainRetryable(t *testing.T) {
	for _, phase := range []string{"describe", "start", "ambiguous acceptance"} {
		t.Run(phase, func(t *testing.T) {
			d, db, e, f := dispatcherFixture(t)
			approveDispatch(t, db, e.EventId)
			if phase == "describe" {
				f.describeErr = errors.New("temporary")
			} else {
				f.startErr = context.DeadlineExceeded
				f.acceptedWithError = phase == "ambiguous acceptance"
			}
			_, err := d.DispatchOne(t.Context())
			require.Error(t, err)
			require.Equal(t, "approved", dispatchStatus(t, db, e.EventId))
			f.describeErr = nil
			f.startErr = nil
			done, err := d.DispatchOne(t.Context())
			require.NoError(t, err)
			require.True(t, done)
			require.Equal(t, "started", dispatchStatus(t, db, e.EventId))
			require.Equal(t, 1, f.starts)
		})
	}
}
func TestDispatcherHandlesConcurrentManualSubmission(t *testing.T) {
	d, db, e, f := dispatcherFixture(t)
	approveDispatch(t, db, e.EventId)
	f.exists = true
	f.hideExisting = true
	done, err := d.DispatchOne(t.Context())
	require.NoError(t, err)
	require.True(t, done)
	require.Equal(t, "started", dispatchStatus(t, db, e.EventId))
	require.Zero(t, f.starts)
}
