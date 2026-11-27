package delivery

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/SJSU-CMPE-273-Project/cmpe-273-project/contracts"
	pb "github.com/SJSU-CMPE-273-Project/cmpe-273-project/gen/go/relayguard/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type ReplayDispatchClient interface {
	ReplayStarter
	DescribeWorkflowExecution(context.Context, string, string) (*workflowservice.DescribeWorkflowExecutionResponse, error)
}

type ReplayDispatcher struct {
	DSN      string
	Temporal ReplayDispatchClient
}

// Run polls only committed human approvals. Transient failures leave the request
// approved. A bounded attempt and one-second backoff prevent a dependency outage
// from blocking Kafka delivery or spinning on a failed request.
func (d *ReplayDispatcher) Run(ctx context.Context) {
	for ctx.Err() == nil {
		attemptCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		processed, err := d.DispatchOne(attemptCtx)
		cancel()
		if err != nil && ctx.Err() == nil {
			slog.Warn("replay dispatch deferred", "reason", "database_or_temporal_unavailable")
		}
		if err == nil && processed {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

// DispatchOne holds only the approval row lock across Temporal submission.
// A rollback/crash leaves it retryable; the stable workflow ID closes the gap
// between acceptance by Temporal and committing status=started in PostgreSQL.
func (d *ReplayDispatcher) DispatchOne(ctx context.Context) (bool, error) {
	c, err := pgx.Connect(ctx, d.DSN)
	if err != nil {
		return false, err
	}
	defer c.Close(context.Background())
	tx, err := c.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(context.Background())
	var id, operator string
	err = tx.QueryRow(ctx, `SELECT event_id::text, operator FROM replay_requests
   WHERE status='approved' ORDER BY created_at,event_id LIMIT 1 FOR UPDATE SKIP LOCKED`).Scan(&id, &operator)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	workflowID := "delivery-replay/" + id
	finish := func(status, reason string) (bool, error) {
		_, err := tx.Exec(ctx, `UPDATE replay_requests SET status=$2,reason=$3,updated_at=now(),
    workflow_id=CASE WHEN $2='started' THEN $4 ELSE NULL END,
    started_at=CASE WHEN $2='started' THEN COALESCE(started_at,now()) ELSE NULL END WHERE event_id=$1`, id, status, reason, workflowID)
		if err != nil {
			return false, err
		}
		_, err = tx.Exec(ctx, `INSERT INTO ops_audit(operator,action,target,result) VALUES($1,$2,$3,$4)`,
			operator, "replay_dispatch_"+status, id, reason)
		if err != nil {
			return false, err
		}
		return true, tx.Commit(ctx)
	}
	// Reconcile BEFORE checking current ledger state or TTL: an earlier attempt
	// may have started (even finished) the workflow before its DB commit was lost.
	_, err = d.Temporal.DescribeWorkflowExecution(ctx, workflowID, "")
	if err == nil {
		return finish("started", "workflow_already_exists")
	}
	var notFound *serviceerror.NotFound
	if !errors.As(err, &notFound) {
		return false, err
	}
	var data []byte
	var replayed, expired bool
	var state, account string
	var sequence int64
	err = tx.QueryRow(ctx, `SELECT dl.original,dl.replayed_at IS NOT NULL,
   COALESCE(d.expires_at<=clock_timestamp(),false),d.state::text,d.account_id,d.sequence
   FROM dead_letters dl JOIN deliveries d USING(event_id) WHERE dl.event_id=$1`, id).
		Scan(&data, &replayed, &expired, &state, &account, &sequence)
	if errors.Is(err, pgx.ErrNoRows) {
		return finish("refused", "dead_letter_not_found")
	}
	if err != nil {
		return false, err
	}
	if replayed {
		return finish("refused", "already_replayed")
	}
	if expired {
		return finish("refused", "expired")
	}
	if state != "dead_lettered" {
		return finish("refused", "not_in_dlq")
	}
	if strings.TrimSpace(operator) == "" {
		return finish("refused", "invalid_operator")
	}
	if len(data) == 0 {
		return finish("refused", "missing_original_payload")
	}
	e := new(pb.NotificationRequested)
	if proto.Unmarshal(data, e) != nil {
		return finish("refused", "invalid_original_payload")
	}
	originalID, parseErr := uuid.Parse(e.EventId)
	if parseErr != nil || originalID.String() != id || e.AccountId != account || e.Sequence != uint64(sequence) {
		return finish("refused", "invalid_original_payload")
	}
	// Validate the stored original, without logging or changing its sensitive fields.
	jsonData, marshalErr := protojson.Marshal(e)
	if marshalErr != nil {
		return finish("refused", "invalid_original_payload")
	}
	if _, err := contracts.Decode(jsonData); err != nil {
		return finish("refused", "invalid_original_payload")
	}
	if e.ExpiresAt != nil && !time.Now().Before(e.ExpiresAt.AsTime()) {
		return finish("refused", "expired")
	}
	err = StartReplay(ctx, d.Temporal, id, operator)
	if err != nil {
		var duplicate *serviceerror.WorkflowExecutionAlreadyStarted
		if !errors.As(err, &duplicate) {
			return false, err
		}
		return finish("started", "workflow_already_exists")
	}
	return finish("started", "workflow_submitted")
}
