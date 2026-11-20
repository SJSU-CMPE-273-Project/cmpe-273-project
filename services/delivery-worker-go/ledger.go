package delivery

import (
	"context"
	"errors"
	"time"

	pb "github.com/SJSU-CMPE-273-Project/cmpe-273-project/gen/go/relayguard/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Ledger serializes all sends and replay transitions for an event across workers.
// A session lock spans the HTTP request; closing the connection releases it even
// after cancellation. Attempt reservations commit BEFORE HTTP so a crash leaves
// visible evidence rather than silently erasing the attempt.
type Ledger struct {
	DSN      string
	Adapters *Adapters
	Publish  func(context.Context, *pb.DeadLetter) error
}

func (l *Ledger) lock(ctx context.Context, id string) (*pgx.Conn, error) {
	key := id
	if parsed, err := uuid.Parse(id); err == nil {
		key = parsed.String()
	}
	c, err := pgx.Connect(ctx, l.DSN)
	if err != nil {
		return nil, err
	}
	if _, err = c.Exec(ctx, "SELECT pg_advisory_lock(hashtextextended($1, 0))", key); err != nil {
		c.Close(context.Background())
		return nil, err
	}
	return c, nil
}

func ensureDelivery(ctx context.Context, c *pgx.Conn, e *pb.NotificationRequested) error {
	var expires any
	if e.ExpiresAt != nil {
		expires = e.ExpiresAt.AsTime()
	}
	_, err := c.Exec(ctx, `INSERT INTO deliveries(event_id,account_id,sequence,priority,expires_at) VALUES($1,$2,$3,$4,$5) ON CONFLICT(event_id) DO NOTHING`, e.EventId, e.AccountId, int64(e.Sequence), int(e.Priority), expires)
	return err
}

func (l *Ledger) Send(ctx context.Context, a Attempt) (Result, error) {
	c, err := l.lock(ctx, a.Event.EventId)
	if err != nil {
		return Result{}, err
	}
	defer c.Close(context.Background())
	if err = ensureDelivery(ctx, c, a.Event); err != nil {
		return Result{}, err
	}
	var state string
	if err = c.QueryRow(ctx, `SELECT state::text FROM deliveries WHERE event_id=$1`, a.Event.EventId).Scan(&state); err != nil {
		return Result{}, err
	}
	if state == "delivered" || state == "dead_lettered" {
		return Result{Accepted: true}, nil
	}
	if state == "expired" {
		return Result{Expired: true}, nil
	}
	tx, err := c.Begin(ctx)
	if err != nil {
		return Result{}, err
	}
	defer tx.Rollback(context.Background())
	var n int
	// Reserve one globally increasing audit ordinal, including activity retries.
	err = tx.QueryRow(ctx, `UPDATE deliveries SET attempts=attempts+1, state='retrying',updated_at=now() WHERE event_id=$1 RETURNING attempts`, a.Event.EventId).Scan(&n)
	if err != nil {
		return Result{}, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO delivery_attempts(event_id,attempt,channel,provider,succeeded,error) VALUES($1,$2,$3,$4,false,'interrupted_or_pending')`, a.Event.EventId, n, a.Target.Channel.String(), a.Target.Provider)
	if err != nil {
		return Result{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Result{}, err
	}
	start := time.Now()
	r, sendErr := l.Adapters.Send(ctx, a)
	code := resultCode(r)
	if sendErr != nil {
		code = "activity_error"
	}
	_, err = c.Exec(ctx, `UPDATE delivery_attempts SET succeeded=$3,error=$4,latency_ms=$5 WHERE event_id=$1 AND attempt=$2`, a.Event.EventId, n, r.Accepted, code, int(time.Since(start).Milliseconds()))
	if err != nil {
		return Result{}, err
	}
	next := "retrying"
	if r.Accepted {
		next = "delivered"
	}
	if r.Expired {
		next = "expired"
	}
	_, err = c.Exec(ctx, `UPDATE deliveries SET state=$2::delivery_state,channel=$3,provider=$4,delivered_at=CASE WHEN $2='delivered' THEN now() ELSE delivered_at END,updated_at=now() WHERE event_id=$1`, a.Event.EventId, next, a.Target.Channel.String(), a.Target.Provider)
	if err != nil {
		return Result{}, err
	}
	return r, sendErr
}

func resultCode(r Result) string {
	switch {
	case r.Accepted:
		return ""
	case r.Expired:
		return "expired"
	case r.Ambiguous:
		return "ambiguous"
	case r.Invalid:
		return "invalid_request"
	case r.RateLimited:
		return "rate_limited"
	case r.CircuitOpen:
		return "circuit_open"
	case r.Retryable:
		return "unavailable"
	default:
		return "rejected"
	}
}

func (l *Ledger) Expire(ctx context.Context, e *pb.NotificationRequested) error {
	c, err := l.lock(ctx, e.EventId)
	if err != nil {
		return err
	}
	defer c.Close(context.Background())
	if err = ensureDelivery(ctx, c, e); err != nil {
		return err
	}
	_, err = c.Exec(ctx, `UPDATE deliveries SET state='expired',updated_at=now() WHERE event_id=$1 AND state IN ('pending','retrying')`, e.EventId)
	return err
}

func (l *Ledger) DeadLetter(ctx context.Context, e *pb.NotificationRequested, reason string) error {
	c, err := l.lock(ctx, e.EventId)
	if err != nil {
		return err
	}
	defer c.Close(context.Background())
	if err = ensureDelivery(ctx, c, e); err != nil {
		return err
	}
	// An old DLQ publication retry must not overwrite an operator replay.
	var replayToken *string
	err = c.QueryRow(ctx, `SELECT replay_token FROM dead_letters WHERE event_id=$1`, e.EventId).Scan(&replayToken)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if replayToken != nil {
		owner := ""
		if activity.IsActivity(ctx) {
			info := activity.GetInfo(ctx)
			owner = info.WorkflowExecution.ID + "/" + info.WorkflowExecution.RunID
		}
		if owner != *replayToken {
			return nil
		}
	}
	data, err := proto.Marshal(e)
	if err != nil {
		return err
	}
	tx, err := c.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	var state string
	if err = tx.QueryRow(ctx, `SELECT state::text FROM deliveries WHERE event_id=$1`, e.EventId).Scan(&state); err != nil {
		return err
	}
	if state == "delivered" || state == "expired" {
		return nil
	}
	_, err = tx.Exec(ctx, `INSERT INTO dead_letters(event_id,reason,original) VALUES($1,$2,$3) ON CONFLICT(event_id) DO NOTHING`, e.EventId, reason, data)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE deliveries SET state='dead_lettered',updated_at=now() WHERE event_id=$1`, e.EventId)
	if err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	dl := &pb.DeadLetter{Original: e, Reason: reason}
	return l.publishHistory(ctx, c, dl)
}

func (l *Ledger) publishHistory(ctx context.Context, c *pgx.Conn, dl *pb.DeadLetter) error {
	var at time.Time
	if err := c.QueryRow(ctx, `SELECT reason,dead_lettered_at FROM dead_letters WHERE event_id=$1`, dl.Original.EventId).Scan(&dl.Reason, &at); err != nil {
		return err
	}
	dl.DeadLetteredAt = timestamppb.New(at)
	rows, err := c.Query(ctx, `SELECT attempt,channel,provider,COALESCE(error,''),at FROM delivery_attempts WHERE event_id=$1 ORDER BY attempt`, dl.Original.EventId)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		a := new(pb.DeliveryAttempt)
		var ch string
		var at time.Time
		if err = rows.Scan(&a.Attempt, &ch, &a.Provider, &a.Error, &at); err != nil {
			return err
		}
		a.Channel = pb.Channel(pb.Channel_value[ch])
		a.At = timestamppb.New(at)
		dl.Attempts = append(dl.Attempts, a)
	}
	if err = rows.Err(); err != nil {
		return err
	}
	return l.Publish(ctx, dl)
}

// LoadReplay is retried safely by Temporal. The dedicated replay workflow ID
// rejects repeated operator requests, while the DB transition and send lock
// prevent overlapping delivery even across worker processes.
func (l *Ledger) LoadReplay(ctx context.Context, id, operator string) (*pb.NotificationRequested, error) {
	info := activity.GetInfo(ctx)
	return l.claimReplay(ctx, id, operator, info.WorkflowExecution.ID+"/"+info.WorkflowExecution.RunID)
}

func (l *Ledger) claimReplay(ctx context.Context, id, operator, token string) (*pb.NotificationRequested, error) {
	c, err := l.lock(ctx, id)
	if err != nil {
		return nil, err
	}
	defer c.Close(context.Background())
	var data []byte
	err = c.QueryRow(ctx, `SELECT original FROM dead_letters WHERE event_id=$1`, id).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, temporal.NewNonRetryableApplicationError("dead_letter_not_found", "invalid_replay", nil)
	}
	if err != nil {
		return nil, err
	}
	e := new(pb.NotificationRequested)
	parsedID, idErr := uuid.Parse(id)
	if err = proto.Unmarshal(data, e); err != nil {
		return nil, temporal.NewNonRetryableApplicationError("invalid_replay_payload", "invalid_replay", nil)
	}
	originalID, originalErr := uuid.Parse(e.EventId)
	if idErr != nil || originalErr != nil || parsedID != originalID {
		return nil, temporal.NewNonRetryableApplicationError("invalid_replay_payload", "invalid_replay", nil)
	}
	tx, err := c.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.Background())
	tag, err := tx.Exec(ctx, `UPDATE dead_letters SET replayed_at=COALESCE(replayed_at,now()),replayed_by=COALESCE(replayed_by,$2),replay_token=$3 WHERE event_id=$1 AND (replay_token IS NULL OR replay_token=$3)`, id, operator, token)
	if err == nil && tag.RowsAffected() != 1 {
		return nil, temporal.NewNonRetryableApplicationError("already_replayed", "invalid_replay", nil)
	}
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(ctx, `UPDATE deliveries SET state='pending',updated_at=now() WHERE event_id=$1 AND state='dead_lettered'`, id)
	if err != nil {
		return nil, err
	}
	return e, tx.Commit(ctx)
}
