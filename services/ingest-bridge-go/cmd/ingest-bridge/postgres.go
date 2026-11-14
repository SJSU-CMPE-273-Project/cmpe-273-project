package main

import (
	"bytes"
	"context"
	"errors"
	"strconv"

	pb "github.com/SJSU-CMPE-273-Project/cmpe-273-project/gen/go/relayguard/v1"
	"github.com/jackc/pgx/v5"
)

type postgresLedger struct {
	conn           *pgx.Conn
	topicCreatedAt string
}

func (s postgresLedger) rejection(ctx context.Context, pos position) (string, error) {
	var reason string
	err := s.conn.QueryRow(ctx, `
		SELECT reason FROM ingest_rejections
		WHERE topic_created_at = $1::text::numeric AND partition_id = $2 AND source_offset = $3::text::numeric`,
		s.topicCreatedAt, pos.partition, strconv.FormatUint(pos.offset, 10)).Scan(&reason)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return reason, err
}

func (s postgresLedger) reject(ctx context.Context, pos position, reason, eventID string) error {
	_, err := s.conn.Exec(ctx, `
		INSERT INTO ingest_rejections (topic_created_at, partition_id, source_offset, reason, event_id)
		VALUES ($1::text::numeric, $2, $3::text::numeric, $4, NULLIF($5, '')::uuid)
		ON CONFLICT DO NOTHING`,
		s.topicCreatedAt, pos.partition, strconv.FormatUint(pos.offset, 10), reason, eventID)
	return err
}

func (s postgresLedger) prepare(ctx context.Context, event *pb.NotificationRequested, fingerprint [32]byte) (string, error) {
	var storedHash []byte
	var state string
	err := s.conn.QueryRow(ctx, `
		SELECT payload_sha256, publish_state FROM ingested_events WHERE event_id = $1`,
		event.EventId).Scan(&storedHash, &state)
	switch {
	case err == nil:
		if !bytes.Equal(storedHash, fingerprint[:]) {
			return "event_id_conflict", nil
		}
		return state, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return "", err
	}

	// The single bridge session owns all six partitions. Lock the producer
	// control row through the insert so an operator update and admission serialize.
	tx, err := s.conn.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	var enabled bool
	err = tx.QueryRow(ctx, `SELECT enabled FROM producer_controls WHERE producer = $1 FOR SHARE`,
		event.Producer).Scan(&enabled)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && !enabled) {
		return "producer_disabled", nil
	}
	if err != nil {
		return "", err
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO ingested_events (event_id, account_id, sequence, producer, payload_sha256)
		VALUES ($1, $2, $3, $4, $5) ON CONFLICT (account_id, sequence) DO NOTHING`,
		event.EventId, event.AccountId, int64(event.Sequence), event.Producer, fingerprint[:])
	if err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	if tag.RowsAffected() == 0 {
		return "sequence_conflict", nil
	}
	return pending, nil
}

func (s postgresLedger) markPublished(ctx context.Context, eventID string) error {
	tag, err := s.conn.Exec(ctx, `
		UPDATE ingested_events SET publish_state = 'published'
		WHERE event_id = $1 AND publish_state = 'pending'`, eventID)
	if err == nil && tag.RowsAffected() != 1 {
		return errors.New("missing pending event")
	}
	return err
}
