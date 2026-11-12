package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	pb "github.com/SJSU-CMPE-273-Project/cmpe-273-project/gen/go/relayguard/v1"
	iggy "github.com/apache/iggy/foreign/go/contracts"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"
)

const projectRoot = "../../../.."

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(projectRoot, "contracts", "fixtures", name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestValidateOTPExpiry(t *testing.T) {
	data := fixture(t, "otp")
	hash := sha256.Sum256([]byte("otp-0001"))
	partition := binary.BigEndian.Uint32(hash[:4]) % partitions
	for _, tc := range []struct {
		name, expiry, reason string
	}{
		{"valid and already expired", "2026-11-09T18:10:00Z", ""},
		{"missing", "", "invalid_expiry"},
		{"equal to occurredAt", "2026-11-09T18:05:00Z", "invalid_expiry"},
		{"before occurredAt", "2026-11-09T18:04:59Z", "invalid_expiry"},
		{"over five minutes", "2026-11-09T18:10:00.001Z", "invalid_expiry"},
		{"malformed", "never", "invalid_json_or_schema"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := bytes.ReplaceAll(data, []byte("2026-11-09T18:10:00Z"), []byte(tc.expiry))
			if tc.expiry == "" {
				input = bytes.ReplaceAll(input, []byte(`"expiresAt": "",`), nil)
			}
			_, reason := validate(partition, input)
			if reason != tc.reason {
				t.Fatalf("validation reason = %q, want %q", reason, tc.reason)
			}
		})
	}
}

type memoryLedger struct {
	state string
}

func (*memoryLedger) rejection(context.Context, position) (string, error) { return "", nil }

func (*memoryLedger) reject(context.Context, position, string) error {
	return errors.New("unexpected rejection")
}

func (s *memoryLedger) prepare(context.Context, *pb.NotificationRequested, [32]byte) (string, error) {
	if s.state == "" {
		s.state = pending
	}
	return s.state, nil
}

func (s *memoryLedger) markPublished(context.Context, string) error {
	s.state = published
	return nil
}

func TestPendingReplayAfterFailedPublish(t *testing.T) {
	payload := fixture(t, "ordinary")
	hash := sha256.Sum256([]byte("fraud-0001"))
	partition := binary.BigEndian.Uint32(hash[:4]) % partitions
	batch := []iggy.IggyMessage{{Payload: payload}}
	var store memoryLedger
	var publishedCount, commits int
	kafkaUp := false
	publish := func(context.Context, []byte, []byte) error {
		if store.state != pending {
			t.Fatalf("publish with state %q, expected pending", store.state)
		}
		if !kafkaUp {
			return errors.New("Kafka unavailable")
		}
		publishedCount++
		return nil
	}
	commit := func(context.Context, position) error {
		if store.state != published {
			t.Fatalf("commit with state %q, expected published", store.state)
		}
		commits++
		return nil
	}
	process := func() error {
		return (bridge{store: &store, publish: publish}).processBatch(t.Context(), partition, batch, commit, slog.New(slog.DiscardHandler))
	}
	if err := process(); err == nil || store.state != pending || commits != 0 {
		t.Fatalf("failed publish left state %q, commits %d, error %v", store.state, commits, err)
	}
	kafkaUp = true
	for range 2 {
		if err := process(); err != nil {
			t.Fatal(err)
		}
	}
	if publishedCount != 1 || commits != 2 {
		t.Fatalf("recovery and replay produced %d Kafka records and %d commits, want 1 and 2", publishedCount, commits)
	}
}

type failMarkLedger struct{ ledger }

func (s failMarkLedger) markPublished(context.Context, string) error {
	return errors.New("injected crash after Kafka acknowledgement")
}

func TestCrashRecovery(t *testing.T) {
	connect := testPostgres(t)
	ctx := t.Context()
	first := []byte(`{
		"eventId":"361906a1-cf3e-43a5-a8bb-33c343efca22","accountId":"crash-test",
		"sequence":"1","type":"NOTIFICATION_TYPE_FRAUD_ALERT","priority":"PRIORITY_HIGH",
		"channels":["CHANNEL_SMS"],"recipient":"+15550001111","body":"private message",
		"occurredAt":"2026-11-09T18:05:00Z","producer":"fraud-engine-python",
		"traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	}`)
	second := bytes.ReplaceAll(first, []byte("361906a1"), []byte("861906a1"))
	second = bytes.ReplaceAll(second, []byte(`"sequence":"1"`), []byte(`"sequence":"2"`))
	hash := sha256.Sum256([]byte("crash-test"))
	partition := binary.BigEndian.Uint32(hash[:4]) % partitions
	messages := []iggy.IggyMessage{
		{Header: iggy.MessageHeader{Offset: 0}, Payload: first},
		{Header: iggy.MessageHeader{Offset: 3}, Payload: second},
	}
	var sent []string
	var offsets []uint64
	ack := func(_ context.Context, key, value []byte) error {
		event := new(pb.NotificationRequested)
		if err := proto.Unmarshal(value, event); err != nil || string(key) != "crash-test" {
			t.Fatalf("unexpected Kafka wire record, decode error %v", err)
		}
		sent = append(sent, event.EventId)
		return nil
	}
	commit := func(_ context.Context, pos position) error {
		offsets = append(offsets, pos.offset)
		return nil
	}
	run := func(store ledger, publish func(context.Context, []byte, []byte) error,
		batch []iggy.IggyMessage, commit func(context.Context, position) error, wantError string) {
		t.Helper()
		err := (bridge{store: store, publish: publish}).processBatch(ctx, partition, batch, commit, slog.New(slog.DiscardHandler))
		if (wantError == "" && err != nil) || (wantError != "" && (err == nil || err.Error() != wantError)) {
			t.Fatalf("processBatch error = %v, want %q", err, wantError)
		}
	}
	exec := func(db *pgx.Conn, sql string) {
		t.Helper()
		if _, err := db.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	assertState := func(db *pgx.Conn, pendingCount, publishedCount int) {
		t.Helper()
		var waiting, completed int
		err := db.QueryRow(ctx, `SELECT count(*) FILTER (WHERE publish_state='pending'),
			count(*) FILTER (WHERE publish_state='published') FROM ingested_events`).Scan(&waiting, &completed)
		if err != nil || waiting != pendingCount || completed != publishedCount {
			t.Fatalf("pending=%d published=%d, want %d/%d, error=%v", waiting, completed, pendingCount, publishedCount, err)
		}
	}
	firstID := "361906a1-cf3e-43a5-a8bb-33c343efca22"
	secondID := "861906a1-cf3e-43a5-a8bb-33c343efca22"

	db := connect()
	run(postgresLedger{db}, func(context.Context, []byte, []byte) error {
		return errors.New("injected failure before Kafka acceptance")
	}, messages, commit, "publish_kafka")
	_ = db.Close(ctx)

	db = connect()
	assertState(db, 1, 0)
	if len(offsets) != 0 || len(sent) != 0 {
		t.Fatal("first failure advanced the offset or published a record")
	}
	exec(db, "UPDATE producer_controls SET enabled = false WHERE producer = 'fraud-engine-python'")
	run(failMarkLedger{postgresLedger{db}}, ack, messages, commit, "mark_published")
	_ = db.Close(ctx)

	db = connect()
	assertState(db, 1, 0)
	if !slices.Equal(sent, []string{firstID}) || len(offsets) != 0 {
		t.Fatal("Kafka ack alone advanced the offset or allowed a later record")
	}
	exec(db, "UPDATE producer_controls SET enabled = true WHERE producer = 'fraud-engine-python'")
	run(postgresLedger{db}, ack, messages, func(context.Context, position) error {
		return errors.New("injected failure storing Iggy offset")
	}, "commit_iggy")
	_ = db.Close(ctx)

	db = connect()
	assertState(db, 0, 1)
	run(postgresLedger{db}, ack, messages, commit, "")
	replays := []iggy.IggyMessage{
		{Header: iggy.MessageHeader{Offset: 4}, Payload: append([]byte(" \n"), first...)},
		{Header: iggy.MessageHeader{Offset: 5}, Payload: second},
	}
	run(postgresLedger{db}, ack, replays, commit, "")
	if !slices.Equal(sent, []string{firstID, firstID, secondID}) ||
		!slices.Equal(offsets, []uint64{0, 3, 4, 5}) {
		t.Fatalf("expected ordered recovery and zero Kafka records for completed replays, got %d sends and offsets %v", len(sent), offsets)
	}
	assertState(db, 0, 2)

	rejected := []iggy.IggyMessage{
		{Header: iggy.MessageHeader{Offset: 6}, Payload: []byte("broken JSON")},
		{Header: iggy.MessageHeader{Offset: 7}, Payload: bytes.ReplaceAll(first, []byte("private message"), []byte("changed"))},
		{Header: iggy.MessageHeader{Offset: 8}, Payload: bytes.ReplaceAll(first, []byte("361906a1"), []byte("461906a1"))},
	}
	run(postgresLedger{db}, ack, rejected, commit, "")
	disabled := bytes.ReplaceAll(first, []byte("361906a1"), []byte("561906a1"))
	disabled = bytes.ReplaceAll(disabled, []byte(`"sequence":"1"`), []byte(`"sequence":"3"`))
	disabledBatch := []iggy.IggyMessage{{Header: iggy.MessageHeader{Offset: 9}, Payload: disabled}}
	exec(db, "UPDATE producer_controls SET enabled = false WHERE producer = 'fraud-engine-python'")
	run(postgresLedger{db}, ack, disabledBatch, func(context.Context, position) error {
		return errors.New("injected failure committing a rejected offset")
	}, "commit_iggy")
	exec(db, "UPDATE producer_controls SET enabled = true WHERE producer = 'fraud-engine-python'")
	run(postgresLedger{db}, ack, disabledBatch, commit, "")
	for i, reason := range []string{"invalid_json_or_schema", "event_id_conflict", "sequence_conflict", "producer_disabled"} {
		got, err := (postgresLedger{db}).rejection(ctx, position{partition: partition, offset: uint64(i + 6)})
		if got != reason || err != nil {
			t.Fatalf("rejection = %q, want %q, error=%v", got, reason, err)
		}
	}
	if len(sent) != 3 || !slices.Equal(offsets, []uint64{0, 3, 4, 5, 6, 7, 8, 9}) {
		t.Fatalf("rejections should only advance Iggy, got %d sends and offsets %v", len(sent), offsets)
	}
	assertState(db, 0, 2)
}

func testPostgres(t *testing.T) func() *pgx.Conn {
	t.Helper()
	dsn := os.Getenv("BRIDGE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set BRIDGE_TEST_DATABASE_URL to run the crash test against Postgres")
	}
	ctx := t.Context()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := pgx.Identifier{"bridge_test_" + rand.Text()}.Sanitize()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = admin.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
		_ = admin.Close(ctx)
	})
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	connect := func() *pgx.Conn {
		t.Helper()
		db, err := pgx.Connect(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close(context.Background()) })
		if _, err := db.Exec(ctx, "SET search_path = "+schema); err != nil {
			t.Fatal(err)
		}
		return db
	}
	db := connect()
	// The base schema already includes the 002 sequence constraints.
	for _, name := range []string{"001_schema.sql", "003_ingest_recovery.sql", "003_ingest_recovery.sql"} {
		sql, err := os.ReadFile(filepath.Join(projectRoot, "infra", "postgres", "init", name))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("apply %s: %v", name, err)
		}
	}
	_ = db.Close(ctx)
	return connect
}
