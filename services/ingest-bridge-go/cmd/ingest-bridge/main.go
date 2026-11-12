package main

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/apache/iggy/foreign/go/client"
	"github.com/apache/iggy/foreign/go/client/tcp"
	iggy "github.com/apache/iggy/foreign/go/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	requestedTopic = "notifications.requested"
	// Fixed advisory lock for relayguard/requests. One session owns all partitions.
	bridgeLockID = int64(0x52454c4159470001)
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	for ctx.Err() == nil {
		err := run(ctx, logger)
		if ctx.Err() != nil {
			return
		}
		// run and the batch handler return fixed reason codes. Raw library
		// errors can contain SQL values, message data, or connection secrets.
		logger.Warn("bridge backing off", "reason", err.Error())
		if !pause(ctx, time.Second) {
			return
		}
	}
}

func run(ctx context.Context, logger *slog.Logger) error {
	setup, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	db, err := pgx.Connect(setup, env("DATABASE_URL", "postgresql://relayguard:relayguard@localhost:5432/relayguard"))
	if err != nil {
		return errors.New("connect_postgres")
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = db.Close(closeCtx)
	}()
	var locked bool
	if err := db.QueryRow(setup, "SELECT pg_try_advisory_lock($1)", bridgeLockID).Scan(&locked); err != nil {
		return errors.New("acquire_bridge_lock")
	}
	if !locked {
		return errors.New("bridge_already_active")
	}
	// Verify the migration before joining the Iggy group, even on an empty topic.
	if _, err := db.Exec(setup, `SET synchronous_commit = on;
		SELECT payload_sha256, publish_state FROM ingested_events LIMIT 0;
		SELECT reason FROM ingest_rejections LIMIT 0`); err != nil {
		return errors.New("ingest_schema_unavailable")
	}

	kafka, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(env("KAFKA_BROKERS", "localhost:9094"), ",")...),
		kgo.RequiredAcks(kgo.AllISRAcks()), // franz-go enables idempotent writes by default.
		kgo.RecordDeliveryTimeout(10*time.Second),
	)
	if err != nil {
		return errors.New("configure_kafka")
	}
	defer kafka.Close()
	b := bridge{
		store: postgresLedger{conn: db},
		publish: func(ctx context.Context, key, value []byte) error {
			return kafka.ProduceSync(ctx, &kgo.Record{Topic: requestedTopic, Key: key, Value: value}).FirstErr()
		},
	}

	c, err := client.NewIggyClient(client.WithTcp(tcp.WithServerAddress(env("IGGY_TCP_ADDRESS", "localhost:8090"))))
	if err != nil {
		return errors.New("configure_iggy")
	}
	defer c.Close()
	if err := c.Connect(setup); err != nil {
		return errors.New("connect_iggy")
	}
	if _, err := c.LoginUser(setup, env("IGGY_ROOT_USERNAME", "iggy"), env("IGGY_ROOT_PASSWORD", "Secret123")); err != nil {
		return errors.New("login_iggy")
	}
	// The constant names are valid Iggy identifiers.
	stream, _ := iggy.NewIdentifier("relayguard")
	topic, _ := iggy.NewIdentifier("requests")
	group, _ := iggy.NewIdentifier("ingest-bridge")
	details, err := c.GetTopic(setup, stream, topic)
	if err != nil {
		return errors.New("inspect_iggy_topic")
	}
	if details.PartitionsCount != partitions ||
		details.MessageExpiry != iggy.IggyExpiryNeverExpire || details.MaxTopicSize != math.MaxUint64 ||
		string(details.Options["durability"].Value) != "persisted" ||
		string(details.Options["consumer_offset_durability"].Value) != "persisted" {
		return errors.New("iggy_topic_configuration")
	}
	if err := c.JoinConsumerGroup(setup, stream, topic, group); err != nil {
		return errors.New("join_iggy_group")
	}
	cancel()
	logger.Info("bridge ready", "partitions", partitions)
	consumer := iggy.NewGroupConsumer(group)
	var next [partitions]iggy.PollingStrategy
	for partition := range next {
		next[partition] = iggy.NextPollingStrategy()
	}
	commit := func(ctx context.Context, pos position) error {
		if err := c.StoreConsumerOffset(ctx, consumer, stream, topic, pos.offset, &pos.partition); err != nil {
			return err
		}
		next[pos.partition] = iggy.OffsetPollingStrategy(pos.offset + 1)
		return nil
	}
	for ctx.Err() == nil {
		active := false
		for partition := uint32(0); partition < partitions; partition++ {
			poll, cancel := context.WithTimeout(ctx, 10*time.Second)
			// A single Iggy message can nearly fill a TCP frame. Poll one
			// record so oversized inputs reach the rejection path.
			batch, err := c.PollMessages(poll, stream, topic, consumer, next[partition], 1, false, &partition)
			cancel()
			if err != nil {
				return errors.New("poll_iggy")
			}
			if len(batch.Messages) == 0 {
				continue
			}
			if batch.PartitionId != partition {
				return errors.New("unexpected_iggy_partition")
			}
			active = true
			if err := b.processBatch(ctx, partition, batch.Messages, commit, logger); err != nil {
				return err
			}
		}
		if !active && !pause(ctx, 100*time.Millisecond) {
			break
		}
	}
	return ctx.Err()
}

func pause(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
