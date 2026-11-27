package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"log/slog"
	"strings"
	"time"

	contract "github.com/SJSU-CMPE-273-Project/cmpe-273-project/contracts"
	pb "github.com/SJSU-CMPE-273-Project/cmpe-273-project/gen/go/relayguard/v1"
	"github.com/SJSU-CMPE-273-Project/cmpe-273-project/internal/telemetry"
	iggy "github.com/apache/iggy/foreign/go/contracts"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	partitions = 6
	pending    = "pending"
	published  = "published"
	duplicate  = "duplicate"
)

type position struct {
	partition uint32
	offset    uint64
}

type ledger interface {
	rejection(context.Context, position) (string, error)
	reject(context.Context, position, string, string) error
	prepare(context.Context, *pb.NotificationRequested, [32]byte) (string, error)
	markPublished(context.Context, string) error
}

type bridge struct {
	store               ledger
	publish             func(ctx context.Context, key, value []byte) error
	checkRejectionTopic func(context.Context) error
}

func (b bridge) handle(ctx context.Context, pos position, payload []byte) (outcome string, err error) {
	reason, err := b.store.rejection(ctx, pos)
	if err != nil {
		return "", errors.New("read_rejection")
	}
	if reason != "" {
		if b.checkRejectionTopic != nil {
			if err := b.checkRejectionTopic(ctx); err != nil {
				return "", err
			}
		}
		return reason, nil
	}
	event, reason := validate(pos.partition, payload)
	if reason != "" {
		return b.reject(ctx, pos, reason, contract.RejectedEventID(payload))
	}
	// One span per valid event, continuing the producer's trace. It records
	// whether the event was published, absorbed as a duplicate or rejected.
	ctx, span := telemetry.Tracer().Start(telemetry.FromTraceparent(ctx, event.Traceparent), "bridge.process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(telemetry.EventAttributes(event.EventId, event.AccountId, event.Sequence)...),
		trace.WithAttributes(attribute.Int64("iggy.partition", int64(pos.partition)), attribute.Int64("iggy.offset", int64(pos.offset))))
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetAttributes(attribute.String("relayguard.outcome", outcome))
		}
		span.End()
	}()
	key, value, err := contract.KafkaRecord(event)
	if err != nil {
		return "", errors.New("encode_kafka")
	}
	// Leave headroom below the default Kafka record batch limit for framing.
	if len(key)+len(value) > 1_000_000 {
		return b.reject(ctx, pos, "record_too_large", event.EventId)
	}
	state, err := b.store.prepare(ctx, event, sha256.Sum256(value))
	if err != nil {
		return "", errors.New("prepare_event")
	}
	switch state {
	case published:
		return duplicate, nil
	case pending:
		if err := b.publish(ctx, key, value); err != nil {
			return "", errors.New("publish_kafka")
		}
		if err := b.store.markPublished(ctx, event.EventId); err != nil {
			return "", errors.New("mark_published")
		}
		return published, nil
	case "producer_disabled", "event_id_conflict", "sequence_conflict":
		return b.reject(ctx, pos, state, event.EventId)
	default:
		return "", errors.New("invalid_publish_state")
	}
}

func (b bridge) reject(ctx context.Context, pos position, reason, eventID string) (string, error) {
	if b.checkRejectionTopic != nil {
		if err := b.checkRejectionTopic(ctx); err != nil {
			return "", err
		}
	}
	if err := b.store.reject(ctx, pos, reason, eventID); err != nil {
		return "", errors.New("persist_rejection")
	}
	return reason, nil
}

func (b bridge) processBatch(ctx context.Context, partition uint32, messages []iggy.IggyMessage,
	commit func(context.Context, position) error, logger *slog.Logger) error {
	counts := make(map[string]int)
	defer func() {
		if len(counts) > 0 {
			logger.Info("processed Iggy records", "partition", partition, "counts", counts)
		}
	}()
	for _, message := range messages {
		if err := ctx.Err(); err != nil {
			return err
		}
		pos := position{partition: partition, offset: message.Header.Offset}
		recordCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		outcome, err := b.handle(recordCtx, pos, message.Payload)
		if err != nil {
			cancel()
			return err
		}
		// Iggy stores the last processed offset. Commit only the successful
		// prefix in broker order, even when broker offsets have gaps.
		err = commit(recordCtx, pos)
		cancel()
		if err != nil {
			return errors.New("commit_iggy")
		}
		counts[outcome]++
	}
	return nil
}

func validate(partition uint32, payload []byte) (*pb.NotificationRequested, string) {
	event, err := contract.Decode(payload)
	if err != nil {
		return nil, "invalid_json_or_schema"
	}
	for _, field := range []string{event.AccountId, event.Producer, event.Recipient, event.Body} {
		if strings.TrimSpace(field) == "" || strings.ContainsRune(field, 0) {
			return nil, "invalid_fields"
		}
	}
	isOTP := event.Type == pb.NotificationType_NOTIFICATION_TYPE_OTP
	if event.ExpiresAt == nil && isOTP {
		return nil, "invalid_expiry"
	}
	if event.ExpiresAt != nil {
		ttl := event.ExpiresAt.AsTime().Sub(event.OccurredAt.AsTime())
		if ttl <= 0 || (isOTP && ttl > 5*time.Minute) {
			return nil, "invalid_expiry"
		}
	}
	hash := sha256.Sum256([]byte(event.AccountId))
	if binary.BigEndian.Uint32(hash[:4])%partitions != partition {
		return nil, "wrong_partition"
	}
	// Normalize the UUID to the same identity used by Postgres and Lane 2.
	event.EventId = strings.ToLower(event.EventId)
	return event, ""
}
