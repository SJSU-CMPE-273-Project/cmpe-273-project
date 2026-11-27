package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/SJSU-CMPE-273-Project/cmpe-273-project/contracts"
	pb "github.com/SJSU-CMPE-273-Project/cmpe-273-project/gen/go/relayguard/v1"
	delivery "github.com/SJSU-CMPE-273-Project/cmpe-273-project/services/delivery-worker-go"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil && ctx.Err() == nil {
		slog.Error("delivery worker stopped", "reason", "dependency_or_handoff_failure")
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	temporalClient, err := client.DialContext(ctx, client.Options{HostPort: env("TEMPORAL_ADDRESS", "localhost:7233"), Namespace: env("TEMPORAL_NAMESPACE", "relayguard")})
	if err != nil {
		return err
	}
	defer temporalClient.Close()
	if len(os.Args) > 1 && os.Args[1] == "replay" {
		if len(os.Args) != 4 || strings.TrimSpace(os.Args[3]) == "" {
			return errors.New("usage: delivery-worker replay EVENT_ID OPERATOR")
		}
		return delivery.StartReplay(ctx, temporalClient, os.Args[2], os.Args[3])
	}

	kafka, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(env("KAFKA_BROKERS", "localhost:9094"), ",")...),
		kgo.ConsumerGroup("delivery-worker"), kgo.ConsumeTopics("notifications.requested"), kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.RecordDeliveryTimeout(10*time.Second))
	if err != nil {
		return err
	}
	defer kafka.Close()
	publishDLQ := func(ctx context.Context, dl *pb.DeadLetter) error {
		data, err := proto.Marshal(dl)
		if err != nil {
			return errors.New("encode_dlq")
		}
		return kafka.ProduceSync(ctx, &kgo.Record{Topic: "notifications.dlq", Key: []byte(dl.GetOriginal().GetAccountId()), Value: data}).FirstErr()
	}
	w := worker.New(temporalClient, delivery.TaskQueue, worker.Options{})
	w.RegisterWorkflow(delivery.AccountWorkflow)
	w.RegisterWorkflow(delivery.ReplayWorkflow)
	adapters := &delivery.Adapters{}
	ledger := &delivery.Ledger{DSN: env("DATABASE_URL", "postgresql://relayguard:relayguard@localhost:5432/relayguard"), Adapters: adapters, Publish: publishDLQ}
	w.RegisterActivityWithOptions(ledger.Send, activity.RegisterOptions{Name: delivery.AttemptActivity})
	w.RegisterActivityWithOptions(ledger.DeadLetter, activity.RegisterOptions{Name: delivery.DeadLetterActivity})
	w.RegisterActivityWithOptions(ledger.Expire, activity.RegisterOptions{Name: delivery.ExpireActivity})
	w.RegisterActivityWithOptions(ledger.LoadReplay, activity.RegisterOptions{Name: delivery.LoadReplayActivity})
	if err := w.Start(); err != nil {
		return err
	}
	defer w.Stop()
	slog.Info("delivery worker ready")
	handoff := func(ctx context.Context, record *kgo.Record) error {
		ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		event, err := decode(record)
		if err != nil {
			return publishDLQ(ctx, &pb.DeadLetter{Original: event, Reason: "invalid_kafka_record", DeadLetteredAt: timestamppb.Now()})
		}
		return delivery.Submit(ctx, temporalClient, event)
	}
	for ctx.Err() == nil {
		fetches := kafka.PollRecords(ctx, 64)
		err := fetches.Err()
		if err == nil {
			err = handoffBatch(ctx, fetches.Records(), handoff, kafka.CommitRecords)
		}
		kafka.AllowRebalance()
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}

func handoffBatch(ctx context.Context, records []*kgo.Record, handoff func(context.Context, *kgo.Record) error, commit func(context.Context, ...*kgo.Record) error) error {
	if len(records) == 0 {
		return nil
	}
	// The Kafka key is the account ID. Preserve its signal order because the
	// workflow can advance its high-water mark as soon as a signal arrives.
	byAccount := make(map[string][]*kgo.Record)
	for _, record := range records {
		key := string(record.Key)
		byAccount[key] = append(byAccount[key], record)
	}
	batchCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var wg sync.WaitGroup
	for _, accountRecords := range byAccount {
		wg.Go(func() {
			for _, record := range accountRecords {
				if batchCtx.Err() != nil {
					return
				}
				if err := handoff(batchCtx, record); err != nil {
					cancel(err)
					return
				}
			}
		})
	}
	wg.Wait()
	if err := context.Cause(batchCtx); err != nil {
		return err
	}
	return commit(ctx, records...)
}

func decode(record *kgo.Record) (*pb.NotificationRequested, error) {
	e := new(pb.NotificationRequested)
	if proto.Unmarshal(record.Value, e) != nil {
		return nil, errors.New("invalid_protobuf")
	}
	if string(record.Key) != e.AccountId {
		return e, errors.New("account_key_mismatch")
	}
	data, err := protojson.Marshal(e)
	if err != nil {
		return e, err
	}
	checked, err := contracts.Decode(data)
	if err != nil {
		return e, err
	}
	return checked, nil
}
