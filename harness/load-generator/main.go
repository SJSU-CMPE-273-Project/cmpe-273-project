// Package main measures the Iggy-to-Kafka ingest path with synthetic fraud events.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	pb "github.com/SJSU-CMPE-273-Project/cmpe-273-project/gen/go/relayguard/v1"
	"github.com/apache/iggy/foreign/go/client"
	"github.com/apache/iggy/foreign/go/client/tcp"
	iggy "github.com/apache/iggy/foreign/go/contracts"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const partitions = 6

type benchmark struct {
	rate      int
	duration  time.Duration
	drain     time.Duration
	batch     int
	oracleURL string
}

type result struct {
	OracleRegistered  int     `json:"oracle_registered,omitempty"`
	AccountPrefix     string  `json:"account_prefix"`
	TargetPerSecond   int     `json:"target_messages_per_second"`
	Acknowledged      int     `json:"iggy_acknowledged"`
	Published         int     `json:"bridge_published"`
	PeakBacklog       int     `json:"peak_sampled_backlog"`
	SendWindowSeconds float64 `json:"send_window_seconds"`
	DrainSeconds      float64 `json:"observed_drain_seconds"`
	AckPerSecond      float64 `json:"iggy_acknowledged_per_second"`
	BridgePerSecond   float64 `json:"bridge_published_per_second_in_send_window"`
}

func main() {
	var cfg benchmark
	rate := flag.String("rate", "normal", "normal, 10x, or events per second (0 for unpaced)")
	baseRate := flag.Int("base-rate", 10, "normal rate in events per second")
	flag.StringVar(&cfg.oracleURL, "oracle-url", "", "Oracle base URL for registering acknowledged events")
	flag.DurationVar(&cfg.duration, "duration", 120*time.Second, "send window")
	flag.DurationVar(&cfg.drain, "drain-timeout", 2*time.Minute, "time allowed for the bridge to catch up")
	flag.IntVar(&cfg.batch, "batch", 100, "maximum records per Iggy request")
	flag.Parse()
	if *baseRate < 1 || *baseRate > 100_000 {
		log.Fatal("base-rate must be in 1..100000")
	}
	switch *rate {
	case "normal":
		cfg.rate = *baseRate
	case "10x":
		cfg.rate = *baseRate * 10
	default:
		var err error
		cfg.rate, err = strconv.Atoi(*rate)
		if err != nil {
			log.Fatal("rate must be normal, 10x, or an integer")
		}
	}
	if cfg.rate < 0 || cfg.rate > 1_000_000 || cfg.batch < 1 || cfg.batch > 1000 || cfg.duration <= 0 || cfg.drain <= 0 {
		log.Fatal("rate must be in 0..1000000, batch must be 1..1000, and durations must be positive")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := cfg.run(ctx); err != nil {
		log.Fatal(err)
	}
}

func (cfg benchmark) run(ctx context.Context) error {
	setup, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	db, err := pgx.Connect(setup, env("DATABASE_URL", "postgresql://relayguard:relayguard@localhost:5432/relayguard"))
	if err != nil {
		return errors.New("connect to Postgres")
	}
	defer db.Close(context.Background())
	c, err := client.NewIggyClient(client.WithTcp(tcp.WithServerAddress(env("IGGY_TCP_ADDRESS", "localhost:8090"))))
	if err != nil {
		return err
	}
	defer c.Close()
	if err := c.Connect(setup); err != nil {
		return err
	}
	if _, err := c.LoginUser(setup, env("IGGY_ROOT_USERNAME", "iggy"), env("IGGY_ROOT_PASSWORD", "Secret123")); err != nil {
		return errors.New("log in to Iggy")
	}
	stream, _ := iggy.NewIdentifier("relayguard")
	topic, _ := iggy.NewIdentifier("requests")
	details, err := c.GetTopic(setup, stream, topic)
	if err != nil || details.PartitionsCount != partitions {
		return errors.New("expected relayguard/requests with six partitions")
	}
	cancel()

	stats := result{AccountPrefix: "fraud-load-" + uuid.NewString() + "-", TargetPerSecond: cfg.rate}
	const accounts = 60
	var accountNames [accounts]string
	var accountPartitions [accounts]uint32
	var sequence [accounts]uint64
	for i := range accountNames {
		accountNames[i] = fmt.Sprintf("%s%02d", stats.AccountPrefix, i)
		hash := sha256.Sum256([]byte(accountNames[i]))
		accountPartitions[i] = binary.BigEndian.Uint32(hash[:4]) % partitions
	}

	sample := func(ctx context.Context) error {
		query, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		// The bridge marks published only after Kafka acknowledges. Count
		// this run's accounts so concurrent demo traffic is excluded.
		err := db.QueryRow(query, `SELECT count(*) FROM ingested_events
			WHERE account_id = ANY($1) AND publish_state = 'published'`, accountNames[:]).Scan(&stats.Published)
		stats.PeakBacklog = max(stats.PeakBacklog, stats.Acknowledged-stats.Published)
		return err
	}
	if err := sample(ctx); err != nil {
		return err
	}

	oracle := &http.Client{Timeout: 5 * time.Second}
	start := time.Now()
	deadline := start.Add(cfg.duration)
	nextSample := start.Add(time.Second)
	defer func() {
		if stats.SendWindowSeconds == 0 {
			stats.SendWindowSeconds = time.Since(start).Seconds()
			stats.AckPerSecond = float64(stats.Acknowledged) / stats.SendWindowSeconds
		}
		_ = json.NewEncoder(os.Stdout).Encode(stats)
	}()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		now := time.Now()
		count := cfg.batch
		if !now.Before(deadline) {
			break
		}
		if cfg.rate > 0 {
			due := int(now.Sub(start).Seconds() * float64(cfg.rate))
			count = min(count, due-stats.Acknowledged)
		}
		if !now.Before(nextSample) {
			if err := sample(ctx); err != nil {
				return err
			}
			nextSample = now.Add(time.Second)
		}
		if count <= 0 {
			if err := wait(ctx, min(100*time.Millisecond, time.Until(deadline))); err != nil {
				return err
			}
			continue
		}
		var batches [partitions][]iggy.IggyMessage
		for i := range count {
			account := (stats.Acknowledged + i) % accounts
			sequence[account]++
			msg, err := fraudEvent(accountNames[account], sequence[account], now)
			if err != nil {
				return err
			}
			p := accountPartitions[account]
			batches[p] = append(batches[p], msg)
		}
		for p, batch := range batches {
			if len(batch) == 0 {
				continue
			}
			send, cancel := context.WithTimeout(ctx, 10*time.Second)
			_, err := c.SendMessages(send, stream, topic, iggy.PartitionId(uint32(p)), batch)
			cancel()
			if err != nil {
				return fmt.Errorf("Iggy send at partition %d: %w", p, err)
			}
			stats.Acknowledged += len(batch)
			if cfg.oracleURL != "" {
				if err := registerExpected(ctx, oracle, cfg.oracleURL, batch); err != nil {
					return err
				}
				stats.OracleRegistered += len(batch)
			}
		}
	}
	sendEnd := time.Now()
	stats.SendWindowSeconds = sendEnd.Sub(start).Seconds()
	stats.AckPerSecond = float64(stats.Acknowledged) / stats.SendWindowSeconds
	if err := sample(ctx); err != nil {
		return err
	}
	stats.BridgePerSecond = float64(stats.Published) / stats.SendWindowSeconds

	drain, cancel := context.WithDeadline(ctx, sendEnd.Add(cfg.drain))
	defer cancel()
	for stats.Published < stats.Acknowledged {
		if err := wait(drain, 250*time.Millisecond); err != nil {
			return fmt.Errorf("bridge drain: %w", err)
		}
		if err := sample(drain); err != nil {
			return fmt.Errorf("sample bridge: %w", err)
		}
		stats.DrainSeconds = time.Since(sendEnd).Seconds()
	}
	if stats.Acknowledged == 0 {
		return errors.New("no events were acknowledged")
	}
	return nil
}

func registerExpected(ctx context.Context, client *http.Client, baseURL string, batch []iggy.IggyMessage) error {
	events := make([]json.RawMessage, len(batch))
	for i, msg := range batch {
		events[i] = msg.Payload
	}
	body, err := json.Marshal(events)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+"/expected", bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	if token := os.Getenv("ORACLE_ADMIN_TOKEN"); token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("register expected events: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("register expected events: HTTP %d", response.StatusCode)
	}
	var result struct {
		Registered int `json:"registered"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return fmt.Errorf("read oracle registration: %w", err)
	}
	if result.Registered != len(batch) {
		return fmt.Errorf("oracle registered %d events, expected %d", result.Registered, len(batch))
	}
	return nil
}

func fraudEvent(account string, sequence uint64, now time.Time) (iggy.IggyMessage, error) {
	id := uuid.New()
	trace := uuid.New()
	span := uuid.New()
	payload, err := protojson.Marshal(&pb.NotificationRequested{
		EventId: id.String(), AccountId: account, Sequence: sequence,
		Type: pb.NotificationType_NOTIFICATION_TYPE_FRAUD_ALERT, Priority: pb.Priority_PRIORITY_HIGH,
		Channels: []pb.Channel{pb.Channel_CHANNEL_SMS}, Recipient: "+14085550123",
		Body: "Synthetic fraud load", OccurredAt: timestamppb.New(now.Truncate(time.Millisecond)),
		Producer: "fraud-engine-python", Traceparent: fmt.Sprintf("00-%x-%x-01", trace[:], span[:8]),
	})
	if err != nil {
		return iggy.IggyMessage{}, err
	}
	return iggy.NewIggyMessage(payload, iggy.WithID(id))
}

func wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
