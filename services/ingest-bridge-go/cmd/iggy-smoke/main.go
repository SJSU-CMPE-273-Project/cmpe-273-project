// Compatibility probe only. Commit follows validation and binary conversion;
// a production bridge must wait for its durable Kafka acknowledgement instead.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	contract "github.com/SJSU-CMPE-273-Project/cmpe-273-project/contracts"
	pb "github.com/SJSU-CMPE-273-Project/cmpe-273-project/gen/go/relayguard/v1"
	"github.com/apache/iggy/foreign/go/client"
	"github.com/apache/iggy/foreign/go/client/tcp"
	iggy "github.com/apache/iggy/foreign/go/contracts"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
func id(s string) iggy.Identifier { v, err := iggy.NewIdentifier(s); must(err); return v }
func main() {
	mode := flag.String("mode", "poll", "codec, baseline, or poll")
	group := flag.String("group", "ingest-bridge-smoke", "dedicated smoke consumer group")
	commit := flag.Bool("commit", true, "explicitly store last successfully handled offset")
	idle := flag.Duration("idle", 2*time.Second, "exit after this long without records")
	minimum := flag.Int("min", 0, "minimum number of consumed records")
	flag.Parse()
	if *mode == "codec" {
		data, err := io.ReadAll(os.Stdin)
		must(err)
		m, err := contract.Decode(data)
		must(err)
		_, value, err := contract.KafkaRecord(m)
		must(err)
		j, err := protojson.Marshal(m)
		must(err)
		must(json.NewEncoder(os.Stdout).Encode(map[string]any{"json": json.RawMessage(j), "binary": base64.StdEncoding.EncodeToString(value)}))
		return
	}
	if *mode != "baseline" && *mode != "poll" {
		log.Fatal("unknown mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c, err := client.NewIggyClient(client.WithTcp(tcp.WithServerAddress(env("IGGY_TCP_ADDRESS", "localhost:8090"))))
	must(err)
	must(c.Connect(ctx))
	defer c.Close()
	_, err = c.LoginUser(ctx, env("IGGY_ROOT_USERNAME", "iggy"), env("IGGY_ROOT_PASSWORD", "Secret123"))
	must(err)
	stream, topic, g := id("relayguard"), id("requests"), id(*group)
	groups, err := c.GetConsumerGroups(ctx, stream, topic)
	must(err)
	found := false
	for _, entry := range groups {
		if entry.Name == *group {
			found = true
		}
	}
	if !found {
		_, err = c.CreateConsumerGroup(ctx, stream, topic, *group)
		must(err)
	}
	must(c.JoinConsumerGroup(ctx, stream, topic, g))
	defer c.LeaveConsumerGroup(ctx, stream, topic, g)
	consumer := iggy.NewGroupConsumer(g)
	if *mode == "baseline" {
		// Explicit opt-in for a NEW smoke group. Never baseline the real bridge group.
		if *group == "ingest-bridge" {
			log.Fatal("refusing to reset bridge offsets")
		}
		for p := uint32(0); p < 6; p++ {
			batch, err := c.PollMessages(ctx, stream, topic, consumer, iggy.LastPollingStrategy(), 1, false, &p)
			must(err)
			if len(batch.Messages) > 0 {
				must(c.StoreConsumerOffset(ctx, consumer, stream, topic, batch.Messages[len(batch.Messages)-1].Header.Offset, &p))
			}
		}
		fmt.Println("Smoke group baseline stored")
		return
	}
	offsets := map[uint32]uint64{}
	count := 0
	last := time.Now()
	for time.Since(last) < *idle {
		for p := uint32(0); p < 6; p++ {
			strategy := iggy.NextPollingStrategy()
			if offset, ok := offsets[p]; ok {
				strategy = iggy.OffsetPollingStrategy(offset)
			}
			batch, err := c.PollMessages(ctx, stream, topic, consumer, strategy, 100, false, &p)
			must(err)
			for _, record := range batch.Messages {
				m, err := contract.Decode(record.Payload)
				must(err)
				key, value, err := contract.KafkaRecord(m)
				must(err)
				roundtrip := new(pb.NotificationRequested)
				must(proto.Unmarshal(value, roundtrip))
				if !proto.Equal(m, roundtrip) || string(key) != m.AccountId {
					log.Fatal("Kafka codec mismatch")
				}
				if *commit {
					must(c.StoreConsumerOffset(ctx, consumer, stream, topic, record.Header.Offset, &p))
				}
				offsets[p] = record.Header.Offset + 1
				must(json.NewEncoder(os.Stdout).Encode(map[string]any{"eventId": m.EventId, "accountId": m.AccountId, "sequence": fmt.Sprint(m.Sequence), "partition": p, "offset": record.Header.Offset}))
				count++
				last = time.Now()
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if count < *minimum {
		log.Fatalf("consumed %d, expected at least %d", count, *minimum)
	}
	fmt.Fprintf(os.Stderr, "Consumed %d; explicit commits=%t\n", count, *commit)
}
