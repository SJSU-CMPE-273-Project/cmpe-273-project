package delivery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/SJSU-CMPE-273-Project/cmpe-273-project/contracts"
	pb "github.com/SJSU-CMPE-273-Project/cmpe-273-project/gen/go/relayguard/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestComposeDelivery uses the running local Compose stack. It creates unique
// synthetic accounts and event-scoped faults; it never resets Oracle state or
// changes shared adapter fault modes. Run with DELIVERY_INTEGRATION=1.
func TestComposeDelivery(t *testing.T) {
	if os.Getenv("DELIVERY_INTEGRATION") != "1" {
		t.Skip("set DELIVERY_INTEGRATION=1 with the Compose stack running")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	dsn := os.Getenv("DELIVERY_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = "postgresql://relayguard:relayguard@localhost:5432/relayguard"
	}
	db, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	defer db.Close(context.Background())

	tc, err := client.DialContext(ctx, client.Options{HostPort: "localhost:7233", Namespace: "relayguard"})
	require.NoError(t, err)
	defer tc.Close()
	kafka, err := kgo.NewClient(kgo.SeedBrokers("localhost:9094"), kgo.RequiredAcks(kgo.AllISRAcks()))
	require.NoError(t, err)
	defer kafka.Close()
	httpClient := &http.Client{Timeout: 5 * time.Second}
	request := func(method, path string, input, output any) {
		t.Helper()
		var data []byte
		if input != nil {
			data, err = json.Marshal(input)
			require.NoError(t, err)
		}
		req, err := http.NewRequestWithContext(ctx, method, "http://localhost:18080"+path, bytes.NewReader(data))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		if token := os.Getenv("ORACLE_ADMIN_TOKEN"); token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := httpClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, 200, resp.StatusCode)
		if output != nil {
			require.NoError(t, json.NewDecoder(resp.Body).Decode(output))
		}
	}
	type receipt struct {
		EventID    string    `json:"event_id"`
		AccountID  string    `json:"account_id"`
		Provider   string    `json:"provider"`
		Sequence   uint64    `json:"sequence"`
		ReceivedAt time.Time `json:"received_at"`
	}
	runID := "verify-" + uuid.NewString()[:8]
	makeEvent := func(account string, seq uint64, channels ...pb.Channel) *pb.NotificationRequested {
		return &pb.NotificationRequested{EventId: uuid.NewString(), AccountId: runID + "-" + account, Sequence: seq,
			Type: pb.NotificationType_NOTIFICATION_TYPE_DEPOSIT_CONFIRMED, Priority: pb.Priority_PRIORITY_NORMAL,
			Channels: channels, Recipient: "+14085550123", Body: "Synthetic delivery verification", Producer: "core-banking-java",
			OccurredAt: timestamppb.Now(), ExpiresAt: timestamppb.New(time.Now().Add(2 * time.Minute)), Traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}
	}
	slow := makeEvent("ordered", 1, pb.Channel_CHANNEL_SMS)
	next := makeEvent("ordered", 2, pb.Channel_CHANNEL_SMS)
	fast := makeEvent("parallel", 1, pb.Channel_CHANNEL_SMS)
	secondary := makeEvent("secondary", 1, pb.Channel_CHANNEL_SMS)
	push := makeEvent("push", 1, pb.Channel_CHANNEL_SMS, pb.Channel_CHANNEL_PUSH)
	email := makeEvent("email", 1, pb.Channel_CHANNEL_EMAIL)
	email.Recipient = "verification@example.com"
	events := []*pb.NotificationRequested{slow, next, fast, secondary, push, email}
	for _, e := range events {
		request(http.MethodPost, "/expected", map[string]any{"event_id": e.EventId, "account_id": e.AccountId, "sequence": e.Sequence, "expires_at": e.ExpiresAt.AsTime()}, nil)
	}
	for _, rule := range []map[string]any{
		{"event_id": slow.EventId, "action": "fail", "times": 2},
		{"event_id": secondary.EventId, "action": "reject", "times": 1},
		{"event_id": push.EventId, "action": "reject", "times": 2},
	} {
		request(http.MethodPost, "/admin/rules", rule, nil)
	}
	publish := func(e *pb.NotificationRequested) {
		t.Helper()
		key, value, err := contracts.KafkaRecord(e)
		require.NoError(t, err)
		hash := sha256.Sum256(value)
		_, err = db.Exec(ctx, `INSERT INTO ingested_events(event_id,account_id,sequence,producer,payload_sha256,publish_state) VALUES($1,$2,$3,$4,$5,'published') ON CONFLICT(event_id) DO NOTHING`, e.EventId, e.AccountId, int64(e.Sequence), e.Producer, hash[:])
		require.NoError(t, err)
		require.NoError(t, kafka.ProduceSync(ctx, &kgo.Record{Topic: "notifications.requested", Key: key, Value: value}).FirstErr())
	}
	for _, e := range events {
		publish(e)
	}
	publish(slow) // Duplicate while the first event is retrying.
	var rows []receipt
	await := func(check func() bool) {
		t.Helper()
		deadline := time.Now().Add(40 * time.Second)
		for time.Now().Before(deadline) {
			if check() {
				return
			}
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(100 * time.Millisecond):
			}
		}
		t.Fatal("timed out waiting for delivery evidence")
	}
	read := func() { request(http.MethodGet, "/deliveries?limit=10000", nil, &rows) }
	await(func() bool {
		read()
		count := 0
		for _, r := range rows {
			for _, e := range events {
				if r.EventID == e.EventId {
					count++
				}
			}
		}
		return count >= len(events)
	})
	byID := map[string][]receipt{}
	for _, r := range rows {
		byID[r.EventID] = append(byID[r.EventID], r)
	}
	for _, e := range events {
		require.Len(t, byID[e.EventId], 1)
		description, err := tc.DescribeWorkflowExecution(ctx, e.AccountId, "")
		require.NoError(t, err)
		require.Equal(t, e.AccountId, description.WorkflowExecutionInfo.Execution.WorkflowId)
	}
	require.Equal(t, "sms_primary", byID[slow.EventId][0].Provider)
	require.Equal(t, "sms_secondary", byID[secondary.EventId][0].Provider)
	require.Equal(t, "push", byID[push.EventId][0].Provider)
	require.Equal(t, "email", byID[email.EventId][0].Provider)
	require.True(t, byID[fast.EventId][0].ReceivedAt.Before(byID[slow.EventId][0].ReceivedAt), "another account must finish while the first retries")
	require.True(t, byID[slow.EventId][0].ReceivedAt.Before(byID[next.EventId][0].ReceivedAt), "sequence 2 must wait for sequence 1")
	publish(slow) // Duplicate after successful delivery.
	// Wait for the duplicate to enter Temporal before checking Oracle again.
	describe, err := tc.DescribeWorkflowExecution(ctx, slow.AccountId, "")
	require.NoError(t, err)
	baseline := describe.WorkflowExecutionInfo.HistoryLength
	publish(slow)
	await(func() bool {
		description, err := tc.DescribeWorkflowExecution(ctx, slow.AccountId, "")
		require.NoError(t, err)
		return description.WorkflowExecutionInfo.HistoryLength > baseline
	})
	time.Sleep(500 * time.Millisecond)
	read()
	count := 0
	for _, r := range rows {
		if r.EventID == slow.EventId {
			count++
		}
	}
	require.Equal(t, 1, count)
	t.Logf("PASS %s: six unique deliveries; account ordering/isolation; duplicate signals; primary/secondary/push/email; Temporal account identities", runID)
	fmt.Fprintf(os.Stdout, "Verification account prefix: %s\n", runID)
}
