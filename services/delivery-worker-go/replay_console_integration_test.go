package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	pb "github.com/SJSU-CMPE-273-Project/cmpe-273-project/gen/go/relayguard/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Opt-in live check against the local fake-phone stack. It uses one unique
// synthetic account, preserves all shared Oracle rules/results, and never calls
// a model. The approval HTTP request simulates the human confirmation boundary.
func TestComposeConsoleReplay(t *testing.T) {
	if os.Getenv("REPLAY_CONSOLE_INTEGRATION") != "1" {
		t.Skip("set REPLAY_CONSOLE_INTEGRATION=1 with migrated Compose services running")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	dsn := os.Getenv("DELIVERY_TEST_DATABASE_URL")
	require.NotEmpty(t, dsn)
	db, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	defer db.Close(context.Background())
	console := os.Getenv("OPS_CONSOLE_URL")
	if console == "" {
		console = "http://127.0.0.1:18084"
	}
	oracle := os.Getenv("ORACLE_URL")
	if oracle == "" {
		oracle = "http://127.0.0.1:18080"
	}
	c := &http.Client{Timeout: 10 * time.Second}
	request := func(method, endpoint, token string, input, output any) int {
		t.Helper()
		var body []byte
		if input != nil {
			body, err = json.Marshal(input)
			require.NoError(t, err)
		}
		req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-RelayGuard-Operator", "dispatch-integration")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := c.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		if output != nil {
			require.NoError(t, json.NewDecoder(resp.Body).Decode(output))
		}
		return resp.StatusCode
	}
	e := &pb.NotificationRequested{EventId: uuid.NewString(), AccountId: "dispatch-test-" + uuid.NewString(), Sequence: 1,
		Type: pb.NotificationType_NOTIFICATION_TYPE_DEPOSIT_CONFIRMED, Priority: pb.Priority_PRIORITY_NORMAL,
		Channels: []pb.Channel{pb.Channel_CHANNEL_SMS}, Recipient: "+14085550123", Body: "Synthetic console replay verification",
		Producer: "core-banking-java", OccurredAt: timestamppb.Now(), ExpiresAt: timestamppb.New(time.Now().Add(2 * time.Minute)),
		Traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}
	data, err := proto.Marshal(e)
	require.NoError(t, err)
	require.Equal(t, 200, request("POST", oracle+"/expected", os.Getenv("ORACLE_ADMIN_TOKEN"),
		map[string]any{"event_id": e.EventId, "account_id": e.AccountId, "sequence": 1, "expires_at": e.ExpiresAt.AsTime()}, nil))
	var fingerprint bool
	require.NoError(t, db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM information_schema.columns
  WHERE table_schema=current_schema() AND table_name='ingested_events' AND column_name='payload_sha256')`).Scan(&fingerprint))
	if fingerprint {
		_, err = db.Exec(ctx, `INSERT INTO ingested_events(event_id,account_id,sequence,producer,payload_sha256,publish_state)
   VALUES($1,$2,1,$3,$4,'published')`, e.EventId, e.AccountId, e.Producer, make([]byte, 32))
	} else {
		_, err = db.Exec(ctx, `INSERT INTO ingested_events(event_id,account_id,sequence,producer) VALUES($1,$2,1,$3)`, e.EventId, e.AccountId, e.Producer)
	}
	require.NoError(t, err)
	_, err = db.Exec(ctx, `INSERT INTO deliveries(event_id,account_id,sequence,priority,state,expires_at)
  VALUES($1,$2,1,2,'dead_lettered',$3)`, e.EventId, e.AccountId, e.ExpiresAt.AsTime())
	require.NoError(t, err)
	_, err = db.Exec(ctx, `INSERT INTO dead_letters(event_id,reason,original) VALUES($1,'poison_after_fix',$2)`, e.EventId, data)
	require.NoError(t, err)
	var proposalID int64
	require.NoError(t, db.QueryRow(ctx, `INSERT INTO agent_proposals(event_id,action,rationale,prompt_version,model,source)
  VALUES($1,'replay','Synthetic approval-boundary test','dlq-v2','fixture','integration_fixture') RETURNING id`, e.EventId).Scan(&proposalID))
	var queued int
	require.NoError(t, db.QueryRow(ctx, `SELECT count(*) FROM replay_requests WHERE event_id=$1`, e.EventId).Scan(&queued))
	require.Zero(t, queued)
	var approval struct {
		ReplayStatus string `json:"replay_status"`
	}
	endpoint := console + "/api/agent/proposals/" + strconv.FormatInt(proposalID, 10) + "/approve"
	require.Equal(t, 200, request("POST", endpoint, os.Getenv("OPERATOR_TOKEN"), nil, &approval))
	require.Equal(t, "approved", approval.ReplayStatus)
	require.Eventually(t, func() bool {
		var status, state string
		err := db.QueryRow(ctx, `SELECT r.status,d.state::text FROM replay_requests r JOIN deliveries d USING(event_id) WHERE r.event_id=$1`, e.EventId).Scan(&status, &state)
		return err == nil && status == "started" && state == "delivered"
	}, 40*time.Second, 200*time.Millisecond)
	var workflowID, operator, version string
	require.NoError(t, db.QueryRow(ctx, `SELECT workflow_id,operator,prompt_version FROM replay_requests WHERE event_id=$1`, e.EventId).Scan(&workflowID, &operator, &version))
	require.Equal(t, "delivery-replay/"+e.EventId, workflowID)
	require.Equal(t, "dispatch-integration", operator)
	require.Equal(t, "dlq-v2", version)
	require.Equal(t, 409, request("POST", endpoint, os.Getenv("OPERATOR_TOKEN"), nil, nil))
	require.Equal(t, 409, request("POST", console+"/api/dlq/"+e.EventId+"/replay", os.Getenv("OPERATOR_TOKEN"), nil, nil))
	tc, err := client.DialContext(ctx, client.Options{HostPort: "localhost:7233", Namespace: "relayguard"})
	require.NoError(t, err)
	defer tc.Close()
	require.Error(t, StartReplay(ctx, tc, e.EventId, "duplicate-integration"))
	var receipts []struct {
		EventID string `json:"event_id"`
	}
	require.Equal(t, 200, request("GET", oracle+"/deliveries?account_id="+url.QueryEscape(e.AccountId), "", nil, &receipts))
	require.Len(t, receipts, 1)
	require.Equal(t, e.EventId, receipts[0].EventID)
	var attempts int
	require.NoError(t, db.QueryRow(ctx, `SELECT count(*) FROM delivery_attempts WHERE event_id=$1`, e.EventId).Scan(&attempts))
	require.Equal(t, 1, attempts)
	t.Logf("console approval -> %s -> delivered exactly once; duplicate API/Temporal requests refused", workflowID)
}
