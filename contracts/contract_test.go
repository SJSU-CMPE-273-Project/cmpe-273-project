package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pb "github.com/SJSU-CMPE-273-Project/cmpe-273-project/gen/go/relayguard/v1"
	"google.golang.org/protobuf/proto"
)

func TestFixtures(t *testing.T) {
	files, err := filepath.Glob("fixtures/*.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) < 7 {
		t.Fatal("missing fixtures")
	}
	for _, path := range files {
		t.Run(path, func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			m, err := Decode(data)
			if err != nil {
				t.Fatal(err)
			}
			key, value, err := KafkaRecord(m)
			if err != nil {
				t.Fatal(err)
			}
			decoded := new(pb.NotificationRequested)
			if err = proto.Unmarshal(value, decoded); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(m, decoded) || string(key) != m.AccountId {
				t.Fatal("Kafka wire mismatch")
			}
		})
	}
}
func TestInvalidProfile(t *testing.T) {
	data, err := os.ReadFile("fixtures/ordinary.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, sequence := range []any{"0", "-1", "01", "9223372036854775808", "18446744073709551615", 42} {
		var doc map[string]any
		json.Unmarshal(data, &doc)
		doc["sequence"] = sequence
		value, _ := json.Marshal(doc)
		if _, err := Decode(value); err == nil {
			t.Errorf("accepted invalid sequence %v", sequence)
		}
	}
	for _, replacement := range []string{
		strings.Replace(string(data), `"PRIORITY_HIGH"`, `3`, 1),
		strings.Replace(string(data), `"2026-11-09T18:05:00Z"`, `"2026-02-30T00:00:00Z"`, 1),
		strings.Replace(string(data), `"eventId":`, `"sequence":"44", "eventId":`, 1),
	} {
		if _, err := Decode([]byte(replacement)); err == nil {
			t.Error("accepted invalid JSON profile")
		}
	}
}

func TestTraceparentRequiredAndValid(t *testing.T) {
	data, err := os.ReadFile("fixtures/ordinary.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, traceparent := range []any{
		nil, "", "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"00-00000000000000000000000000000000-00f067aa0ba902b7-01",
		"00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
		"00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-03",
	} {
		var doc map[string]any
		if err := json.Unmarshal(data, &doc); err != nil {
			t.Fatal(err)
		}
		if traceparent == nil {
			delete(doc, "traceparent")
		} else {
			doc["traceparent"] = traceparent
		}
		value, err := json.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Decode(value); err == nil {
			t.Errorf("accepted invalid traceparent %v", traceparent)
		}
	}
	unsampled := strings.Replace(string(data), "00f067aa0ba902b7-01", "00f067aa0ba902b7-00", 1)
	if _, err := Decode([]byte(unsampled)); err != nil {
		t.Fatalf("unsampled trace context rejected: %v", err)
	}
}
