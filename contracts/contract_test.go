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
