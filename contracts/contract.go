// Package contracts provides the bridge's canonical ingest validation and codec.
package contracts

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"math"
	"strings"

	pb "github.com/SJSU-CMPE-273-Project/cmpe-273-project/gen/go/relayguard/v1"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

//go:embed jsonschema/notification_requested.schema.json
var schemaJSON []byte

var schema = func() *jsonschema.Schema {
	var doc any
	if err := json.Unmarshal(schemaJSON, &doc); err != nil {
		panic(err)
	}
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	if err := c.AddResource("notification.json", doc); err != nil {
		panic(err)
	}
	s, err := c.Compile("notification.json")
	if err != nil {
		panic(err)
	}
	return s
}()

// Decode rejects JSON outside the application profile before Protobuf parsing.
func Decode(data []byte) (*pb.NotificationRequested, error) {
	// Reject duplicate keys: JSON Schema validators would otherwise see only the last.
	if err := uniqueKeys(json.NewDecoder(bytes.NewReader(data))); err != nil {
		return nil, err
	}
	var doc any
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	if err := schema.Validate(doc); err != nil {
		return nil, fmt.Errorf("invalid NotificationRequested")
	}
	m := new(pb.NotificationRequested)
	if err := protojson.Unmarshal(data, m); err != nil {
		return nil, fmt.Errorf("invalid protobuf JSON")
	}
	if m.Sequence == 0 || m.Sequence > math.MaxInt64 {
		return nil, fmt.Errorf("sequence outside BIGINT range")
	}
	return m, nil
}

func uniqueKeys(d *json.Decoder) error {
	t, err := d.Token()
	if err != nil {
		return err
	}
	if delim, ok := t.(json.Delim); ok {
		switch delim {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				key, err := d.Token()
				if err != nil {
					return err
				}
				k, ok := key.(string)
				if !ok || seen[k] {
					return fmt.Errorf("duplicate JSON key")
				}
				seen[k] = true
				if err := uniqueKeys(d); err != nil {
					return err
				}
			}
		case '[':
			for d.More() {
				if err := uniqueKeys(d); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("unexpected JSON delimiter")
		}
		_, err = d.Token()
		return err
	}
	return nil
}

// KafkaRecord is raw Protobuf (no Confluent framing); key is UTF-8 account_id.
func KafkaRecord(m *pb.NotificationRequested) ([]byte, []byte, error) {
	if m.Sequence == 0 || m.Sequence > math.MaxInt64 || strings.TrimSpace(m.AccountId) == "" {
		return nil, nil, fmt.Errorf("invalid account or sequence")
	}
	value, err := proto.Marshal(m)
	return []byte(m.AccountId), value, err
}
