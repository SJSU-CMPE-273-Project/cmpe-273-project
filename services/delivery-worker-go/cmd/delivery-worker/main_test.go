package main

import (
	"os"
	"testing"

	"github.com/SJSU-CMPE-273-Project/cmpe-273-project/contracts"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestDecodeBridgeProtobuf(t *testing.T) {
	data, err := os.ReadFile("../../../../contracts/fixtures/ordinary.json")
	require.NoError(t, err)
	event, err := contracts.Decode(data)
	require.NoError(t, err)
	key, value, err := contracts.KafkaRecord(event)
	require.NoError(t, err)
	got, err := decode(&kgo.Record{Key: key, Value: value})
	require.NoError(t, err)
	require.Equal(t, event.EventId, got.EventId)
	_, err = decode(&kgo.Record{Key: []byte("another-account"), Value: value})
	require.Error(t, err)
	_, err = decode(&kgo.Record{Key: key, Value: []byte("invalid")})
	require.Error(t, err)
}
