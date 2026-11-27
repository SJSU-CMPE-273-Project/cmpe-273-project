package main

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

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

func TestBatchOverlapsAccountsAndCommitsAfterHandoff(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	records := []*kgo.Record{{Key: []byte("a")}, {Key: []byte("a"), Offset: 1}, {Key: []byte("b")}}
	aStarted := make(chan struct{})
	bStarted := make(chan struct{})
	var order []int64
	commits := 0
	err := handoffBatch(ctx, records, func(ctx context.Context, r *kgo.Record) error {
		if string(r.Key) == "b" {
			close(bStarted)
			select {
			case <-aStarted:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if r.Offset == 0 {
			close(aStarted)
		}
		select {
		case <-bStarted:
		case <-ctx.Done():
			return ctx.Err()
		}
		order = append(order, r.Offset)
		return nil
	}, func(_ context.Context, got ...*kgo.Record) error {
		commits++
		require.Equal(t, records, got)
		require.Equal(t, []int64{0, 1}, order)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, commits)
}

func TestBatchHandoffFailureSkipsCommit(t *testing.T) {
	failure := errors.New("signal failed")
	err := handoffBatch(t.Context(), []*kgo.Record{{}, {Offset: 1}}, func(_ context.Context, r *kgo.Record) error {
		if r.Offset == 1 {
			return failure
		}
		return nil
	}, func(context.Context, ...*kgo.Record) error {
		t.Fatal("committed after failed handoff")
		return nil
	})
	require.ErrorIs(t, err, failure)
}
