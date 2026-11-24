package delivery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

type replayStarter struct {
	calls   int
	options client.StartWorkflowOptions
	args    []interface{}
}

func (s *replayStarter) ExecuteWorkflow(_ context.Context, o client.StartWorkflowOptions, _ interface{}, args ...interface{}) (client.WorkflowRun, error) {
	s.calls++
	s.options = o
	s.args = args
	return nil, nil
}
func TestReplaySubmissionRejectsDuplicateRuns(t *testing.T) {
	s := new(replayStarter)
	require.NoError(t, StartReplay(t.Context(), s, "18B8FAF5-6706-4671-8A83-95A5AA2A010A", " operator "))
	require.Equal(t, "delivery-replay/18b8faf5-6706-4671-8a83-95a5aa2a010a", s.options.ID)
	require.Equal(t, TaskQueue, s.options.TaskQueue)
	require.Equal(t, enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE, s.options.WorkflowIDReusePolicy)
	require.Equal(t, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL, s.options.WorkflowIDConflictPolicy)
	require.True(t, s.options.WorkflowExecutionErrorWhenAlreadyStarted)
	require.Equal(t, []interface{}{"18b8faf5-6706-4671-8a83-95a5aa2a010a", "operator"}, s.args)
	require.Error(t, StartReplay(t.Context(), s, "invalid", "operator"))
	require.Error(t, StartReplay(t.Context(), s, "18b8faf5-6706-4671-8a83-95a5aa2a010a", " "))
	require.Equal(t, 1, s.calls)
}
