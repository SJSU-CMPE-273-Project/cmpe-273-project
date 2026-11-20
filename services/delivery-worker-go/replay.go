package delivery

import (
	"context"
	"errors"
	"strings"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

type ReplayStarter interface {
	ExecuteWorkflow(context.Context, client.StartWorkflowOptions, interface{}, ...interface{}) (client.WorkflowRun, error)
}

// StartReplay durably submits one operator replay. Canonical UUIDs ensure
// alternate spellings cannot bypass the workflow ID duplicate policy.
func StartReplay(ctx context.Context, c ReplayStarter, id, operator string) error {
	parsed, err := uuid.Parse(id)
	if err != nil || strings.TrimSpace(operator) == "" {
		return errors.New("invalid_replay_identity")
	}
	id = parsed.String()
	_, err = c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: "delivery-replay/" + id, TaskQueue: TaskQueue,
		WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
	}, ReplayWorkflow, id, strings.TrimSpace(operator))
	return err
}
