package delegate

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/yurika0211/luckyagent-orchestrator/collab"
	taskstore "github.com/yurika0211/luckyagent-orchestrator/task"
)

func (dm *DelegateManager) SetMDPSnapshotPath(path string) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	if err := dm.planner.LoadMDP(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	dm.mdpPath = path
	return nil
}

func (dm *DelegateManager) SetTaskPlanner(planner collab.TaskPlannerFunc) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.taskPlanner = planner
}

func (dm *DelegateManager) SetResultVerifier(verifier func(context.Context, *collab.SubTask, string) error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.resultVerifier = verifier
}

// SetQueueExecutor injects the application's durable background queue adapter.
// Queue mode is rejected if the application has not wired one.
func (dm *DelegateManager) SetQueueExecutor(executor AgentExecutorWithOptionsFunc) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.queueExecutor = executor
}

func (dm *DelegateManager) runChild(ctx context.Context, description, input string, options AgentExecutorOptions) (string, error) {
	return dm.WithExecutionSlot(ctx, func() (string, error) {
		dm.mu.RLock()
		executor, withOptions := dm.agentExecutor, dm.agentExecutorWithOptions
		dm.mu.RUnlock()
		if withOptions != nil {
			return withOptions(ctx, description, input, options)
		}
		if executor != nil {
			return executor(ctx, description, input)
		}
		return "", fmt.Errorf("no agent executor configured")
	})
}

// WithExecutionSlot shares the runtime's execution quota with other adapters.
func (dm *DelegateManager) WithExecutionSlot(ctx context.Context, run func() (string, error)) (string, error) {
	select {
	case dm.executorSlots <- struct{}{}:
		defer func() { <-dm.executorSlots }()
	case <-ctx.Done():
		return "", ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return run()
}

type modeHandler struct {
	dm             *DelegateManager
	allowRecursive bool
}

func (h modeHandler) HandleSubTask(ctx context.Context, sub *collab.SubTask) (string, error) {
	ctx = context.WithValue(ctx, delegateTaskIDContextKey{}, sub.ID)
	ctx = context.WithValue(ctx, delegateExecutionSessionIDContextKey{}, sub.ExecutionSessionID)
	return h.dm.runChild(ctx, sub.Description, sub.Input, AgentExecutorOptions{AllowRecursiveDelegate: h.allowRecursive})
}

func (h modeHandler) VerifySubTask(ctx context.Context, sub *collab.SubTask, output string) error {
	h.dm.mu.RLock()
	verify := h.dm.resultVerifier
	h.dm.mu.RUnlock()
	if verify == nil {
		return fmt.Errorf("verification required but no verifier is configured")
	}
	return verify(ctx, sub, output)
}

func (dm *DelegateManager) executeMode(ctx context.Context, parent DelegateTask, description, input string, timeout time.Duration) (string, error) {
	dm.mu.RLock()
	queue, planner, store, modePlanner, mdpPath := dm.queueExecutor, dm.taskPlanner, dm.taskStore, dm.planner, dm.mdpPath
	dm.mu.RUnlock()
	if parent.Mode == taskstore.ModeAutonomyQueue {
		if queue == nil {
			return "", fmt.Errorf("autonomy_queue executor is not configured")
		}
		return dm.WithExecutionSlot(ctx, func() (string, error) {
			return queue(ctx, description, input, AgentExecutorOptions{AllowRecursiveDelegate: parent.AllowRecursiveDelegate})
		})
	}
	mode, err := collab.ParseMode(string(parent.Mode))
	if err != nil {
		return "", err
	}
	registry := collab.NewRegistry()
	count := parent.MaxChildren
	if count <= 0 {
		count = 1
	}
	ids := syntheticAgentIDs(count)
	for i, id := range ids {
		if err := registry.Register(&collab.AgentProfile{ID: id, Name: id,
			Description: fmt.Sprintf("Collaborator %d of %d", i+1, count), Runtime: collab.AgentRuntimeProfile{CWD: parent.Workspace}}); err != nil {
			return "", err
		}
	}
	manager := collab.NewDelegateManager(registry, modeHandler{dm: dm, allowRecursive: parent.AllowRecursiveDelegate})
	manager.SetTaskPlanner(planner)
	// This transient manager writes its own new task tree, without reconciling
	// other active collaboration runs in the same application store.
	manager.AttachTaskStore(store)
	job, err := manager.DelegateChild(ctx, mode, description, input, ids, timeout, parent.ExecutionSessionID, parent.ID)
	if err != nil {
		return "", err
	}
	snapshot, err := manager.Wait(ctx, job.ID)
	if err != nil {
		_ = manager.CancelTask(job.ID)
		return "", err
	}
	outcome := "failure"
	if snapshot.State == collab.TaskCompleted {
		outcome = "success"
	} else if snapshot.Metadata["partial_failure"] == "true" {
		outcome = "partial"
	}
	modePlanner.ObserveExecution(collab.PlanRequest{Description: description, Input: parent.Context, AgentIDs: ids, Timeout: timeout}, mode, outcome, snapshot.CompletedAt.Sub(snapshot.CreatedAt))
	if mdpPath != "" {
		if err := modePlanner.SaveMDP(mdpPath); err != nil {
			return snapshot.Result, fmt.Errorf("save planner feedback: %w", err)
		}
	}
	if snapshot.State != collab.TaskCompleted {
		return snapshot.Result, fmt.Errorf("collaboration %s: %s", snapshot.State, snapshot.Result)
	}
	return snapshot.Result, nil
}
