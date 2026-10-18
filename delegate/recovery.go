package delegate

import (
	"fmt"
	"time"

	taskstore "github.com/yurika0211/luckyagent-orchestrator/task"
)

func restoreDelegateTasks(store taskstore.Store) (map[string]*DelegateTask, error) {
	records, err := store.List(taskstore.ListFilter{})
	if err != nil {
		return nil, err
	}
	restored := make(map[string]*DelegateTask)
	for _, record := range records {
		if record.Runtime.Type != "delegate" {
			continue
		}
		result, _, err := store.Result(record.ID)
		if err != nil {
			return nil, err
		}
		status := StatusFailed
		switch record.Status {
		case taskstore.StatusCompleted:
			status = StatusCompleted
		case taskstore.StatusCancelled:
			status = StatusCancelled
		case taskstore.StatusPending, taskstore.StatusRunning:
			record.Status, record.CompletedAt = taskstore.StatusFailed, time.Now()
			record.Outcome.Status, record.Outcome.Verified = taskstore.StatusFailed, false
			record.Outcome.UserFeedback = "delegate interrupted by runtime restart; inspect child sessions and any linked autonomy task before retrying"
			if record.Metadata == nil {
				record.Metadata = make(map[string]string)
			}
			record.Metadata["interrupted"] = "true"
			if err := store.Update(record); err != nil {
				return nil, err
			}
			if err := taskstore.NewEventBus(store).Failed(record, record.Outcome.UserFeedback); err != nil {
				return nil, err
			}
		}
		restored[record.ID] = &DelegateTask{ID: record.ID, Description: record.Description, Context: record.Input, Workspace: record.Runtime.Workspace,
			Mode: record.Mode, PlannedMode: taskstore.Mode(record.Metadata["planned_mode"]), PlannerSummary: record.Metadata["planner_summary"],
			Status: status, Result: result, Error: record.Outcome.UserFeedback, StartedAt: record.StartedAt, CompletedAt: record.CompletedAt,
			OwnerSessionID: record.Metadata["owner_session_id"], ExecutionSessionID: record.ExecutionSessionID, ParentSessionID: record.ParentSessionID,
			Role: record.Role, OwnerSource: record.Metadata["owner_source"], RequestKey: record.Metadata["request_key"], MaxChildren: record.Budget.MaxChildren}
		if restored[record.ID].ExecutionSessionID == "" {
			restored[record.ID].ExecutionSessionID = delegateExecutionSessionID(record.ID)
		}
	}
	return restored, nil
}

func (dm *DelegateManager) checkAdmissionLocked() error {
	active := 0
	for _, t := range dm.tasks {
		if t.Status == StatusPending || t.Status == StatusRunning {
			active++
		}
	}
	if active >= dm.config.MaxConcurrent {
		return fmt.Errorf("max concurrent tasks reached (%d)", dm.config.MaxConcurrent)
	}
	return nil
}
