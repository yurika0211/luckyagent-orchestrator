package collab

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	taskstore "github.com/yurika0211/luckyagent-orchestrator/task"
)

func restoreCollabTasks(store taskstore.Store) (map[string]*CollabTask, error) {
	records, err := store.List(taskstore.ListFilter{})
	if err != nil {
		return nil, fmt.Errorf("restore collaboration history: %w", err)
	}
	tasks := make(map[string]*CollabTask)
	for _, record := range records {
		if record.Runtime.Type != "collab" && !(record.Source == taskstore.SourceHTTP && record.Role == "orchestrator" && strings.HasPrefix(record.ID, "collab-")) {
			continue
		}
		result, _, err := store.Result(record.ID)
		if err != nil {
			return nil, err
		}
		metadata := cloneStringMap(record.Metadata)
		if metadata == nil {
			metadata = make(map[string]string)
		}
		tasks[record.ID] = &CollabTask{ID: record.ID, ParentID: record.ParentID, ExecutionSessionID: record.ExecutionSessionID,
			ParentSessionID: record.ParentSessionID, Role: record.Role, Mode: CollabMode(record.Mode),
			Description: record.Description, Input: record.Input, State: TaskState(record.Status),
			Result: result, CreatedAt: record.CreatedAt, CompletedAt: record.CompletedAt, Timeout: record.Budget.Timeout, Metadata: metadata}
	}
	for _, record := range records {
		parent := tasks[record.ParentID]
		if parent == nil || record.Runtime.Type != "collab-subtask" {
			continue
		}
		result, _, err := store.Result(record.ID)
		if err != nil {
			return nil, err
		}
		sub := &SubTask{ID: record.ID, ParentID: record.ParentID, ExecutionSessionID: record.ExecutionSessionID,
			ParentSessionID: record.ParentSessionID, Role: record.Role, AgentID: record.Runtime.AgentID,
			Description: record.Description, Input: record.Input, Output: result, State: TaskState(record.Status),
			Error: record.Outcome.UserFeedback, Verified: record.Outcome.Verified, StartedAt: record.StartedAt,
			CompletedAt: record.CompletedAt, Timeout: record.Budget.Timeout}
		if raw := record.Runtime.Metadata["profile_json"]; raw != "" {
			if err := json.Unmarshal([]byte(raw), &sub.Runtime); err != nil {
				return nil, fmt.Errorf("restore %s profile: %w", sub.ID, err)
			}
		} else {
			sub.Runtime = AgentRuntimeProfile{AgentID: sub.AgentID, CWD: record.Runtime.Workspace,
				Provider: record.Runtime.Metadata["provider"], Model: record.Runtime.Metadata["model"]}
		}
		if raw := record.Metadata["acceptance_criteria"]; raw != "" {
			if err := json.Unmarshal([]byte(raw), &sub.AcceptanceCriteria); err != nil {
				return nil, err
			}
		}
		if record.Runtime.Metadata["tool_allowlist_set"] == "true" && sub.Runtime.ToolAllowlist == nil {
			sub.Runtime.ToolAllowlist = []string{}
		}
		parent.SubTasks = append(parent.SubTasks, sub)
	}
	for _, t := range tasks {
		sort.Slice(t.SubTasks, func(i, j int) bool { return subTaskNumber(t.SubTasks[i].ID) < subTaskNumber(t.SubTasks[j].ID) })
		interrupted := t.State == TaskPending || t.State == TaskRunning
		for _, sub := range t.SubTasks {
			if sub.State == TaskPending || sub.State == TaskRunning {
				sub.State, sub.Verified, sub.CompletedAt = TaskFailed, false, time.Now()
				sub.Error = "execution interrupted by runtime restart; review before retrying"
				if err := reconcileInterruptedRecord(store, sub.ID, sub.Error); err != nil {
					return nil, err
				}
				interrupted = true
			}
		}
		if interrupted {
			t.State, t.CompletedAt = TaskFailed, time.Now()
			t.Metadata["interrupted"] = "true"
			t.Metadata["partial_failure"] = "true"
			if err := reconcileInterruptedRecord(store, t.ID, "collaboration interrupted by runtime restart; review child sessions before retrying"); err != nil {
				return nil, err
			}
		}
	}
	return tasks, nil
}

func subTaskNumber(id string) int {
	parts := strings.Split(id, "-sub-")
	n, _ := strconv.Atoi(parts[len(parts)-1])
	return n
}

func reconcileInterruptedRecord(store taskstore.Store, id, reason string) error {
	r, ok, err := store.Get(id)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("missing interrupted task %s", id)
	}
	r.Status, r.CompletedAt = taskstore.StatusFailed, time.Now()
	r.Outcome.Status, r.Outcome.Verified, r.Outcome.UserFeedback = r.Status, false, reason
	if r.Metadata == nil {
		r.Metadata = make(map[string]string)
	}
	r.Metadata["interrupted"] = "true"
	if err := store.Update(r); err != nil {
		return err
	}
	return store.AppendEvent(taskstore.Event{Type: taskstore.EventFailed, TaskID: id, ParentID: r.ParentID,
		ExecutionSessionID: r.ExecutionSessionID, Status: r.Status, Message: reason})
}
