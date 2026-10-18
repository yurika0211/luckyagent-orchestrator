package delegate

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	taskstore "github.com/yurika0211/luckyagent-orchestrator/task"
)

func TestDelegateManagerCreate(t *testing.T) {
	cfg := DefaultDelegateConfig()
	dm := NewDelegateManager(cfg)

	if dm.config.MaxConcurrent != 3 {
		t.Errorf("expected max 3, got %d", dm.config.MaxConcurrent)
	}
	if dm.config.MaxChildren != 3 || dm.config.ChildMaxIterations != 5 || dm.config.ChildTimeout != 60*time.Second {
		t.Errorf("unexpected child defaults: max_children=%d max_iterations=%d timeout=%s", dm.config.MaxChildren, dm.config.ChildMaxIterations, dm.config.ChildTimeout)
	}
}

func TestDelegateInitializesSessionBeforeReturningOrExecuting(t *testing.T) {
	dm := NewDelegateManager(DefaultDelegateConfig())
	initialized := make(chan string, 1)
	dm.SetSessionInitializer(func(task *DelegateTask) error {
		initialized <- task.ExecutionSessionID
		return nil
	})
	executed := make(chan bool, 1)
	dm.SetAgentExecutor(func(ctx context.Context, _, _ string) (string, error) {
		select {
		case sessionID := <-initialized:
			executed <- sessionID == DelegateExecutionSessionID(ctx)
		default:
			executed <- false
		}
		return "done", nil
	})
	if _, err := dm.HandleDelegate(map[string]any{"description": "precreate session"}); err != nil {
		t.Fatalf("delegate: %v", err)
	}
	select {
	case ok := <-executed:
		if !ok {
			t.Fatal("executor ran before the execution session was initialized")
		}
	case <-time.After(time.Second):
		t.Fatal("delegate executor did not run")
	}
}

func TestWaitForTasksReturnsTerminalFailuresWithoutBlocking(t *testing.T) {
	dm := NewDelegateManager(DefaultDelegateConfig())
	now := time.Now()
	dm.tasks["task-failed"] = &DelegateTask{ID: "task-failed", Description: "failed", Status: StatusFailed, Error: "boom", StartedAt: now, CompletedAt: now}
	dm.tasks["task-cancelled"] = &DelegateTask{ID: "task-cancelled", Description: "cancelled", Status: StatusCancelled, Error: "cancelled", StartedAt: now, CompletedAt: now}

	result, err := dm.HandleWaitForTasks(context.Background(), map[string]any{"task_ids": []string{"task-failed", "task-cancelled"}})
	if err != nil {
		t.Fatalf("wait_for_tasks: %v", err)
	}
	var wait WaitResponse
	if err := json.Unmarshal([]byte(result), &wait); err != nil {
		t.Fatalf("decode wait response: %v", err)
	}
	if !wait.AllTerminal || wait.TimedOut || len(wait.Tasks) != 2 {
		t.Fatalf("unexpected terminal wait response: %+v", wait)
	}
	if wait.Tasks[0].Status != StatusFailed.String() || wait.Tasks[1].Status != StatusCancelled.String() {
		t.Fatalf("expected failed and cancelled statuses, got %+v", wait.Tasks)
	}
}

func TestWaitForTasksReturnsTimeoutAndPendingIDs(t *testing.T) {
	dm := NewDelegateManager(DefaultDelegateConfig())
	dm.tasks["task-running"] = &DelegateTask{ID: "task-running", Description: "running", Status: StatusRunning, StartedAt: time.Now()}

	started := time.Now()
	result, err := dm.HandleWaitForTasks(context.Background(), map[string]any{
		"task_ids":         []any{"task-running"},
		"timeout":          0.05,
		"poll_interval_ms": 10,
	})
	if err != nil {
		t.Fatalf("wait_for_tasks: %v", err)
	}
	if elapsed := time.Since(started); elapsed < 40*time.Millisecond || elapsed > time.Second {
		t.Fatalf("unexpected wait duration: %s", elapsed)
	}
	var wait WaitResponse
	if err := json.Unmarshal([]byte(result), &wait); err != nil {
		t.Fatalf("decode wait response: %v", err)
	}
	if wait.AllTerminal || !wait.TimedOut || len(wait.PendingTaskIDs) != 1 || wait.PendingTaskIDs[0] != "task-running" {
		t.Fatalf("expected timed-out pending response, got %+v", wait)
	}
}

func TestWaitForTasksValidatesIDsAndHonorsCancellation(t *testing.T) {
	dm := NewDelegateManager(DefaultDelegateConfig())
	if _, err := dm.HandleWaitForTasks(context.Background(), map[string]any{"task_ids": []any{}}); err == nil {
		t.Fatal("expected empty task_ids to fail")
	}
	if _, err := dm.HandleWaitForTasks(context.Background(), map[string]any{"task_ids": []any{"task-1", 2}}); err == nil {
		t.Fatal("expected non-string task_id to fail")
	}

	dm.tasks["task-running"] = &DelegateTask{ID: "task-running", Description: "running", Status: StatusRunning, StartedAt: time.Now()}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := dm.HandleWaitForTasks(ctx, map[string]any{"task_ids": []string{"task-running"}}); err == nil {
		t.Fatal("expected cancelled context to stop wait")
	}
}

func TestWaitForTasksReadsUnifiedStoreRecords(t *testing.T) {
	store, err := taskstore.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	record, err := store.Create(taskstore.Record{
		ID:          "external-task",
		Source:      taskstore.SourceHTTP,
		Status:      taskstore.StatusCompleted,
		Description: "completed elsewhere",
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := store.SaveResult(record.ID, "external result"); err != nil {
		t.Fatalf("SaveResult: %v", err)
	}
	dm := NewDelegateManager(DefaultDelegateConfig())
	dm.SetTaskStore(store)

	result, err := dm.HandleWaitForTasks(context.Background(), map[string]any{"task_ids": []string{record.ID}})
	if err != nil {
		t.Fatalf("wait_for_tasks: %v", err)
	}
	var wait WaitResponse
	if err := json.Unmarshal([]byte(result), &wait); err != nil {
		t.Fatalf("decode wait response: %v", err)
	}
	if !wait.AllTerminal || len(wait.Tasks) != 1 || wait.Tasks[0].Source != string(taskstore.SourceHTTP) || wait.Tasks[0].Result != "external result" {
		t.Fatalf("unexpected unified wait response: %+v", wait)
	}
}

func TestDelegateTaskPersistsExecutionMetrics(t *testing.T) {
	store, err := taskstore.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	dm := NewDelegateManager(DefaultDelegateConfig())
	dm.SetTaskStore(store)
	dm.SetAgentExecutor(func(ctx context.Context, description, contextStr string) (string, error) {
		taskID := DelegateTaskID(ctx)
		if taskID == "" {
			t.Fatal("delegate task id missing from executor context")
		}
		dm.RecordExecutionMetrics(taskID, 3, "file_read")
		return "metrics result", nil
	})
	result, err := dm.HandleDelegate(map[string]any{"description": "record metrics"})
	if err != nil {
		t.Fatalf("HandleDelegate: %v", err)
	}
	var start StartResponse
	if err := json.Unmarshal([]byte(result), &start); err != nil {
		t.Fatalf("decode start: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		record, ok, err := store.Get(start.TaskID)
		if err != nil {
			t.Fatalf("get task: %v", err)
		}
		if ok && record.Status == taskstore.StatusCompleted {
			if record.Outcome.Cost.ToolCalls != 3 || record.Outcome.Cost.Elapsed <= 0 {
				t.Fatalf("unexpected cost snapshot: %+v", record.Outcome.Cost)
			}
			if record.Metadata["last_tool"] != "file_read" {
				t.Fatalf("last tool = %q", record.Metadata["last_tool"])
			}
			status, err := dm.HandleStatus(map[string]any{"task_id": start.TaskID, "include_result": false})
			if err != nil {
				t.Fatalf("HandleStatus: %v", err)
			}
			var payload map[string]any
			if err := json.Unmarshal([]byte(status), &payload); err != nil {
				t.Fatalf("decode status: %v", err)
			}
			if payload["tool_calls"] != float64(3) || payload["last_tool"] != "file_read" {
				t.Fatalf("status metrics missing: %+v", payload)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for metrics task %s", start.TaskID)
}

func TestDelegateTaskAutoModeWritesPlannerTrace(t *testing.T) {
	store, err := taskstore.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	dm := NewDelegateManager(DefaultDelegateConfig())
	dm.SetTaskStore(store)
	dm.SetAgentExecutor(func(ctx context.Context, description, contextStr string) (string, error) {
		return "planned result", nil
	})

	result, err := dm.HandleDelegate(map[string]any{
		"description":   "Compare independent modules A, B, and C and report risks for each module",
		"mode":          "auto",
		"max_children":  3,
		"include_trace": true,
	})
	if err != nil {
		t.Fatalf("HandleDelegate: %v", err)
	}
	var start map[string]any
	if err := json.Unmarshal([]byte(result), &start); err != nil {
		t.Fatalf("parse start: %v", err)
	}
	taskID, _ := start["task_id"].(string)
	if taskID == "" || start["planner_summary"] == "" || start["trace_available"] != true {
		t.Fatalf("unexpected start response: %+v", start)
	}
	if _, ok, err := store.PlannerTrace(taskID); err != nil || !ok {
		t.Fatalf("expected planner trace artifact: ok=%t err=%v", ok, err)
	}
	record, ok, err := store.Get(taskID)
	if err != nil || !ok {
		t.Fatalf("get task record: ok=%t err=%v", ok, err)
	}
	if record.Mode == "" || record.Metadata["planner_summary"] == "" {
		t.Fatalf("expected planned task metadata, got %+v", record)
	}

	statusResult, err := dm.HandleStatus(map[string]any{
		"task_id":       taskID,
		"include_trace": true,
	})
	if err != nil {
		t.Fatalf("HandleStatus: %v", err)
	}
	var status map[string]any
	if err := json.Unmarshal([]byte(statusResult), &status); err != nil {
		t.Fatalf("parse status: %v", err)
	}
	if _, ok := status["planner_trace"].(map[string]any); !ok {
		t.Fatalf("expected planner trace in status: %+v", status)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		record, ok, err := store.Get(taskID)
		if err != nil {
			t.Fatalf("get task record: %v", err)
		}
		if ok && record.Status == taskstore.StatusCompleted {
			if _, resultOK, err := store.Result(taskID); err != nil || !resultOK {
				t.Fatalf("completed task result was not persisted: ok=%t err=%v", resultOK, err)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for task completion: %s", taskID)
}

func TestTaskStatusReadsUnifiedTaskEvents(t *testing.T) {
	store, err := taskstore.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	record, err := store.Create(taskstore.Record{
		ID:          "collab-42",
		Source:      taskstore.SourceHTTP,
		Mode:        taskstore.ModeParallel,
		Status:      taskstore.StatusCompleted,
		Description: "collab task",
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := store.AppendEvent(taskstore.Event{
		Type:     taskstore.EventCompleted,
		TaskID:   record.ID,
		Status:   taskstore.StatusCompleted,
		Evidence: []string{"collab completed"},
	}); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	if err := store.SaveResult(record.ID, "collab result"); err != nil {
		t.Fatalf("SaveResult: %v", err)
	}

	dm := NewDelegateManager(DefaultDelegateConfig())
	dm.SetTaskStore(store)
	result, err := dm.HandleStatus(map[string]any{
		"task_id":        record.ID,
		"include_events": true,
	})
	if err != nil {
		t.Fatalf("HandleStatus: %v", err)
	}
	var resp map[string]any
	if err := json.Unmarshal([]byte(result), &resp); err != nil {
		t.Fatalf("parse status: %v", err)
	}
	if resp["source"] != "http" || resp["mode"] != "parallel" || resp["result"] != "collab result" {
		t.Fatalf("unexpected unified status: %+v", resp)
	}
	if _, ok := resp["observation"].(map[string]any); !ok {
		t.Fatalf("expected observation in status: %+v", resp)
	}
	events, ok := resp["events"].([]any)
	if !ok || len(events) != 1 {
		t.Fatalf("expected one event, got %+v", resp["events"])
	}
}

func TestListTasksIncludesUnifiedStoreRecords(t *testing.T) {
	store, err := taskstore.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	_, err = store.Create(taskstore.Record{
		ID:          "collab-43",
		Source:      taskstore.SourceHTTP,
		Mode:        taskstore.ModeParallel,
		Status:      taskstore.StatusCompleted,
		Description: "collab list task",
		CreatedAt:   time.Now(),
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	dm := NewDelegateManager(DefaultDelegateConfig())
	dm.SetTaskStore(store)
	result, err := dm.HandleList(map[string]any{"source": "http"})
	if err != nil {
		t.Fatalf("HandleList: %v", err)
	}
	var resp ListResponse
	if err := json.Unmarshal([]byte(result), &resp); err != nil {
		t.Fatalf("parse list: %v", err)
	}
	if resp.Count != 1 || len(resp.Tasks) != 1 || resp.Tasks[0].TaskID != "collab-43" {
		t.Fatalf("expected unified collab task, got %+v", resp)
	}
	if resp.Tasks[0].Source != "http" || resp.Tasks[0].Mode != "parallel" {
		t.Fatalf("unexpected unified list item: %+v", resp.Tasks[0])
	}
}

func TestPrepareDelegateExecutionContextUsesDefaultWorkspace(t *testing.T) {
	workspace, contextStr, err := prepareDelegateExecutionContext("task-99", "summarize", "")
	if err != nil {
		t.Fatalf("prepareDelegateExecutionContext: %v", err)
	}
	if !strings.Contains(filepath.ToSlash(workspace), "/luckyagent-delegate/task-99") {
		t.Fatalf("expected default delegate workspace, got %q", workspace)
	}
	if got := ExtractDelegateWorkspace(contextStr); got != workspace {
		t.Fatalf("expected workspace round trip %q, got %q", workspace, got)
	}
}

func TestListTasksFilterLimitAndOrder(t *testing.T) {
	dm := NewDelegateManager(DefaultDelegateConfig())
	now := time.Now()
	dm.tasks["old-completed"] = &DelegateTask{ID: "old-completed", Description: "old", Status: StatusCompleted, StartedAt: now.Add(-2 * time.Hour), CompletedAt: now.Add(-time.Hour), Result: "done"}
	dm.tasks["new-running"] = &DelegateTask{ID: "new-running", Description: "new", Status: StatusRunning, StartedAt: now}
	dm.tasks["mid-running"] = &DelegateTask{ID: "mid-running", Description: "mid", Status: StatusRunning, StartedAt: now.Add(-time.Hour)}

	result, err := dm.HandleList(map[string]any{"status": "running", "limit": 1, "order": "desc", "include_result": true})
	if err != nil {
		t.Fatalf("HandleList: %v", err)
	}
	var resp struct {
		Tasks []struct {
			TaskID      string `json:"task_id"`
			CompletedAt string `json:"completed_at"`
		} `json:"tasks"`
		Count    int            `json:"count"`
		Total    int            `json:"total"`
		ByStatus map[string]int `json:"by_status"`
	}
	if err := json.Unmarshal([]byte(result), &resp); err != nil {
		t.Fatalf("parse list: %v", err)
	}
	if resp.Count != 1 || resp.Total != 2 {
		t.Fatalf("expected count=1 total=2, got count=%d total=%d", resp.Count, resp.Total)
	}
	if len(resp.Tasks) != 1 || resp.Tasks[0].TaskID != "new-running" {
		t.Fatalf("expected newest running task first, got %#v", resp.Tasks)
	}
	if resp.Tasks[0].CompletedAt != "" {
		t.Fatalf("running task completed_at should be empty, got %q", resp.Tasks[0].CompletedAt)
	}
	if resp.ByStatus["completed"] != 1 || resp.ByStatus["running"] != 2 {
		t.Fatalf("unexpected by_status: %#v", resp.ByStatus)
	}
}

func TestTaskStatusResultTruncationAndOmitResult(t *testing.T) {
	cfg := DefaultDelegateConfig()
	cfg.MaxResultBytesInline = 10
	dm := NewDelegateManager(cfg)
	dm.tasks["task-large"] = &DelegateTask{
		ID:          "task-large",
		Description: "large",
		Status:      StatusCompleted,
		Result:      "first paragraph\n\nsecond paragraph with extra data",
		StartedAt:   time.Now().Add(-time.Minute),
		CompletedAt: time.Now(),
	}

	result, err := dm.HandleStatus(map[string]any{"task_id": "task-large"})
	if err != nil {
		t.Fatalf("HandleStatus: %v", err)
	}
	var status map[string]any
	if err := json.Unmarshal([]byte(result), &status); err != nil {
		t.Fatalf("parse status: %v", err)
	}
	if status["result_truncated"] != true {
		t.Fatalf("expected result_truncated=true, got %#v", status)
	}
	if got := status["result"].(string); !strings.Contains(got, "truncated") {
		t.Fatalf("expected truncated result marker, got %q", got)
	}

	result, err = dm.HandleStatus(map[string]any{"task_id": "task-large", "include_result": false})
	if err != nil {
		t.Fatalf("HandleStatus without result: %v", err)
	}
	status = map[string]any{}
	if err := json.Unmarshal([]byte(result), &status); err != nil {
		t.Fatalf("parse status: %v", err)
	}
	if _, ok := status["result"]; ok {
		t.Fatalf("expected result omitted when include_result=false, got %#v", status)
	}
}

func TestDelegateTimeoutClamp(t *testing.T) {
	cfg := DefaultDelegateConfig()
	cfg.MinTimeout = 10 * time.Second
	cfg.MaxTimeout = 20 * time.Second
	cfg.Timeout = 15 * time.Second
	dm := NewDelegateManager(cfg)

	if got := dm.delegateTimeoutFromArgs(map[string]any{"timeout": 1}); got != 10*time.Second {
		t.Fatalf("expected min timeout clamp, got %v", got)
	}
	if got := dm.delegateTimeoutFromArgs(map[string]any{"timeout": 100}); got != 20*time.Second {
		t.Fatalf("expected max timeout clamp, got %v", got)
	}
	if got := dm.delegateTimeoutFromArgs(map[string]any{"timeout": 0}); got != 15*time.Second {
		t.Fatalf("expected default timeout, got %v", got)
	}
}

func TestTaskStatusString(t *testing.T) {
	tests := []struct {
		status   TaskStatus
		expected string
	}{
		{StatusPending, "pending"},
		{StatusRunning, "running"},
		{StatusCompleted, "completed"},
		{StatusFailed, "failed"},
		{StatusCancelled, "cancelled"},
	}
	for _, tt := range tests {
		if got := tt.status.String(); got != tt.expected {
			t.Errorf("TaskStatus(%d).String() = %q, want %q", tt.status, got, tt.expected)
		}
	}
}
