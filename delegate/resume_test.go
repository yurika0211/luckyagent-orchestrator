package delegate

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	taskstore "github.com/yurika0211/luckyagent-orchestrator/task"
)

func TestResumeFromTerminalTaskReusesExecutionSession(t *testing.T) {
	dm := NewDelegateManager(DefaultDelegateConfig())
	var seen []string
	dm.SetAgentExecutorWithOptions(func(ctx context.Context, description, contextStr string, options AgentExecutorOptions) (string, error) {
		seen = append(seen, DelegateExecutionSessionID(ctx)+":"+description+":"+contextStr)
		if strings.Contains(description, "continue") {
			if contextStr != "" {
				t.Errorf("resume forwarded context %q", contextStr)
			}
			return "continued", nil
		}
		return strings.Repeat("x", 5000), nil
	})
	owner := Owner{SessionID: "parent-1", Source: "tool", RequestKey: "same-request"}
	raw, err := dm.HandleDelegateWithOwner(map[string]any{"description": "read the file"}, owner)
	if err != nil {
		t.Fatal(err)
	}
	var started StartResponse
	if err := json.Unmarshal([]byte(raw), &started); err != nil {
		t.Fatal(err)
	}
	original := awaitDelegate(t, dm, started.TaskID)
	if original.Status != StatusCompleted || original.Result == "" {
		t.Fatalf("original=%+v", original)
	}
	beforeResult, beforeStatus := original.Result, original.Status

	resumedRaw, err := dm.HandleDelegateWithOwner(map[string]any{
		"description": "continue from the file",
		"resume_from": started.TaskID,
	}, owner)
	if err != nil {
		t.Fatal(err)
	}
	var resumed StartResponse
	if err := json.Unmarshal([]byte(resumedRaw), &resumed); err != nil {
		t.Fatal(err)
	}
	if resumed.TaskID == "" || resumed.TaskID == started.TaskID || resumed.ResumedFrom != started.TaskID {
		t.Fatalf("start=%+v", resumed)
	}
	if resumed.ExecutionSessionID != started.ExecutionSessionID {
		t.Fatalf("session %s want %s", resumed.ExecutionSessionID, started.ExecutionSessionID)
	}
	finished := awaitDelegate(t, dm, resumed.TaskID)
	if finished.Status != StatusCompleted || finished.Result != "continued" || finished.ResumedFrom != started.TaskID {
		t.Fatalf("resumed task=%+v", finished)
	}
	if len(seen) != 2 || !strings.HasPrefix(seen[1], started.ExecutionSessionID+":") {
		t.Fatalf("executor calls=%v", seen)
	}
	again := awaitDelegate(t, dm, started.TaskID)
	if again.Status != beforeStatus || again.Result != beforeResult {
		t.Fatalf("original changed status=%s result=%s", again.Status, again.Result)
	}
}

func TestResumeFromRejectsNonTerminalAndForeignTasks(t *testing.T) {
	cfg := DefaultDelegateConfig()
	cfg.MaxConcurrent = 2
	dm := NewDelegateManager(cfg)
	release := make(chan struct{})
	dm.SetAgentExecutor(func(context.Context, string, string) (string, error) {
		<-release
		return "done", nil
	})
	owner := Owner{SessionID: "parent-1"}
	raw, err := dm.HandleDelegateWithOwner(map[string]any{"description": "still running"}, owner)
	if err != nil {
		t.Fatal(err)
	}
	var running StartResponse
	if err := json.Unmarshal([]byte(raw), &running); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		dm.mu.RLock()
		status := dm.tasks[running.TaskID].Status
		dm.mu.RUnlock()
		if status == StatusRunning {
			break
		}
		time.Sleep(time.Millisecond)
	}
	before := len(dm.tasks)
	if _, err := dm.HandleDelegateWithOwner(map[string]any{
		"description": "too soon",
		"resume_from": running.TaskID,
	}, owner); err == nil || !strings.Contains(err.Error(), "running") {
		t.Fatalf("running resume err=%v", err)
	}
	if _, err := dm.HandleDelegateWithOwner(map[string]any{
		"description": "missing",
		"resume_from": "task-does-not-exist",
	}, owner); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("missing resume err=%v", err)
	}
	if len(dm.tasks) != before {
		t.Fatalf("rejected resume created a task: %d -> %d", before, len(dm.tasks))
	}
	close(release)
	if done := awaitDelegate(t, dm, running.TaskID); done.Status != StatusCompleted {
		t.Fatal(done.Error)
	}
	if _, err := dm.HandleDelegateWithOwner(map[string]any{
		"description": "other session",
		"resume_from": running.TaskID,
	}, Owner{SessionID: "parent-2"}); err == nil || !strings.Contains(err.Error(), "current session") {
		t.Fatalf("foreign resume err=%v", err)
	}
	if len(dm.tasks) != before {
		t.Fatalf("foreign resume created a task: %d", len(dm.tasks))
	}
}

func TestResumeCopiesRoleAndRecursiveFlag(t *testing.T) {
	cfg := DefaultDelegateConfig()
	cfg.ChildDisabledTools = []string{"file_write"}
	cfg.ChildModel = "child-model-a"
	dm := NewDelegateManager(cfg)
	seenOptions := map[string]AgentExecutorOptions{}
	dm.SetAgentExecutorWithOptions(func(ctx context.Context, description, _ string, options AgentExecutorOptions) (string, error) {
		seenOptions[DelegateTaskID(ctx)] = options
		return "ok", nil
	})
	owner := Owner{SessionID: "parent-1"}
	raw, err := dm.HandleDelegateWithOwner(map[string]any{
		"description":              "restricted child",
		"allow_recursive_delegate": false,
	}, owner)
	if err != nil {
		t.Fatal(err)
	}
	var started StartResponse
	if err := json.Unmarshal([]byte(raw), &started); err != nil {
		t.Fatal(err)
	}
	_ = awaitDelegate(t, dm, started.TaskID)
	dm.mu.Lock()
	dm.tasks[started.TaskID].Role = "explore"
	dm.tasks[started.TaskID].AllowRecursiveDelegate = false
	dm.mu.Unlock()

	resumedRaw, err := dm.HandleDelegateWithOwner(map[string]any{
		"description":              "resume restricted",
		"resume_from":              started.TaskID,
		"allow_recursive_delegate": true,
	}, owner)
	if err != nil {
		t.Fatal(err)
	}
	var resumed StartResponse
	if err := json.Unmarshal([]byte(resumedRaw), &resumed); err != nil {
		t.Fatal(err)
	}
	if resumed.Role != "explore" {
		t.Fatalf("role=%s", resumed.Role)
	}
	finished := awaitDelegate(t, dm, resumed.TaskID)
	if finished.Role != "explore" || finished.AllowRecursiveDelegate {
		t.Fatalf("copied policy=%+v", finished)
	}
	if len(finished.ChildDisabledTools) != 1 || finished.ChildDisabledTools[0] != "file_write" || finished.ChildModel != "child-model-a" {
		t.Fatalf("copied child config=%+v", finished)
	}
	resumedOptions := seenOptions[resumed.TaskID]
	if len(resumedOptions.DisabledTools) != 1 || resumedOptions.DisabledTools[0] != "file_write" || resumedOptions.Model != "child-model-a" || resumedOptions.AllowRecursiveDelegate {
		t.Fatalf("executor options=%+v", resumedOptions)
	}
	cfg.ChildDisabledTools = []string{"terminal"}
	cfg.ChildModel = "child-model-b"
	dm.mu.Lock()
	dm.config = cfg
	dm.mu.Unlock()
	changedRaw, err := dm.HandleDelegateWithOwner(map[string]any{
		"description": "resume after config change",
		"resume_from": started.TaskID,
	}, owner)
	if err != nil {
		t.Fatal(err)
	}
	var changed StartResponse
	if err := json.Unmarshal([]byte(changedRaw), &changed); err != nil {
		t.Fatal(err)
	}
	changedTask := awaitDelegate(t, dm, changed.TaskID)
	changedOptions := seenOptions[changed.TaskID]
	if changedTask.ChildModel != "child-model-a" || len(changedOptions.DisabledTools) != 1 || changedOptions.DisabledTools[0] != "file_write" || changedOptions.Model != "child-model-a" {
		t.Fatalf("resume followed the new config: task=%+v options=%+v", changedTask, changedOptions)
	}
}

func TestResumeFromFailedAndCancelledKeepsMode(t *testing.T) {
	dm := NewDelegateManager(DefaultDelegateConfig())
	dm.SetAgentExecutor(func(_ context.Context, _, _ string) (string, error) {
		return "", context.DeadlineExceeded
	})
	owner := Owner{SessionID: "parent-1"}
	raw, err := dm.HandleDelegateWithOwner(map[string]any{
		"description": "single child",
		"mode":        "single",
	}, owner)
	if err != nil {
		t.Fatal(err)
	}
	var failedStart StartResponse
	if err := json.Unmarshal([]byte(raw), &failedStart); err != nil {
		t.Fatal(err)
	}
	failedTask := awaitDelegate(t, dm, failedStart.TaskID)
	if failedTask.Status != StatusFailed || failedTask.Mode != taskstore.ModeSingle {
		t.Fatalf("failed=%+v", failedTask)
	}
	beforeResult, beforeErr := failedTask.Result, failedTask.Error
	resumedRaw, err := dm.HandleDelegateWithOwner(map[string]any{
		"description": "resume failed child",
		"resume_from": failedStart.TaskID,
		"mode":        "debate",
	}, owner)
	if err != nil {
		t.Fatal(err)
	}
	var resumed StartResponse
	if err := json.Unmarshal([]byte(resumedRaw), &resumed); err != nil {
		t.Fatal(err)
	}
	if resumed.TaskID == failedStart.TaskID || resumed.ResumedFrom != failedStart.TaskID || resumed.Mode != "single" {
		t.Fatalf("resume start=%+v", resumed)
	}
	if resumed.ExecutionSessionID != failedStart.ExecutionSessionID {
		t.Fatalf("session %s want %s", resumed.ExecutionSessionID, failedStart.ExecutionSessionID)
	}
	finished := awaitDelegate(t, dm, resumed.TaskID)
	if finished.Status != StatusFailed || finished.Mode != taskstore.ModeSingle {
		t.Fatalf("resumed=%+v", finished)
	}
	again := awaitDelegate(t, dm, failedStart.TaskID)
	if again.Status != StatusFailed || again.Result != beforeResult || again.Error != beforeErr {
		t.Fatalf("original changed status=%s result=%q err=%q", again.Status, again.Result, again.Error)
	}

	cfg := DefaultDelegateConfig()
	cfg.MaxConcurrent = 2
	cancelDM := NewDelegateManager(cfg)
	release := make(chan struct{})
	cancelDM.SetAgentExecutorWithOptions(func(_ context.Context, description, _ string, _ AgentExecutorOptions) (string, error) {
		if strings.Contains(description, "resume") {
			return "continued", nil
		}
		<-release
		return "should-not-finish", nil
	})
	raw, err = cancelDM.HandleDelegateWithOwner(map[string]any{
		"description": "pipeline child",
		"mode":        "pipeline",
	}, owner)
	if err != nil {
		t.Fatal(err)
	}
	var running StartResponse
	if err := json.Unmarshal([]byte(raw), &running); err != nil {
		t.Fatal(err)
	}
	if running.Mode != "pipeline" {
		t.Fatalf("planned mode=%s", running.Mode)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		cancelDM.mu.RLock()
		status := cancelDM.tasks[running.TaskID].Status
		cancelDM.mu.RUnlock()
		if status == StatusRunning {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err := cancelDM.CancelTask(running.TaskID, "stop"); err != nil {
		t.Fatal(err)
	}
	close(release)
	cancelled := awaitDelegate(t, cancelDM, running.TaskID)
	if cancelled.Status != StatusCancelled || cancelled.Mode != taskstore.ModePipeline {
		t.Fatalf("cancelled=%+v", cancelled)
	}
	beforeCancelResult := cancelled.Result
	resumedRaw, err = cancelDM.HandleDelegateWithOwner(map[string]any{
		"description": "resume cancelled child",
		"resume_from": running.TaskID,
		"mode":        "debate",
	}, owner)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(resumedRaw), &resumed); err != nil {
		t.Fatal(err)
	}
	if resumed.TaskID == running.TaskID || resumed.ResumedFrom != running.TaskID || resumed.Mode != "pipeline" {
		t.Fatalf("resume start=%+v", resumed)
	}
	if resumed.ExecutionSessionID != running.ExecutionSessionID {
		t.Fatalf("session %s want %s", resumed.ExecutionSessionID, running.ExecutionSessionID)
	}
	continued := awaitDelegate(t, cancelDM, resumed.TaskID)
	if continued.Status != StatusCompleted || continued.Mode != taskstore.ModePipeline || continued.Result != "continued" {
		t.Fatalf("resumed=%+v", continued)
	}
	again = awaitDelegate(t, cancelDM, running.TaskID)
	if again.Status != StatusCancelled || again.Result != beforeCancelResult {
		t.Fatalf("original changed status=%s result=%q", again.Status, again.Result)
	}
}
