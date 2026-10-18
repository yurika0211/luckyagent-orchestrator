package collab

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	taskstore "github.com/yurika0211/luckyagent-orchestrator/task"
)

// Successful verification is explicit in fixtures that exercise verified modes.
type acceptingTestHandler struct{ TaskHandler }

func (h acceptingTestHandler) VerifySubTask(_ context.Context, _ *SubTask, output string) error {
	if strings.TrimSpace(output) == "" {
		return fmt.Errorf("empty output")
	}
	return nil
}

func TestMessageEncodeDecode(t *testing.T) {
	msg := NewMessage(MsgTaskAssign, "agent-1", "agent-2", map[string]any{
		"task": "analyze data",
	})
	msg.WithPriority(PriorityHigh)

	data, err := msg.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, err := DecodeMessage(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded.ID != msg.ID {
		t.Errorf("ID mismatch: got %s, want %s", decoded.ID, msg.ID)
	}
	if decoded.Type != MsgTaskAssign {
		t.Errorf("Type mismatch: got %s, want %s", decoded.Type, MsgTaskAssign)
	}
	if decoded.From != "agent-1" {
		t.Errorf("From mismatch: got %s, want agent-1", decoded.From)
	}
	if decoded.Priority != PriorityHigh {
		t.Errorf("Priority mismatch: got %d, want %d", decoded.Priority, PriorityHigh)
	}
}

func TestRegistryRuntimeProfileIsolation(t *testing.T) {
	r := NewRegistry()
	profile := &AgentProfile{
		ID:           "runtime-agent",
		Name:         "Runtime Agent",
		Capabilities: []string{"code"},
		Runtime: AgentRuntimeProfile{
			Provider:      "openai",
			Model:         "gpt-test",
			ToolAllowlist: []string{"read_file"},
			CWD:           "/tmp/runtime-agent",
			AllowDelegate: false,
		},
		Metadata: map[string]string{"tier": "test"},
	}
	if err := r.Register(profile); err != nil {
		t.Fatalf("Register: %v", err)
	}
	got, ok := r.Get("runtime-agent")
	if !ok {
		t.Fatal("expected runtime-agent")
	}
	got.Runtime.ToolAllowlist[0] = "write_file"
	got.Metadata["tier"] = "mutated"
	again, _ := r.Get("runtime-agent")
	if again.Runtime.ToolAllowlist[0] != "read_file" || again.Metadata["tier"] != "test" {
		t.Fatalf("registry returned mutable profile: %+v", again)
	}
	if again.Runtime.AgentID != "runtime-agent" {
		t.Fatalf("expected runtime agent id to default, got %+v", again.Runtime)
	}
}

func TestMessageValidate(t *testing.T) {
	tests := []struct {
		name    string
		msg     *Message
		wantErr bool
	}{
		{
			name:    "valid message",
			msg:     NewMessage(MsgTaskAssign, "agent-1", "agent-2", nil),
			wantErr: false,
		},
		{
			name: "missing ID",
			msg: &Message{
				Type:      MsgTaskAssign,
				From:      "agent-1",
				Timestamp: time.Now(),
			},
			wantErr: true,
		},
		{
			name: "missing type",
			msg: &Message{
				ID:        "msg-1",
				From:      "agent-1",
				Timestamp: time.Now(),
			},
			wantErr: true,
		},
		{
			name: "missing from",
			msg: &Message{
				ID:        "msg-1",
				Type:      MsgTaskAssign,
				Timestamp: time.Now(),
			},
			wantErr: true,
		},
		{
			name: "missing timestamp",
			msg: &Message{
				ID:   "msg-1",
				Type: MsgTaskAssign,
				From: "agent-1",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.msg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRegistryRegister(t *testing.T) {
	r := NewRegistry()

	profile := &AgentProfile{
		ID:           "agent-1",
		Name:         "Test Agent",
		Description:  "A test agent",
		Capabilities: []string{"analysis", "coding"},
	}

	err := r.Register(profile)
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	// 验证注册成功
	p, ok := r.Get("agent-1")
	if !ok {
		t.Fatal("agent not found after registration")
	}
	if p.Name != "Test Agent" {
		t.Errorf("Name mismatch: got %s, want Test Agent", p.Name)
	}
	if p.Status != StatusOnline {
		t.Errorf("Status should be online, got %s", p.Status)
	}
}

func TestRegistryDeregister(t *testing.T) {
	r := NewRegistry()

	profile := &AgentProfile{ID: "agent-1", Name: "Test"}
	_ = r.Register(profile)

	err := r.Deregister("agent-1")
	if err != nil {
		t.Fatalf("deregister: %v", err)
	}

	_, ok := r.Get("agent-1")
	if ok {
		t.Error("agent should not exist after deregistration")
	}
}

func TestRegistryListByCapability(t *testing.T) {
	r := NewRegistry()

	_ = r.Register(&AgentProfile{ID: "agent-1", Capabilities: []string{"analysis", "coding"}})
	_ = r.Register(&AgentProfile{ID: "agent-2", Capabilities: []string{"writing"}})
	_ = r.Register(&AgentProfile{ID: "agent-3", Capabilities: []string{"coding", "testing"}})

	codingAgents := r.ListByCapability("coding")
	if len(codingAgents) != 2 {
		t.Errorf("expected 2 coding agents, got %d", len(codingAgents))
	}
}

func TestRegistryHealthCheck(t *testing.T) {
	r := NewRegistry()

	// 注册一个 agent，手动设置 LastSeen 为很久以前
	profile := &AgentProfile{ID: "agent-1", Status: StatusOnline}
	profile.LastSeen = time.Now().Add(-10 * time.Minute)
	_ = r.Register(profile)

	// 5 分钟超时
	stale := r.HealthCheck(5 * time.Minute)
	if stale != 1 {
		t.Errorf("expected 1 stale agent, got %d", stale)
	}

	p, _ := r.Get("agent-1")
	if p.Status != StatusOffline {
		t.Errorf("agent should be offline after health check, got %s", p.Status)
	}
}

func TestRegistryCount(t *testing.T) {
	r := NewRegistry()

	_ = r.Register(&AgentProfile{ID: "agent-1", Status: StatusOnline})
	_ = r.Register(&AgentProfile{ID: "agent-2", Status: StatusBusy})
	_ = r.Register(&AgentProfile{ID: "agent-3", Status: StatusOffline})

	total, online, busy, offline := r.Count()
	if total != 3 {
		t.Errorf("total: got %d, want 3", total)
	}
	if online != 1 {
		t.Errorf("online: got %d, want 1", online)
	}
	if busy != 1 {
		t.Errorf("busy: got %d, want 1", busy)
	}
	if offline != 1 {
		t.Errorf("offline: got %d, want 1", offline)
	}
}

func TestDelegateManagerPipeline(t *testing.T) {
	r := NewRegistry()
	_ = r.Register(&AgentProfile{ID: "agent-1", Name: "Agent 1"})
	_ = r.Register(&AgentProfile{ID: "agent-2", Name: "Agent 2"})

	// Mock handler - 每次调用添加后缀
	handler := TaskHandlerFunc(func(ctx context.Context, task *SubTask) (string, error) {
		return task.Input + "->processed_by_" + task.AgentID, nil
	})

	dm := NewDelegateManager(r, acceptingTestHandler{handler})

	task, err := dm.Delegate(context.Background(), ModePipeline, "test pipeline", "input", []string{"agent-1", "agent-2"}, 10*time.Second)
	if err != nil {
		t.Fatalf("delegate: %v", err)
	}
	if task.ExecutionSessionID != "collab-"+task.ID || task.Role != "orchestrator" {
		t.Fatalf("unexpected task execution references: %+v", task)
	}
	for _, sub := range task.SubTasks {
		if sub.ExecutionSessionID != "collab-"+sub.ID || sub.Role != "subagent" {
			t.Fatalf("unexpected subtask execution references: %+v", sub)
		}
	}

	// 等待完成
	time.Sleep(100 * time.Millisecond)

	updated, ok := dm.GetTask(task.ID)
	if !ok {
		t.Fatal("task not found")
	}

	if updated.State != TaskCompleted {
		t.Errorf("task state: got %s, want completed", updated.State)
	}

	// Pipeline: input -> agent-1 -> agent-2
	if !strings.Contains(updated.Result, "Original task: test pipeline") || !strings.Contains(updated.Result, "input->processed_by_agent-1->processed_by_agent-2") {
		t.Errorf("pipeline lost original request or stage output: %s", updated.Result)
	}
}

func TestDelegateManagerPersistsChildExecutionSessionsAndProgress(t *testing.T) {
	r := NewRegistry()
	_ = r.Register(&AgentProfile{ID: "agent-1", Name: "Agent 1"})
	store, err := taskstore.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	dm := NewDelegateManager(r, TaskHandlerFunc(func(ctx context.Context, task *SubTask) (string, error) {
		return "child result", nil
	}))
	dm.SetTaskStore(store)
	parent, err := dm.DelegateWithSession(context.Background(), ModeParallel, "persist child", "input", []string{"agent-1"}, time.Second, 0, "parent-session")
	if err != nil {
		t.Fatalf("delegate: %v", err)
	}
	child := parent.SubTasks[0]

	// In-memory TaskCompleted flips before deferred store finalization. Wait for
	// durable parent/child completion so TempDir cleanup cannot race writers.
	deadline := time.Now().Add(2 * time.Second)
	var childRecord, parentRecord taskstore.Record
	var ok bool
	for {
		current, found := dm.GetTask(parent.ID)
		if !found || current.State != TaskCompleted {
			if time.Now().After(deadline) {
				t.Fatalf("parent task did not complete in memory")
			}
			time.Sleep(5 * time.Millisecond)
			continue
		}
		childRecord, ok, err = store.Get(child.ID)
		if err != nil {
			t.Fatalf("get child record: %v", err)
		}
		parentRecord, found, err = store.Get(parent.ID)
		if err != nil {
			t.Fatalf("get parent record: %v", err)
		}
		if ok && found && childRecord.Status == taskstore.StatusCompleted && parentRecord.Status == taskstore.StatusCompleted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("delegate persistence incomplete: child_ok=%t child=%+v parent_ok=%t parent=%+v", ok, childRecord, found, parentRecord)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if childRecord.ParentID != parent.ID || childRecord.ExecutionSessionID != child.ExecutionSessionID || childRecord.ParentSessionID != "parent-session" {
		t.Fatalf("child execution references not persisted: %+v", childRecord)
	}
	events, err := store.Events(parent.ID)
	if err != nil {
		t.Fatalf("parent events: %v", err)
	}
	var progress int
	for _, event := range events {
		if event.Type == taskstore.EventProgress && event.ChildID == child.ID {
			progress++
		}
	}
	if progress < 2 {
		t.Fatalf("expected child start and completion progress events, got %+v", events)
	}
}

func TestDelegateManagerDebatePublishesTurnsAndVotes(t *testing.T) {
	r := NewRegistry()
	_ = r.Register(&AgentProfile{ID: "agent-1", Name: "Agent 1"})
	dm := NewDelegateManager(r, TaskHandlerFunc(func(ctx context.Context, task *SubTask) (string, error) {
		return "agent-1", nil
	}))
	task, err := dm.Delegate(context.Background(), ModeDebate, "debate topic", "input", []string{"agent-1"}, time.Second)
	if err != nil {
		t.Fatalf("delegate: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		current, ok := dm.GetTask(task.ID)
		if ok && current.State == TaskCompleted {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	messages := dm.MessageBus().List(MessageFilter{TaskID: task.ID, Limit: 200})
	var turns, votes int
	for _, message := range messages {
		switch message.Type {
		case MsgDebateTurn:
			turns++
			if message.Payload["round"] == nil || message.Payload["total_rounds"] == nil {
				t.Fatalf("debate turn lacks round metadata: %+v", message)
			}
		case MsgDebateVote:
			votes++
		}
	}
	if turns == 0 || votes == 0 {
		t.Fatalf("expected debate turn and vote messages, got %+v", messages)
	}
}

func TestDelegateManagerPassesRuntimeProfileToSubTask(t *testing.T) {
	r := NewRegistry()
	cwd := filepath.Join(t.TempDir(), "runtime-agent")
	_ = r.Register(&AgentProfile{
		ID:   "agent-runtime",
		Name: "Runtime Agent",
		Runtime: AgentRuntimeProfile{
			Provider:        "openai",
			Model:           "gpt-test",
			ToolAllowlist:   []string{"read_file"},
			CWD:             cwd,
			MemoryNamespace: "runtime-agent-memory",
			ApprovalPolicy:  "readonly",
			AllowDelegate:   false,
		},
	})
	seen := make(chan AgentRuntimeProfile, 1)
	handler := TaskHandlerFunc(func(ctx context.Context, task *SubTask) (string, error) {
		seen <- task.Runtime
		return "ok", nil
	})
	dm := NewDelegateManager(r, handler)
	if _, err := dm.Delegate(context.Background(), ModeParallel, "test runtime", "input", []string{"agent-runtime"}, time.Second); err != nil {
		t.Fatalf("delegate: %v", err)
	}
	select {
	case runtime := <-seen:
		if runtime.Provider != "openai" || runtime.Model != "gpt-test" || runtime.CWD != cwd || runtime.AllowDelegate {
			t.Fatalf("unexpected runtime profile: %+v", runtime)
		}
	case <-time.After(time.Second):
		t.Fatal("handler did not receive runtime profile")
	}
}

func TestDelegateManagerAppliesRuntimeProfileConstraints(t *testing.T) {
	cwd := filepath.Join(t.TempDir(), "runtime-cwd")
	handler := TaskHandlerFunc(func(ctx context.Context, task *SubTask) (string, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("expected runtime timeout deadline")
		}
		if remaining := time.Until(deadline); remaining > 200*time.Millisecond {
			t.Fatalf("expected runtime timeout to constrain deadline, remaining=%s", remaining)
		}
		for _, want := range []string{
			runtimeConstraintMarker,
			"Tool allowlist: read_file, grep",
			"Working directory: " + cwd,
			"Memory namespace: runtime-memory",
			"Recursive delegation: disabled",
		} {
			if !strings.Contains(task.Input, want) {
				t.Fatalf("expected runtime constraint %q in input:\n%s", want, task.Input)
			}
		}
		return "ok", nil
	})
	dm := NewDelegateManager(NewRegistry(), handler)
	sub := &SubTask{
		ID:    "runtime-sub",
		Input: "original input",
		Runtime: AgentRuntimeProfile{
			ToolAllowlist:   []string{"read_file", "grep"},
			CWD:             cwd,
			MemoryNamespace: "runtime-memory",
			Timeout:         50 * time.Millisecond,
			AllowDelegate:   false,
		},
		Timeout: time.Second,
	}
	if _, err := dm.executeSubTask(context.Background(), sub); err != nil {
		t.Fatalf("executeSubTask: %v", err)
	}
	if sub.Timeout != 50*time.Millisecond {
		t.Fatalf("expected runtime timeout on subtask, got %s", sub.Timeout)
	}
}

func TestDelegateManagerPolicyDeniesTooManyChildren(t *testing.T) {
	r := NewRegistry()
	_ = r.Register(&AgentProfile{ID: "agent-1", Name: "Agent 1"})
	_ = r.Register(&AgentProfile{ID: "agent-2", Name: "Agent 2"})
	handler := TaskHandlerFunc(func(ctx context.Context, task *SubTask) (string, error) {
		return "ok", nil
	})
	dm := NewDelegateManager(r, handler)
	dm.SetPolicy(taskstore.Policy{MaxChildren: 1})

	_, err := dm.Delegate(context.Background(), ModeParallel, "too many children", "input", []string{"agent-1", "agent-2"}, time.Second)
	if err == nil || !strings.Contains(err.Error(), "child count") {
		t.Fatalf("expected child count policy error, got %v", err)
	}
	if tasks := dm.ListTasks(); len(tasks) != 0 {
		t.Fatalf("policy-denied task should not be created, got %+v", tasks)
	}
}

func TestDelegateManagerParallel(t *testing.T) {
	r := NewRegistry()
	_ = r.Register(&AgentProfile{ID: "agent-1", Name: "Agent 1"})
	_ = r.Register(&AgentProfile{ID: "agent-2", Name: "Agent 2"})
	_ = r.Register(&AgentProfile{ID: "agent-3", Name: "Agent 3"})

	var callCount int64 // 使用 int64 以便原子操作
	handler := TaskHandlerFunc(func(ctx context.Context, task *SubTask) (string, error) {
		atomic.AddInt64(&callCount, 1) // 原子递增，避免 data race
		return "result_from_" + task.AgentID, nil
	})

	dm := NewDelegateManager(r, acceptingTestHandler{handler})

	task, err := dm.Delegate(context.Background(), ModeParallel, "test parallel", "input", []string{"agent-1", "agent-2", "agent-3"}, 10*time.Second)
	if err != nil {
		t.Fatalf("delegate: %v", err)
	}

	// 等待完成
	time.Sleep(100 * time.Millisecond)

	updated, ok := dm.GetTask(task.ID)
	if !ok {
		t.Fatal("task not found")
	}

	if updated.State != TaskCompleted {
		t.Errorf("task state: got %s, want completed", updated.State)
	}

	// 所有 agent 都应该被调用
	if atomic.LoadInt64(&callCount) != 3 {
		t.Errorf("call count: got %d, want 3", callCount)
	}
}

func TestDelegateManagerParallelRetryPolicyRetriesFailedSubTask(t *testing.T) {
	r := NewRegistry()
	_ = r.Register(&AgentProfile{ID: "agent-1", Name: "Agent 1"})
	_ = r.Register(&AgentProfile{ID: "agent-2", Name: "Agent 2"})

	var attempts int64
	handler := TaskHandlerFunc(func(ctx context.Context, task *SubTask) (string, error) {
		if task.AgentID == "agent-1" && atomic.AddInt64(&attempts, 1) == 1 {
			return "", context.DeadlineExceeded
		}
		return "result_from_" + task.AgentID, nil
	})
	dm := NewDelegateManager(r, acceptingTestHandler{handler})

	task, err := dm.Delegate(context.Background(), ModeParallel, "并行比较删除 verifier 风险", "input", []string{"agent-1", "agent-2"}, 10*time.Second)
	if err != nil {
		t.Fatalf("delegate: %v", err)
	}
	time.Sleep(150 * time.Millisecond)

	updated, ok := dm.GetTask(task.ID)
	if !ok {
		t.Fatal("task not found")
	}
	if updated.State != TaskCompleted {
		t.Fatalf("task state: got %s, want completed; result=%s", updated.State, updated.Result)
	}
	if atomic.LoadInt64(&attempts) != 2 {
		t.Fatalf("expected agent-1 to be retried once, attempts=%d", attempts)
	}
}

func TestDelegateManagerVerifierRejectsEmptyOutput(t *testing.T) {
	r := NewRegistry()
	_ = r.Register(&AgentProfile{ID: "agent-1", Name: "Agent 1"})

	handler := TaskHandlerFunc(func(ctx context.Context, task *SubTask) (string, error) {
		return "", nil
	})
	dm := NewDelegateManager(r, handler)

	task, err := dm.Delegate(context.Background(), ModePipeline, "先执行，再 verifier 测试", "input", []string{"agent-1"}, 10*time.Second)
	if err != nil {
		t.Fatalf("delegate: %v", err)
	}
	time.Sleep(150 * time.Millisecond)

	updated, ok := dm.GetTask(task.ID)
	if !ok {
		t.Fatal("task not found")
	}
	if updated.State != TaskFailed {
		t.Fatalf("task state: got %s, want failed; result=%s", updated.State, updated.Result)
	}
	if len(updated.SubTasks) == 0 || !strings.Contains(updated.SubTasks[0].Error, "empty output") {
		t.Fatalf("expected verification error, subtasks=%+v", updated.SubTasks)
	}
}

func TestDelegateManagerCancel(t *testing.T) {
	r := NewRegistry()
	_ = r.Register(&AgentProfile{ID: "agent-1", Name: "Agent 1"})

	// 慢 handler
	handler := TaskHandlerFunc(func(ctx context.Context, task *SubTask) (string, error) {
		time.Sleep(2 * time.Second)
		return "done", nil
	})

	dm := NewDelegateManager(r, handler)

	task, _ := dm.Delegate(context.Background(), ModePipeline, "test cancel", "input", []string{"agent-1"}, 10*time.Second)

	// 立即取消
	err := dm.CancelTask(task.ID)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}

	updated, _ := dm.GetTask(task.ID)
	if updated.State != TaskCancelled {
		t.Errorf("task state: got %s, want cancelled", updated.State)
	}
}

func TestDelegateManagerStats(t *testing.T) {
	r := NewRegistry()
	_ = r.Register(&AgentProfile{ID: "agent-1", Name: "Agent 1"})

	handler := TaskHandlerFunc(func(ctx context.Context, task *SubTask) (string, error) {
		return "done", nil
	})

	dm := NewDelegateManager(r, handler)

	// 创建多个任务
	_, _ = dm.Delegate(context.Background(), ModeParallel, "task 1", "input", []string{"agent-1"}, 10*time.Second)
	_, _ = dm.Delegate(context.Background(), ModeParallel, "task 2", "input", []string{"agent-1"}, 10*time.Second)

	time.Sleep(100 * time.Millisecond)

	total, running, completed, _ := dm.Stats()
	if total != 2 {
		t.Errorf("total: got %d, want 2", total)
	}
	// running + completed 应该等于 total
	if running+completed != total {
		t.Errorf("running+completed: got %d+%d=%d, want %d", running, completed, running+completed, total)
	}
}

func TestAggregatorConcat(t *testing.T) {
	a := NewAggregator()
	results := []string{"result1", "result2", "result3"}

	agg := a.Aggregate(AggConcat, results)

	if agg.Strategy != AggConcat {
		t.Errorf("strategy: got %s, want concat", agg.Strategy)
	}
	if agg.Count != 3 {
		t.Errorf("count: got %d, want 3", agg.Count)
	}
	if agg.Output != "result1\n\n---\n\nresult2\n\n---\n\nresult3" {
		t.Errorf("output mismatch: %s", agg.Output)
	}
}

func TestAggregatorBest(t *testing.T) {
	a := NewAggregator()
	results := []string{"short", "medium length result here", "very long result that should be the best because it has the most content"}

	agg := a.Aggregate(AggBest, results)

	if agg.Strategy != AggBest {
		t.Errorf("strategy: got %s, want best", agg.Strategy)
	}
	if agg.BestIndex != -1 || agg.Metadata["selection"] != "unscored" || !strings.Contains(agg.Output, results[0]) || !strings.Contains(agg.Output, results[2]) {
		t.Errorf("unscored candidates must be preserved: %+v", agg)
	}
}

func TestAggregatorVote(t *testing.T) {
	a := NewAggregator()
	// 两个相似结果，一个不同
	results := []string{
		"same prefix result one",
		"same prefix result two",
		"different result",
	}

	agg := a.Aggregate(AggVote, results)

	if agg.Strategy != AggVote {
		t.Errorf("strategy: got %s, want vote", agg.Strategy)
	}
	// 应该有投票结果
	if len(agg.Votes) == 0 {
		t.Error("votes should not be empty")
	}
}

func TestAggregatorMerge(t *testing.T) {
	a := NewAggregator()
	results := []string{
		"line1\nline2\nline3",
		"line2\nline4",
		"line1\nline5",
	}

	agg := a.Aggregate(AggMerge, results)

	if agg.Strategy != AggMerge {
		t.Errorf("strategy: got %s, want merge", agg.Strategy)
	}
	// 应该去重
	if agg.Count != 3 {
		t.Errorf("count: got %d, want 3", agg.Count)
	}
}

func TestAggregatorSummary(t *testing.T) {
	a := NewAggregator()
	results := []string{
		"short",
		strings.Repeat("x", 300), // 长结果
	}

	agg := a.Aggregate(AggSummary, results)

	if agg.Strategy != AggSummary {
		t.Errorf("strategy: got %s, want summary", agg.Strategy)
	}
	// 长结果应该被截断
	if !strings.Contains(agg.Output, "...") {
		t.Error("long result should be truncated with ...")
	}
}

func TestAggregatorEmpty(t *testing.T) {
	a := NewAggregator()

	agg := a.Aggregate(AggConcat, []string{})

	if agg.Count != 0 {
		t.Errorf("count: got %d, want 0", agg.Count)
	}
	if agg.Output != "" {
		t.Errorf("output should be empty, got %s", agg.Output)
	}
}

func TestModeConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  ModeConfig
		wantErr bool
	}{
		{
			name:    "valid pipeline",
			config:  DefaultModeConfig(ModePipeline),
			wantErr: false,
		},
		{
			name:    "valid parallel",
			config:  DefaultModeConfig(ModeParallel),
			wantErr: false,
		},
		{
			name:    "valid debate",
			config:  DefaultModeConfig(ModeDebate),
			wantErr: false,
		},
		{
			name:    "invalid mode",
			config:  ModeConfig{Mode: CollabMode("invalid")},
			wantErr: true,
		},
		{
			name:    "invalid debate rounds",
			config:  ModeConfig{Mode: ModeDebate, DebateRounds: 0},
			wantErr: true,
		},
		{
			name:    "invalid max concurrent",
			config:  ModeConfig{Mode: ModeParallel, MaxConcurrent: 0},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseMode(t *testing.T) {
	tests := []struct {
		input    string
		expected CollabMode
		wantErr  bool
	}{
		{"pipeline", ModePipeline, false},
		{"Pipeline", ModePipeline, false},
		{"PIPELINE", ModePipeline, false},
		{"parallel", ModeParallel, false},
		{"debate", ModeDebate, false},
		{"  AuTo  ", ModeAuto, false},
		{"mdp", "", true},
		{"invalid", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			mode, err := ParseMode(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseMode() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && mode != tt.expected {
				t.Errorf("mode: got %s, want %s", mode, tt.expected)
			}
		})
	}
}

func TestParseAggregationStrategy(t *testing.T) {
	tests := []struct {
		input    string
		expected AggregationStrategy
		wantErr  bool
	}{
		{"concat", AggConcat, false},
		{"best", AggBest, false},
		{"vote", AggVote, false},
		{"merge", AggMerge, false},
		{"summary", AggSummary, false},
		{"invalid", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			strategy, err := ParseAggregationStrategy(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseAggregationStrategy() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && strategy != tt.expected {
				t.Errorf("strategy: got %s, want %s", strategy, tt.expected)
			}
		})
	}
}

func TestModeDescription(t *testing.T) {
	desc := ModeDescription(ModePipeline)
	if desc == "" {
		t.Error("description should not be empty")
	}
	if !strings.Contains(desc, "Pipeline") {
		t.Errorf("description should contain 'Pipeline': %s", desc)
	}
}

func TestAllModes(t *testing.T) {
	modes := AllModes()
	if len(modes) != 3 {
		t.Errorf("expected 3 modes, got %d", len(modes))
	}
}

func TestPriorityString(t *testing.T) {
	tests := []struct {
		priority Priority
		expected string
	}{
		{PriorityLow, "low"},
		{PriorityNormal, "normal"},
		{PriorityHigh, "high"},
		{PriorityCritical, "critical"},
		{Priority(99), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.priority.String(); got != tt.expected {
				t.Errorf("String(): got %s, want %s", got, tt.expected)
			}
		})
	}
}
