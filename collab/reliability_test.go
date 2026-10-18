package collab

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	taskstore "github.com/yurika0211/luckyagent-orchestrator/task"
)

func TestPartialFailureNeverBecomesVerifiedSuccess(t *testing.T) {
	r := NewRegistry()
	_ = r.Register(&AgentProfile{ID: "first"})
	_ = r.Register(&AgentProfile{ID: "second"})
	store, err := taskstore.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	handler := acceptingTestHandler{TaskHandlerFunc(func(_ context.Context, sub *SubTask) (string, error) {
		if sub.AgentID == "first" {
			return "", fmt.Errorf("required deliverable missing")
		}
		return "useful accepted evidence", nil
	})}
	dm := NewDelegateManager(r, handler)
	if err := dm.SetTaskStore(store); err != nil {
		t.Fatal(err)
	}
	job, err := dm.Delegate(context.Background(), ModeParallel, "inspect independent modules", "original input", []string{"first", "second"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	result, err := dm.Wait(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if result.State != TaskFailed || result.Metadata["partial_failure"] != "true" || !strings.Contains(result.Result, "useful accepted evidence") {
		t.Fatalf("partial result lost: %+v", result)
	}
	record, _, _ := store.Get(job.ID)
	if record.Status == taskstore.StatusCompleted || record.Outcome.Verified {
		t.Fatalf("false success: %+v", record)
	}
	failed, _, _ := store.Get(result.SubTasks[0].ID)
	if failed.Status != taskstore.StatusFailed || failed.Outcome.Verified {
		t.Fatalf("false child success: %+v", failed)
	}
}

func TestRequiredVerificationFailsClosed(t *testing.T) {
	r := NewRegistry()
	_ = r.Register(&AgentProfile{ID: "one"})
	dm := NewDelegateManager(r, TaskHandlerFunc(func(context.Context, *SubTask) (string, error) { return "nonempty but unreviewed", nil }))
	job, err := dm.Delegate(context.Background(), ModeDebate, "decision", "input", []string{"one"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	result, err := dm.Wait(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if result.State != TaskFailed || result.SubTasks[0].Verified || !strings.Contains(result.SubTasks[0].Error, "no verifier") {
		t.Fatalf("accepted missing verifier: %+v", result)
	}
}

func TestRestoreCollaborationHistoryWithoutReplayingOtherWork(t *testing.T) {
	store, err := taskstore.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range []taskstore.Record{
		{ID: "collab-interrupted", Role: "orchestrator", Source: taskstore.SourceHTTP, Runtime: taskstore.RuntimeRef{Type: "collab"}, Status: taskstore.StatusRunning, ExecutionSessionID: "collab-parent-session", Mode: taskstore.ModeParallel},
		{ID: "collab-interrupted-sub-1", ParentID: "collab-interrupted", Runtime: taskstore.RuntimeRef{Type: "collab-subtask", AgentID: "one"}, Status: taskstore.StatusRunning, ExecutionSessionID: "child-session"},
		{ID: "ordinary", Runtime: taskstore.RuntimeRef{Type: "delegate"}, Status: taskstore.StatusRunning},
		{ID: "foreground", Source: taskstore.SourceAutonomy, Status: taskstore.StatusRunning},
	} {
		if _, err := store.Create(record); err != nil {
			t.Fatal(err)
		}
	}
	dm := NewDelegateManager(NewRegistry(), nil)
	if err := dm.SetTaskStore(store); err != nil {
		t.Fatal(err)
	}
	recovered, ok := dm.GetTask("collab-interrupted")
	if !ok || len(dm.ListTasks()) != 1 || recovered.State != TaskFailed || recovered.Metadata["interrupted"] != "true" || recovered.SubTasks[0].ExecutionSessionID != "child-session" {
		t.Fatalf("lost recovered task/session: %+v", recovered)
	}
	for _, id := range []string{"ordinary", "foreground"} {
		record, _, _ := store.Get(id)
		if record.Status != taskstore.StatusRunning {
			t.Fatalf("modified unrelated %s", id)
		}
	}
	restarted := NewDelegateManager(NewRegistry(), nil)
	if err := restarted.SetTaskStore(store); err != nil {
		t.Fatal(err)
	}
	if len(restarted.ListTasks()) != 1 {
		t.Fatal("history lost on next restart")
	}
}

func TestAggregationNeedsEvidenceForSelectionAndConsensus(t *testing.T) {
	a := NewAggregator()
	short, long := "answer: yes", strings.Repeat("verbose irrelevant text ", 100)+"answer: no"
	result := a.Aggregate(AggBest, []string{short, long})
	if result.BestIndex != -1 || !strings.Contains(result.Output, short) || !strings.Contains(result.Output, long) {
		t.Fatal("unscored result was ranked")
	}
	a.SetScorer(AggBest, func(s string) float64 {
		if s == short {
			return 1
		}
		return 0
	})
	if result := a.Aggregate(AggBest, []string{long, short}); result.Output != short {
		t.Fatal("quality evaluator was ignored")
	}
	prefix := strings.Repeat("共同背景", 80)
	result = a.Aggregate(AggVote, []string{prefix + "同意", prefix + "反对"})
	if len(result.Votes) != 2 || result.Metadata["consensus"] != "false" || !strings.Contains(result.Output, "反对") {
		t.Fatalf("opposing conclusions became consensus: %+v", result)
	}
	result = a.Aggregate(AggSummary, []string{strings.Repeat("汉", 300)})
	if !utf8.ValidString(result.Output) {
		t.Fatal("summary split a Unicode character")
	}
}

func TestPlannerRejectsDuplicateResponsibilities(t *testing.T) {
	r := NewRegistry()
	_ = r.Register(&AgentProfile{ID: "one"})
	_ = r.Register(&AgentProfile{ID: "two"})
	dm := NewDelegateManager(r, nil)
	dm.SetTaskPlanner(func(context.Context, CollabMode, string, string, []*AgentProfile) ([]Assignment, error) {
		return []Assignment{{AgentID: "one", Description: "same task", AcceptanceCriteria: []string{"check"}}, {AgentID: "two", Description: "same task", AcceptanceCriteria: []string{"check"}}}, nil
	})
	if _, err := dm.Delegate(context.Background(), ModeParallel, "work", "", []string{"one", "two"}, time.Second); err == nil {
		t.Fatal("duplicated work was accepted as a plan")
	}
}
