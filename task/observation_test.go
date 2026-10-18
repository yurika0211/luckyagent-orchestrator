package task

import "testing"

func TestReduceObservationRecommendsVerifyForCompletedMutationWithoutTests(t *testing.T) {
	record := Record{
		ID:     "task-verify",
		Mode:   ModeParallel,
		Status: StatusCompleted,
	}
	obs := ReduceObservation(record, []Event{{
		Type:   EventProgress,
		TaskID: record.ID,
		Files:  []string{"internal/tool/delegate.go"},
	}})

	if obs.RecommendedNext != "verify" {
		t.Fatalf("expected verify recommendation, got %+v", obs)
	}
	if len(obs.FilesChanged) != 1 {
		t.Fatalf("expected file evidence, got %+v", obs)
	}
}

func TestReduceObservationRecommendsWaitForRunningChildren(t *testing.T) {
	record := Record{
		ID:     "task-wait",
		Mode:   ModeParallel,
		Status: StatusRunning,
	}
	obs := ReduceObservation(record, []Event{{
		Type:    EventChildCreated,
		TaskID:  record.ID,
		ChildID: "task-wait-sub-1",
	}})

	if obs.RunningChildren != 1 || obs.RecommendedNext != "wait" {
		t.Fatalf("expected wait with running child, got %+v", obs)
	}
}

func TestReduceObservationRecommendsAggregateForPartialFailure(t *testing.T) {
	record := Record{
		ID:     "task-aggregate",
		Mode:   ModeParallel,
		Status: StatusRunning,
	}
	obs := ReduceObservation(record, []Event{
		{Type: EventChildCreated, TaskID: record.ID, ChildID: "child-1"},
		{Type: EventChildCreated, TaskID: record.ID, ChildID: "child-2"},
		{Type: EventCompleted, TaskID: record.ID, ChildID: "child-1", Evidence: []string{"implemented module A"}},
		{Type: EventFailed, TaskID: record.ID, ChildID: "child-2", Error: "module B blocked"},
	})

	if obs.CompletedChildren != 1 || obs.FailedChildren != 1 || obs.RecommendedNext != "aggregate" {
		t.Fatalf("expected aggregate on partial failure, got %+v", obs)
	}
	if len(obs.Blockers) != 1 || obs.Blockers[0] != "module B blocked" {
		t.Fatalf("expected blocker, got %+v", obs.Blockers)
	}
}

func TestReduceObservationUsesLatestChildProgressState(t *testing.T) {
	record := Record{ID: "parent", Status: StatusRunning, Mode: ModeDebate}
	events := []Event{
		{Type: EventChildCreated, ChildID: "child-1"},
		{Type: EventChildCreated, ChildID: "child-1"},
		{Type: EventChildCreated, ChildID: "child-2"},
		{Type: EventProgress, ChildID: "child-1", Status: StatusRunning},
		{Type: EventProgress, ChildID: "child-1", Status: StatusFailed, Metadata: map[string]string{"error": "first attempt failed"}},
		{Type: EventProgress, ChildID: "child-1", Status: StatusRunning},
		{Type: EventProgress, ChildID: "child-1", Status: StatusCompleted},
		{Type: EventProgress, ChildID: "child-1", Status: StatusCompleted},
	}
	obs := ReduceObservation(record, events)
	if obs.RunningChildren != 1 || obs.CompletedChildren != 1 || obs.FailedChildren != 0 {
		t.Fatalf("expected one completed child and one pending child, got %+v", obs)
	}
	if len(obs.Blockers) != 0 || obs.Progress != 0.5 {
		t.Fatalf("retry should clear stale failure and progress should count unique children, got %+v", obs)
	}
	events = append(events, Event{Type: EventProgress, ChildID: "child-2", Status: StatusCancelled})
	obs = ReduceObservation(record, events)
	if obs.RunningChildren != 0 || obs.CompletedChildren != 1 || obs.Progress != 1 {
		t.Fatalf("cancelled child must stop contributing to active count, got %+v", obs)
	}
}

func TestReduceObservationReportsLatestChildProgressFailure(t *testing.T) {
	obs := ReduceObservation(Record{ID: "parent", Status: StatusRunning}, []Event{
		{Type: EventChildCreated, ChildID: "child"},
		{Type: EventProgress, ChildID: "child", Status: StatusFailed, Metadata: map[string]string{"error": "step could not finish"}},
	})
	if obs.RunningChildren != 0 || obs.FailedChildren != 1 || len(obs.Blockers) != 1 || obs.Blockers[0] != "step could not finish" {
		t.Fatalf("progress failure must be visible in the parent observation, got %+v", obs)
	}
}
