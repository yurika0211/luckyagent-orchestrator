package task

import "strings"

type MainAgentObservation struct {
	TaskID            string       `json:"task_id"`
	Status            Status       `json:"status"`
	Mode              Mode         `json:"mode"`
	Progress          float64      `json:"progress"`
	RunningChildren   int          `json:"running_children"`
	CompletedChildren int          `json:"completed_children"`
	FailedChildren    int          `json:"failed_children"`
	Blockers          []string     `json:"blockers,omitempty"`
	FreshEvidence     []string     `json:"fresh_evidence,omitempty"`
	FilesChanged      []string     `json:"files_changed,omitempty"`
	TestsRun          []string     `json:"tests_run,omitempty"`
	VerifierStatus    string       `json:"verifier_status,omitempty"`
	Cost              CostSnapshot `json:"cost,omitempty"`
	RecommendedNext   string       `json:"recommended_next"`
}

func ReduceObservation(record Record, events []Event) MainAgentObservation {
	obs := MainAgentObservation{
		TaskID:          record.ID,
		Status:          record.Status,
		Mode:            record.Mode,
		Progress:        progressForStatus(record.Status),
		Cost:            record.Outcome.Cost,
		RecommendedNext: "finalize",
	}
	if record.Status == StatusPending || record.Status == StatusRunning {
		obs.RecommendedNext = "wait"
	}
	if record.Status == StatusFailed || record.Status == StatusBlocked {
		obs.RecommendedNext = "ask_user"
	}
	if record.Status == StatusCancelled {
		obs.RecommendedNext = "finalize"
	}

	seenEvidence := make(map[string]struct{})
	seenFiles := make(map[string]struct{})
	seenTests := make(map[string]struct{})
	// Child events can describe several attempts (and, for a debate, several
	// rounds) for the same child. Reduce its latest state instead of counting
	// every transition as a different child.
	childStates := make(map[string]Status)
	childErrors := make(map[string]string)
	for _, event := range events {
		if event.Progress > obs.Progress {
			obs.Progress = event.Progress
		}
		if event.Cost.TokenEstimate > 0 {
			obs.Cost.TokenEstimate += event.Cost.TokenEstimate
		}
		if event.Cost.ToolCalls > 0 {
			obs.Cost.ToolCalls += event.Cost.ToolCalls
		}
		if event.Cost.ChildCount > 0 {
			obs.Cost.ChildCount += event.Cost.ChildCount
		}
		if childID := strings.TrimSpace(event.ChildID); childID != "" {
			state := event.Status
			switch event.Type {
			case EventChildCreated:
				if state == "" {
					state = StatusPending
				}
			case EventStarted:
				state = StatusRunning
			case EventCompleted:
				state = StatusCompleted
			case EventFailed:
				state = StatusFailed
			case EventCancelled:
				state = StatusCancelled
			}
			if state != "" {
				childStates[childID] = state
				delete(childErrors, childID)
				if state == StatusFailed || state == StatusBlocked {
					childErrors[childID] = strings.TrimSpace(event.Error)
					if childErrors[childID] == "" {
						childErrors[childID] = strings.TrimSpace(event.Metadata["error"])
					}
				}
			}
		} else if event.Type == EventFailed && strings.TrimSpace(event.Error) != "" {
			obs.Blockers = append(obs.Blockers, event.Error)
		}
		appendUnique(&obs.FreshEvidence, seenEvidence, event.Evidence)
		appendUnique(&obs.FilesChanged, seenFiles, event.Files)
		appendUnique(&obs.TestsRun, seenTests, event.Tests)
		if v := strings.TrimSpace(event.Metadata["verifier_status"]); v != "" {
			obs.VerifierStatus = v
		}
	}
	terminalChildren := 0
	for _, state := range childStates {
		switch state {
		case StatusPending, StatusRunning:
			obs.RunningChildren++
		case StatusCompleted:
			obs.CompletedChildren++
			terminalChildren++
		case StatusFailed, StatusBlocked:
			obs.FailedChildren++
			terminalChildren++
		case StatusCancelled:
			terminalChildren++
		}
	}
	// Keep blocker order deterministic and consistent with the timeline.
	for _, event := range events {
		if blocker := childErrors[event.ChildID]; blocker != "" {
			obs.Blockers = append(obs.Blockers, blocker)
			delete(childErrors, event.ChildID)
		}
	}
	if len(childStates) > 0 && (record.Status == StatusPending || record.Status == StatusRunning) {
		obs.Progress = float64(terminalChildren) / float64(len(childStates))
	}

	if obs.RunningChildren > 0 && obs.Status != StatusCompleted && obs.Status != StatusCancelled {
		obs.RecommendedNext = "wait"
	}
	if obs.FailedChildren > 0 && obs.CompletedChildren > 0 {
		obs.RecommendedNext = "aggregate"
	}
	if len(obs.FilesChanged) > 0 && len(obs.TestsRun) == 0 && obs.Status == StatusCompleted {
		obs.RecommendedNext = "verify"
	}
	if obs.Status == StatusFailed || obs.Status == StatusBlocked {
		obs.RecommendedNext = "ask_user"
	}
	return obs
}

func progressForStatus(status Status) float64 {
	switch status {
	case StatusCompleted, StatusCancelled:
		return 1
	case StatusRunning:
		return 0.5
	default:
		return 0
	}
}

func appendUnique(out *[]string, seen map[string]struct{}, values []string) {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		*out = append(*out, value)
	}
}
