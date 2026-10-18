package collab

import (
	"fmt"
	"strings"
)

func defaultAssignments(mode CollabMode, description string, agents []*AgentProfile) []Assignment {
	assignments := make([]Assignment, len(agents))
	for i, agent := range agents {
		responsibility := fmt.Sprintf("Independent perspective %d: %s. Capabilities: %s.", i+1, agent.Description, strings.Join(agent.Capabilities, ", "))
		if mode == ModePipeline {
			switch {
			case i == 0 && len(agents) > 1:
				responsibility = "Establish evidence, constraints and an actionable plan for the next stage."
			case i == len(agents)-1 && len(agents) > 1:
				responsibility = "Review the previous stage against the original request; correct omissions and produce the final deliverable."
			default:
				responsibility = "Execute the assigned plan from the previous stage and report the resulting evidence."
			}
		}
		assignments[i] = Assignment{AgentID: agent.ID, Description: fmt.Sprintf("%s\n\nYour responsibility: %s", description, responsibility),
			AcceptanceCriteria: []string{"Address the assigned responsibility", "Support conclusions with evidence and report unfinished work"}}
	}
	return assignments
}

func assignSubTasks(subs []*SubTask, assignments []Assignment) error {
	if len(assignments) != len(subs) {
		return fmt.Errorf("planner must assign exactly one responsibility per agent")
	}
	seen := make(map[string]bool)
	descriptions := make(map[string]bool)
	for i, assignment := range assignments {
		if assignment.AgentID != subs[i].AgentID || seen[assignment.AgentID] {
			return fmt.Errorf("planner assignments must preserve unique agent order")
		}
		if strings.TrimSpace(assignment.Description) == "" || len(assignment.AcceptanceCriteria) == 0 {
			return fmt.Errorf("assignment for %s needs a description and acceptance criteria", assignment.AgentID)
		}
		seen[assignment.AgentID] = true
		description := strings.Join(strings.Fields(strings.ToLower(assignment.Description)), " ")
		if descriptions[description] {
			return fmt.Errorf("planner duplicated responsibilities")
		}
		descriptions[description] = true
		for _, criterion := range assignment.AcceptanceCriteria {
			if strings.TrimSpace(criterion) == "" {
				return fmt.Errorf("empty acceptance criterion for %s", assignment.AgentID)
			}
		}
		subs[i].Description, subs[i].AcceptanceCriteria = assignment.Description, append([]string(nil), assignment.AcceptanceCriteria...)
	}
	return nil
}
