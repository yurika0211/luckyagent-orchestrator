package collab

import "testing"

func TestBlackboardSetListAndDelete(t *testing.T) {
	board := NewBlackboard()
	entry, err := board.Set("task-1", "plan", "draft", "planner")
	if err != nil {
		t.Fatal(err)
	}
	if entry.TaskID != "task-1" || entry.Key != "plan" || entry.Author != "planner" {
		t.Fatalf("unexpected entry: %+v", entry)
	}
	if got, ok := board.Get("task-1", "plan"); !ok || got.Value != "draft" {
		t.Fatalf("Get() = %+v, %v", got, ok)
	}
	if got := board.List("task-1"); len(got) != 1 {
		t.Fatalf("List() len = %d, want 1", len(got))
	}
	if !board.Delete("task-1", "plan") || board.Delete("task-1", "plan") {
		t.Fatal("Delete should report one successful deletion")
	}
}

func TestMessageBusPublishAndFilter(t *testing.T) {
	bus := NewMessageBus(4)
	message := NewMessage(MsgTaskAssign, "planner", "coder", map[string]any{"task": "review", "task_id": "task-1"})
	if err := bus.Publish(message); err != nil {
		t.Fatal(err)
	}
	items := bus.List(MessageFilter{To: "coder", TaskID: "task-1"})
	if len(items) != 1 || items[0].From != "planner" || items[0].Payload["task"] != "review" {
		t.Fatalf("unexpected messages: %+v", items)
	}
}
