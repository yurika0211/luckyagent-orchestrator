package delegate

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yurika0211/luckyagent-orchestrator/collab"
)

func awaitDelegate(t *testing.T, dm *DelegateManager, id string) DelegateTask {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		dm.mu.RLock()
		snapshot := cloneDelegateTask(dm.tasks[id])
		dm.mu.RUnlock()
		if snapshot.Status == StatusCompleted || snapshot.Status == StatusFailed || snapshot.Status == StatusCancelled {
			return snapshot
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("delegate did not finish")
	return DelegateTask{}
}

func TestPendingReservationEnforcesQuotaAndIdempotency(t *testing.T) {
	cfg := DefaultDelegateConfig()
	cfg.MaxConcurrent = 1
	dm := NewDelegateManager(cfg)
	entered, release := make(chan struct{}), make(chan struct{})
	dm.SetSessionInitializer(func(*DelegateTask) error { close(entered); <-release; return nil })
	dm.SetAgentExecutor(func(context.Context, string, string) (string, error) { return "done", nil })
	owner := Owner{SessionID: "parent", RequestKey: "request"}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := dm.HandleDelegateWithOwner(map[string]any{"description": "first"}, owner); err != nil {
			t.Error(err)
		}
	}()
	<-entered
	if _, err := dm.HandleDelegate(map[string]any{"description": "second"}); err == nil {
		t.Error("pending reservation did not consume quota")
	}
	reused, err := dm.HandleDelegateWithOwner(map[string]any{"description": "first"}, owner)
	if err != nil {
		t.Fatal(err)
	}
	var response StartResponse
	if err := json.Unmarshal([]byte(reused), &response); err != nil {
		t.Fatal(err)
	}
	close(release)
	wg.Wait()
	if result := awaitDelegate(t, dm, response.TaskID); result.Status != StatusCompleted {
		t.Fatal(result.Error)
	}
	if len(dm.tasks) != 1 {
		t.Fatal("idempotent request created another task")
	}
}

func TestDelegateModesExecuteRealWork(t *testing.T) {
	for _, mode := range []string{"parallel", "pipeline", "debate", "autonomy_queue"} {
		t.Run(mode, func(t *testing.T) {
			cfg := DefaultDelegateConfig()
			cfg.MaxChildren = 2
			dm := NewDelegateManager(cfg)
			var calls, queued atomic.Int64
			sessions := sync.Map{}
			dm.SetAgentExecutor(func(ctx context.Context, description, input string) (string, error) {
				calls.Add(1)
				sessions.Store(DelegateExecutionSessionID(ctx), true)
				if strings.Contains(input, "Cast your vote") {
					return "delegate-1", nil
				}
				return "evidence for " + description, nil
			})
			dm.SetResultVerifier(func(context.Context, *collab.SubTask, string) error { return nil })
			dm.SetQueueExecutor(func(context.Context, string, string, AgentExecutorOptions) (string, error) {
				queued.Add(1)
				return "durable queue result", nil
			})
			raw, err := dm.HandleDelegate(map[string]any{"description": "perform assigned work", "mode": mode, "max_children": 2})
			if err != nil {
				t.Fatal(err)
			}
			var response StartResponse
			if err := json.Unmarshal([]byte(raw), &response); err != nil {
				t.Fatal(err)
			}
			result := awaitDelegate(t, dm, response.TaskID)
			if result.Status != StatusCompleted {
				t.Fatalf("%s: %s", mode, result.Error)
			}
			if mode == "autonomy_queue" {
				if queued.Load() != 1 || calls.Load() != 0 {
					t.Fatal("queue mode used ordinary executor")
				}
			} else {
				if calls.Load() < 2 || queued.Load() != 0 {
					t.Fatalf("mode did not execute multiple collaborators: %d", calls.Load())
				}
				count := 0
				sessions.Range(func(key, value any) bool {
					if key == "" {
						t.Error("missing child session")
					}
					count++
					return true
				})
				if count != 2 {
					t.Fatalf("distinct child sessions = %d", count)
				}
			}
		})
	}
}

func TestQueueModeFailsWithoutQueueAdapter(t *testing.T) {
	dm := NewDelegateManager(DefaultDelegateConfig())
	dm.SetAgentExecutor(func(context.Context, string, string) (string, error) {
		return "", fmt.Errorf("ordinary executor must not be used")
	})
	raw, err := dm.HandleDelegate(map[string]any{"description": "queue task", "mode": "autonomy_queue"})
	if err != nil {
		t.Fatal(err)
	}
	var response StartResponse
	_ = json.Unmarshal([]byte(raw), &response)
	result := awaitDelegate(t, dm, response.TaskID)
	if result.Status != StatusFailed || !strings.Contains(result.Error, "autonomy_queue executor") {
		t.Fatalf("unexpected result: %+v", result)
	}
}
