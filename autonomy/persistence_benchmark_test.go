package autonomy

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

func benchmarkQueueState(taskCount, operations int) persistedTaskQueue {
	state := persistedTaskQueue{Version: 1, NextID: int64(taskCount)}
	for i := 0; i < taskCount; i++ {
		task := QueueTask{ID: fmt.Sprintf("tq-%d", i+1), State: TaskInProgress, Checkpoint: json.RawMessage(`{"version":1,"messages":[{"role":"user","content":"task"}]}`)}
		for j := 0; j < operations; j++ {
			task.Operations = append(task.Operations, Operation{ID: fmt.Sprintf("step-%d", j), Name: "fixture", Arguments: "{}", State: "completed", Output: strings.Repeat("output-", 146)})
		}
		state.Tasks = append(state.Tasks, task)
	}
	return state
}

func BenchmarkQueueSnapshotEncoding(b *testing.B) {
	for _, operations := range []int{100, 1000} {
		state := benchmarkQueueState(4, operations)
		for _, compact := range []bool{false, true} {
			b.Run(fmt.Sprintf("operations=%d/compact=%t", operations, compact), func(b *testing.B) {
				var encoded []byte
				var err error
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if compact {
						encoded, err = json.Marshal(state)
					} else {
						encoded, err = json.MarshalIndent(state, "", "  ")
					}
					if err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(len(encoded)), "snapshot-bytes")
			})
		}
	}
}

func BenchmarkQueuePersistLock(b *testing.B) {
	for _, operations := range []int{100, 1000} {
		b.Run(fmt.Sprintf("operations=%d", operations), func(b *testing.B) {
			state := benchmarkQueueState(4, operations)
			q := NewTaskQueue(8)
			q.persistPath = filepath.Join(b.TempDir(), "queue.json")
			for i := range state.Tasks {
				task := state.Tasks[i]
				q.tasks[task.ID] = &task
			}
			q.nextID.Store(state.NextID)
			durations := make([]int64, 0, b.N)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				q.mu.Lock()
				start := time.Now()
				err := q.persistLocked()
				durations = append(durations, time.Since(start).Nanoseconds())
				q.mu.Unlock()
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
			info, err := os.Stat(q.persistPath)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(info.Size()), "snapshot-bytes")
			b.ReportMetric(float64(durations[(len(durations)-1)*95/100]), "persist-lock-p95-ns")
		})
	}
}

func TestCompactQueueSnapshotPreservesVersionOneRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.json")
	state := benchmarkQueueState(2, 3)
	legacy, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, legacy, 0600); err != nil {
		t.Fatal(err)
	}
	q := NewTaskQueue(8)
	if _, err := q.EnablePersistence(path); err != nil {
		t.Fatal(err)
	}
	if err := q.Persist(); err != nil {
		t.Fatal(err)
	}
	compact, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(compact) >= len(legacy) {
		t.Fatal("snapshot still expands whitespace")
	}
	reloaded := NewTaskQueue(8)
	if _, err := reloaded.EnablePersistence(path); err != nil {
		t.Fatal(err)
	}
	for _, original := range state.Tasks {
		task, ok := reloaded.Get(original.ID)
		if !ok || len(task.Operations) != 3 || task.Operations[0].Output != original.Operations[0].Output || string(task.Checkpoint) != string(original.Checkpoint) {
			t.Fatal("compact snapshot lost durable evidence")
		}
	}
}
