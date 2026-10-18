package collab

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// BlackboardEntry is a small, task-scoped shared note. It is intentionally
// plain data so the API and Android client can render it without knowing which
// agent produced it.
type BlackboardEntry struct {
	TaskID    string    `json:"task_id"`
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	Author    string    `json:"author,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Blackboard stores the latest value for each task/key pair. The delegate
// manager owns one board, so parallel, pipeline, and debate workers can share
// progress without coupling their transcripts.
type Blackboard struct {
	mu      sync.RWMutex
	entries map[string]BlackboardEntry
}

func NewBlackboard() *Blackboard {
	return &Blackboard{entries: make(map[string]BlackboardEntry)}
}

func boardKey(taskID, key string) string {
	return strings.TrimSpace(taskID) + "\x00" + strings.TrimSpace(key)
}

func (b *Blackboard) Set(taskID, key, value, author string) (BlackboardEntry, error) {
	if b == nil {
		return BlackboardEntry{}, fmt.Errorf("blackboard is not initialized")
	}
	taskID, key = strings.TrimSpace(taskID), strings.TrimSpace(key)
	if taskID == "" || key == "" {
		return BlackboardEntry{}, fmt.Errorf("task_id and key are required")
	}
	entry := BlackboardEntry{TaskID: taskID, Key: key, Value: value, Author: strings.TrimSpace(author), UpdatedAt: time.Now()}
	b.mu.Lock()
	b.entries[boardKey(taskID, key)] = entry
	b.mu.Unlock()
	return entry, nil
}

func (b *Blackboard) Get(taskID, key string) (BlackboardEntry, bool) {
	if b == nil {
		return BlackboardEntry{}, false
	}
	b.mu.RLock()
	entry, ok := b.entries[boardKey(taskID, key)]
	b.mu.RUnlock()
	return entry, ok
}

func (b *Blackboard) List(taskID string) []BlackboardEntry {
	if b == nil {
		return nil
	}
	taskID = strings.TrimSpace(taskID)
	b.mu.RLock()
	entries := make([]BlackboardEntry, 0, len(b.entries))
	for _, entry := range b.entries {
		if taskID == "" || entry.TaskID == taskID {
			entries = append(entries, entry)
		}
	}
	b.mu.RUnlock()
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].TaskID != entries[j].TaskID {
			return entries[i].TaskID < entries[j].TaskID
		}
		return entries[i].Key < entries[j].Key
	})
	return entries
}

func (b *Blackboard) Delete(taskID, key string) bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	k := boardKey(taskID, key)
	if _, ok := b.entries[k]; !ok {
		return false
	}
	delete(b.entries, k)
	return true
}
