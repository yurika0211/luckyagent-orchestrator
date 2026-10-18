package collab

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// MessageBus is the in-process A2A mailbox used by the collaboration API.
// Messages are retained for the lifetime of the runtime; agent transcripts
// remain the durable source for long-form context.
type MessageBus struct {
	mu       sync.RWMutex
	messages []*Message
	max      int
}

func NewMessageBus(max int) *MessageBus {
	if max <= 0 {
		max = 2000
	}
	return &MessageBus{max: max}
}

func (b *MessageBus) Publish(message *Message) error {
	if b == nil {
		return fmt.Errorf("message bus is not initialized")
	}
	if message == nil {
		return fmt.Errorf("message is required")
	}
	copy := *message
	if err := copy.Validate(); err != nil {
		return err
	}
	copy.Payload = cloneAnyMap(message.Payload)
	copy.Metadata = cloneStringMap(message.Metadata)
	b.mu.Lock()
	b.messages = append(b.messages, &copy)
	if len(b.messages) > b.max {
		b.messages = append([]*Message(nil), b.messages[len(b.messages)-b.max:]...)
	}
	b.mu.Unlock()
	return nil
}

type MessageFilter struct {
	From   string
	To     string
	TaskID string
	Limit  int
}

func (b *MessageBus) List(filter MessageFilter) []*Message {
	if b == nil {
		return nil
	}
	limit := filter.Limit
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	from, to, taskID := strings.TrimSpace(filter.From), strings.TrimSpace(filter.To), strings.TrimSpace(filter.TaskID)
	b.mu.RLock()
	result := make([]*Message, 0, len(b.messages))
	for i := len(b.messages) - 1; i >= 0 && len(result) < limit; i-- {
		message := b.messages[i]
		if from != "" && message.From != from || to != "" && message.To != to {
			continue
		}
		if taskID != "" && message.Metadata["task_id"] != taskID {
			if payloadTask, ok := message.Payload["task_id"].(string); !ok || payloadTask != taskID {
				continue
			}
		}
		copy := *message
		copy.Payload = cloneAnyMap(message.Payload)
		copy.Metadata = cloneStringMap(message.Metadata)
		result = append(result, &copy)
	}
	b.mu.RUnlock()
	sort.SliceStable(result, func(i, j int) bool { return result[i].Timestamp.Before(result[j].Timestamp) })
	return result
}

func cloneAnyMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
