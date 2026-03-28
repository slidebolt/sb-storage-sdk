package storage

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	messenger "github.com/slidebolt/sb-messenger-sdk"
)

func mustMock(t *testing.T) messenger.Messenger {
	t.Helper()
	msg, err := messenger.Mock()
	if err != nil {
		t.Fatalf("mock messenger: %v", err)
	}
	t.Cleanup(func() { msg.Close() })
	return msg
}

func publish(t *testing.T, msg messenger.Messenger, key string, doc map[string]any) {
	t.Helper()
	data, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := msg.Publish("state.changed."+key, data); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

// collector gathers Watch callbacks in a thread-safe way.
type collector struct {
	mu          sync.Mutex
	added       []string
	removed     []string
	updated     []string
	capsUpdated []string
	stateOnly   []string
}

func (c *collector) onAdd(key string, _ json.RawMessage) {
	c.mu.Lock()
	c.added = append(c.added, key)
	c.mu.Unlock()
}
func (c *collector) onRemove(key string, _ json.RawMessage) {
	c.mu.Lock()
	c.removed = append(c.removed, key)
	c.mu.Unlock()
}
func (c *collector) onUpdate(key string, _ json.RawMessage) {
	c.mu.Lock()
	c.updated = append(c.updated, key)
	c.mu.Unlock()
}
func (c *collector) onCapUpdate(key string, _ json.RawMessage) {
	c.mu.Lock()
	c.capsUpdated = append(c.capsUpdated, key)
	c.mu.Unlock()
}
func (c *collector) onStateUpdate(key string, _ json.RawMessage) {
	c.mu.Lock()
	c.stateOnly = append(c.stateOnly, key)
	c.mu.Unlock()
}
func (c *collector) handlers() WatchHandlers {
	return WatchHandlers{
		OnAdd:              c.onAdd,
		OnRemove:           c.onRemove,
		OnUpdate:           c.onUpdate,
		OnCapabilityUpdate: c.onCapUpdate,
		OnStateUpdate:      c.onStateUpdate,
	}
}

func (c *collector) waitFor(t *testing.T, field *[]string, count int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		n := len(*field)
		c.mu.Unlock()
		if n >= count {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	t.Fatalf("timed out waiting for %d items, got %d", count, len(*field))
}

func TestWatch_OnAdd(t *testing.T) {
	msg := mustMock(t)
	c := &collector{}

	query := Query{Where: []Filter{{Field: "labels.PluginHomeassistant", Op: Exists}}}
	w, err := Watch(msg, query, c.handlers())
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	defer w.Stop()

	publish(t, msg, "plugin-kasa.living-room.lamp", map[string]any{
		"labels": map[string]any{"PluginHomeassistant": []any{}},
		"state":  map[string]any{"power": true},
	})

	c.waitFor(t, &c.added, 1)

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.added) != 1 || c.added[0] != "plugin-kasa.living-room.lamp" {
		t.Errorf("added = %v, want [plugin-kasa.living-room.lamp]", c.added)
	}
}

func TestWatch_CapabilityUpdate(t *testing.T) {
	msg := mustMock(t)
	c := &collector{}

	query := Query{Where: []Filter{{Field: "labels.PluginAlexa", Op: Exists}}}
	w, err := Watch(msg, query, c.handlers())
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	defer w.Stop()

	// 1. Initial add
	publish(t, msg, "dev1", map[string]any{
		"labels":   map[string]any{"PluginAlexa": []any{}},
		"commands": []any{"turn_on"},
		"state":    map[string]any{"power": false},
	})
	c.waitFor(t, &c.added, 1)

	// 2. State-only update
	publish(t, msg, "dev1", map[string]any{
		"labels":   map[string]any{"PluginAlexa": []any{}},
		"commands": []any{"turn_on"},
		"state":    map[string]any{"power": true},
	})
	c.waitFor(t, &c.stateOnly, 1)

	// 3. Capability update (new command)
	publish(t, msg, "dev1", map[string]any{
		"labels":   map[string]any{"PluginAlexa": []any{}},
		"commands": []any{"turn_on", "set_brightness"},
		"state":    map[string]any{"power": true},
	})
	c.waitFor(t, &c.capsUpdated, 1)

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.added) != 1 {
		t.Errorf("added = %d, want 1", len(c.added))
	}
	if len(c.stateOnly) != 1 {
		t.Errorf("stateOnly = %d, want 1", len(c.stateOnly))
	}
	if len(c.capsUpdated) != 1 {
		t.Errorf("capsUpdated = %d, want 1", len(c.capsUpdated))
	}
}
