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
	mu      sync.Mutex
	added   []string
	removed []string
	updated []string
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
func (c *collector) handlers() WatchHandlers {
	return WatchHandlers{OnAdd: c.onAdd, OnRemove: c.onRemove, OnUpdate: c.onUpdate}
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
	if len(c.updated) != 0 {
		t.Errorf("updated = %v, want empty", c.updated)
	}
}

func TestWatch_OnUpdate(t *testing.T) {
	msg := mustMock(t)
	c := &collector{}

	query := Query{Where: []Filter{{Field: "labels.PluginHomeassistant", Op: Exists}}}
	w, err := Watch(msg, query, c.handlers())
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	defer w.Stop()

	entity := map[string]any{
		"labels": map[string]any{"PluginHomeassistant": []any{}},
		"state":  map[string]any{"power": true},
	}

	publish(t, msg, "plugin-kasa.lamp", entity)
	c.waitFor(t, &c.added, 1)

	// Publish again — same key, still matches → OnUpdate
	entity["state"] = map[string]any{"power": false}
	publish(t, msg, "plugin-kasa.lamp", entity)
	c.waitFor(t, &c.updated, 1)

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.added) != 1 {
		t.Errorf("added count = %d, want 1", len(c.added))
	}
	if len(c.updated) != 1 || c.updated[0] != "plugin-kasa.lamp" {
		t.Errorf("updated = %v, want [plugin-kasa.lamp]", c.updated)
	}
}

func TestWatch_OnRemove_LabelRemoved(t *testing.T) {
	msg := mustMock(t)
	c := &collector{}

	query := Query{Where: []Filter{{Field: "labels.PluginHomeassistant", Op: Exists}}}
	w, err := Watch(msg, query, c.handlers())
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	defer w.Stop()

	// Add entity with label
	publish(t, msg, "plugin-kasa.lamp", map[string]any{
		"labels": map[string]any{"PluginHomeassistant": []any{}},
	})
	c.waitFor(t, &c.added, 1)

	// Remove label — no longer matches → OnRemove
	publish(t, msg, "plugin-kasa.lamp", map[string]any{
		"labels": map[string]any{},
	})
	c.waitFor(t, &c.removed, 1)

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.removed) != 1 || c.removed[0] != "plugin-kasa.lamp" {
		t.Errorf("removed = %v, want [plugin-kasa.lamp]", c.removed)
	}
}

func TestWatch_NoMatch(t *testing.T) {
	msg := mustMock(t)
	c := &collector{}

	query := Query{Where: []Filter{{Field: "labels.PluginHomeassistant", Op: Exists}}}
	w, err := Watch(msg, query, c.handlers())
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	defer w.Stop()

	// Entity without the label — no callback
	publish(t, msg, "plugin-kasa.lamp", map[string]any{
		"labels": map[string]any{"SomethingElse": true},
	})

	// Give time for non-delivery
	time.Sleep(200 * time.Millisecond)

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.added) != 0 {
		t.Errorf("added = %v, want empty (should not match)", c.added)
	}
}

func TestWatch_PatternAndFilters(t *testing.T) {
	msg := mustMock(t)
	c := &collector{}

	query := Query{
		Pattern: "plugin-kasa.>",
		Where:   []Filter{{Field: "type", Op: Eq, Value: "light"}},
	}
	w, err := Watch(msg, query, c.handlers())
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	defer w.Stop()

	publish(t, msg, "plugin-kasa.lr.lamp", map[string]any{
		"type": "light",
	})
	publish(t, msg, "plugin-esphome.lr.lamp", map[string]any{
		"type": "light",
	})
	publish(t, msg, "plugin-kasa.lr.switch", map[string]any{
		"type": "switch",
	})

	c.waitFor(t, &c.added, 1)

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.added) != 1 || c.added[0] != "plugin-kasa.lr.lamp" {
		t.Fatalf("added = %v, want [plugin-kasa.lr.lamp]", c.added)
	}
}

func TestWatch_MultipleOperators(t *testing.T) {
	msg := mustMock(t)

	tests := []struct {
		name    string
		filters []Filter
		doc     map[string]any
		match   bool
	}{
		{
			name:    "Eq match",
			filters: []Filter{{Field: "type", Op: Eq, Value: "light"}},
			doc:     map[string]any{"type": "light"},
			match:   true,
		},
		{
			name:    "Eq no match",
			filters: []Filter{{Field: "type", Op: Eq, Value: "switch"}},
			doc:     map[string]any{"type": "light"},
			match:   false,
		},
		{
			name:    "Neq match",
			filters: []Filter{{Field: "type", Op: Neq, Value: "switch"}},
			doc:     map[string]any{"type": "light"},
			match:   true,
		},
		{
			name:    "Gt match",
			filters: []Filter{{Field: "state.brightness", Op: Gt, Value: float64(50)}},
			doc:     map[string]any{"state": map[string]any{"brightness": float64(80)}},
			match:   true,
		},
		{
			name:    "Gt no match",
			filters: []Filter{{Field: "state.brightness", Op: Gt, Value: float64(90)}},
			doc:     map[string]any{"state": map[string]any{"brightness": float64(80)}},
			match:   false,
		},
		{
			name:    "Lte match",
			filters: []Filter{{Field: "state.temp", Op: Lte, Value: float64(25)}},
			doc:     map[string]any{"state": map[string]any{"temp": float64(25)}},
			match:   true,
		},
		{
			name:    "Contains match",
			filters: []Filter{{Field: "name", Op: Contains, Value: "living"}},
			doc:     map[string]any{"name": "living room lamp"},
			match:   true,
		},
		{
			name:    "Prefix match",
			filters: []Filter{{Field: "plugin", Op: Prefix, Value: "plugin-"}},
			doc:     map[string]any{"plugin": "plugin-kasa"},
			match:   true,
		},
		{
			name:    "Exists match",
			filters: []Filter{{Field: "labels.Tagged", Op: Exists}},
			doc:     map[string]any{"labels": map[string]any{"Tagged": true}},
			match:   true,
		},
		{
			name:    "Exists no match",
			filters: []Filter{{Field: "labels.Tagged", Op: Exists}},
			doc:     map[string]any{"labels": map[string]any{}},
			match:   false,
		},
		{
			name:    "In match",
			filters: []Filter{{Field: "type", Op: In, Value: []any{"light", "switch"}}},
			doc:     map[string]any{"type": "switch"},
			match:   true,
		},
		{
			name:    "In no match",
			filters: []Filter{{Field: "type", Op: In, Value: []any{"light", "switch"}}},
			doc:     map[string]any{"type": "sensor"},
			match:   false,
		},
		{
			name: "AND multiple filters",
			filters: []Filter{
				{Field: "type", Op: Eq, Value: "light"},
				{Field: "labels.PluginHomeassistant", Op: Exists},
			},
			doc: map[string]any{
				"type":   "light",
				"labels": map[string]any{"PluginHomeassistant": []any{}},
			},
			match: true,
		},
		{
			name: "AND fails on second filter",
			filters: []Filter{
				{Field: "type", Op: Eq, Value: "light"},
				{Field: "labels.PluginHomeassistant", Op: Exists},
			},
			doc:   map[string]any{"type": "light", "labels": map[string]any{}},
			match: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &collector{}
			w, err := Watch(msg, Query{Where: tt.filters}, c.handlers())
			if err != nil {
				t.Fatalf("watch: %v", err)
			}
			defer w.Stop()

			data, _ := json.Marshal(tt.doc)
			msg.Publish("state.changed.test.key", data)

			if tt.match {
				c.waitFor(t, &c.added, 1)
			} else {
				time.Sleep(200 * time.Millisecond)
				c.mu.Lock()
				defer c.mu.Unlock()
				if len(c.added) != 0 {
					t.Errorf("expected no match, got added=%v", c.added)
				}
			}
		})
	}
}

func TestWatch_Stop(t *testing.T) {
	msg := mustMock(t)
	c := &collector{}

	query := Query{Where: []Filter{{Field: "labels.X", Op: Exists}}}
	w, err := Watch(msg, query, c.handlers())
	if err != nil {
		t.Fatalf("watch: %v", err)
	}

	// Verify it works first
	publish(t, msg, "a.b.c", map[string]any{"labels": map[string]any{"X": true}})
	c.waitFor(t, &c.added, 1)

	// Stop and publish again — no callback
	w.Stop()
	time.Sleep(50 * time.Millisecond)
	publish(t, msg, "d.e.f", map[string]any{"labels": map[string]any{"X": true}})
	time.Sleep(200 * time.Millisecond)

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.added) != 1 {
		t.Errorf("added = %d after stop, want 1 (no new adds)", len(c.added))
	}
}
