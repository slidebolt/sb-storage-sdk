package storage

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	messenger "github.com/slidebolt/sb-messenger-sdk"
)

// WatchHandlers defines callbacks for query membership changes.
type WatchHandlers struct {
	OnAdd              func(key string, data json.RawMessage)
	OnRemove           func(key string, data json.RawMessage)
	OnUpdate           func(key string, data json.RawMessage)
	OnCapabilityUpdate func(key string, data json.RawMessage)
	OnStateUpdate      func(key string, data json.RawMessage)
	Fingerprint        func(json.RawMessage) string
}

// Watcher monitors state.changed events and fires callbacks when entities
// enter, leave, or update within a query's result set.
type Watcher struct {
	mu       sync.Mutex
	tracked  map[string]json.RawMessage
	caps     map[string]string // key → capability fingerprint
	handlers WatchHandlers
	pattern  string
	filters  []Filter
	sub      messenger.Subscription
}

// Watch subscribes to state.changed.> and evaluates each event against the
// query filters. Callbacks fire when an entity enters, leaves, or updates
// within the result set. Call Stop() to unsubscribe.
func Watch(msg messenger.Messenger, query Query, h WatchHandlers) (*Watcher, error) {
	if h.Fingerprint == nil {
		h.Fingerprint = capFingerprint
	}

	w := &Watcher{
		tracked:  make(map[string]json.RawMessage),
		caps:     make(map[string]string),
		handlers: h,
		pattern:  query.Pattern,
		filters:  query.Where,
	}

	sub, err := msg.Subscribe("state.changed.>", w.handle)
	if err != nil {
		return nil, fmt.Errorf("storage watch: subscribe: %w", err)
	}
	w.sub = sub

	if err := msg.Flush(); err != nil {
		sub.Unsubscribe()
		return nil, fmt.Errorf("storage watch: flush: %w", err)
	}

	return w, nil
}

// Stop unsubscribes and stops the watcher.
func (w *Watcher) Stop() {
	if w.sub != nil {
		w.sub.Unsubscribe()
	}
}

// Tracked returns a snapshot of all currently tracked entities.
func (w *Watcher) Tracked() map[string]json.RawMessage {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make(map[string]json.RawMessage, len(w.tracked))
	for k, v := range w.tracked {
		out[k] = v
	}
	return out
}

// Populate seeds the watcher with initial entity data. This is useful for
// entities already in storage before the watch started, so that subsequent
// updates can correctly differentiate between state and capability changes.
func (w *Watcher) Populate(key string, data json.RawMessage) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.tracked[key] = data
	w.caps[key] = w.handlers.Fingerprint(data)
}

func (w *Watcher) handle(m *messenger.Message) {
	// Extract key from subject: state.changed.{key}
	key := strings.TrimPrefix(m.Subject, "state.changed.")

	var doc map[string]any
	if err := json.Unmarshal(m.Data, &doc); err != nil {
		return
	}

	matches := matchesQuery(key, doc, w.pattern, w.filters)

	w.mu.Lock()
	_, wasTracked := w.tracked[key]
	prevCap := w.caps[key]
	newCap := w.handlers.Fingerprint(m.Data)

	switch {
	case matches && !wasTracked:
		w.tracked[key] = m.Data
		w.caps[key] = newCap
		w.mu.Unlock()
		if w.handlers.OnAdd != nil {
			w.handlers.OnAdd(key, m.Data)
		}
	case matches && wasTracked:
		w.tracked[key] = m.Data
		w.caps[key] = newCap
		w.mu.Unlock()
		if w.handlers.OnUpdate != nil {
			w.handlers.OnUpdate(key, m.Data)
		}
		if prevCap != newCap {
			if w.handlers.OnCapabilityUpdate != nil {
				w.handlers.OnCapabilityUpdate(key, m.Data)
			}
		} else {
			if w.handlers.OnStateUpdate != nil {
				w.handlers.OnStateUpdate(key, m.Data)
			}
		}
	case !matches && wasTracked:
		prev := w.tracked[key]
		delete(w.tracked, key)
		delete(w.caps, key)
		w.mu.Unlock()
		if w.handlers.OnRemove != nil {
			w.handlers.OnRemove(key, prev)
		}
	default:
		w.mu.Unlock()
	}
}

// capFingerprint returns a hash of the "static" configuration fields of an entity
// (everything except the state field).
func capFingerprint(data json.RawMessage) string {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return ""
	}
	delete(m, "state")
	b, _ := json.Marshal(m)
	return string(b)
}

func matchesQuery(key string, doc map[string]any, pattern string, filters []Filter) bool {
	if pattern != "" && !matchWatchPattern(key, pattern) {
		return false
	}
	return matchesFilters(doc, filters)
}

// matchesFilters evaluates all filters against a JSON document (AND logic).
func matchesFilters(doc map[string]any, filters []Filter) bool {
	for _, f := range filters {
		val := getNestedField(doc, f.Field)
		if !evalFilter(val, f) {
			return false
		}
	}
	return true
}

func matchWatchPattern(key, pattern string) bool {
	if pattern == "" || pattern == ">" {
		return true
	}
	kp := strings.Split(key, ".")
	pp := strings.Split(pattern, ".")
	return matchWatchSegments(kp, pp)
}

func matchWatchSegments(key, pat []string) bool {
	if len(pat) == 0 {
		return len(key) == 0
	}
	if pat[len(pat)-1] == ">" {
		if len(key) < len(pat)-1 {
			return false
		}
		return matchWatchSegments(key[:len(pat)-1], pat[:len(pat)-1])
	}
	if len(key) != len(pat) {
		return false
	}
	for i := range pat {
		if pat[i] != "*" && pat[i] != key[i] {
			return false
		}
	}
	return true
}

// getNestedField navigates a dot-path like "labels.PluginHomeassistant" into
// a nested map structure.
func getNestedField(doc map[string]any, path string) any {
	parts := strings.Split(path, ".")
	var current any = doc
	for _, p := range parts {
		m, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		current = m[p]
	}
	return current
}

// evalFilter applies a single filter predicate to a value.
func evalFilter(val any, f Filter) bool {
	// If the document value is an array (e.g. labels.X = ["GroupA"]),
	// match if ANY element satisfies the filter.
	if arr, ok := val.([]any); ok && f.Op != In && f.Op != Exists {
		for _, item := range arr {
			if evalFilter(item, f) {
				return true
			}
		}
		return false
	}

	switch f.Op {
	case Exists:
		return val != nil
	case Eq:
		return compareEq(val, f.Value)
	case Neq:
		return !compareEq(val, f.Value)
	case Gt:
		n, fv, ok := toNums(val, f.Value)
		return ok && n > fv
	case Gte:
		n, fv, ok := toNums(val, f.Value)
		return ok && n >= fv
	case Lt:
		n, fv, ok := toNums(val, f.Value)
		return ok && n < fv
	case Lte:
		n, fv, ok := toNums(val, f.Value)
		return ok && n <= fv
	case Contains:
		s, ok1 := toString(val)
		sv, ok2 := toString(f.Value)
		return ok1 && ok2 && strings.Contains(s, sv)
	case Prefix:
		s, ok1 := toString(val)
		sv, ok2 := toString(f.Value)
		return ok1 && ok2 && strings.HasPrefix(s, sv)
	case In:
		return evalIn(val, f.Value)
	default:
		return false
	}
}

func compareEq(a, b any) bool {
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

func toNum(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	}
	return 0, false
}

func toNums(a, b any) (float64, float64, bool) {
	na, ok1 := toNum(a)
	nb, ok2 := toNum(b)
	return na, nb, ok1 && ok2
}

func toString(v any) (string, bool) {
	s, ok := v.(string)
	return s, ok
}

func evalIn(val any, filterVal any) bool {
	arr, ok := filterVal.([]any)
	if !ok {
		return false
	}
	for _, item := range arr {
		if compareEq(val, item) {
			return true
		}
	}
	return false
}
