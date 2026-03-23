package storage

// Operator constants for query filters.
const (
	Eq       = "eq"
	Neq      = "neq"
	Gt       = "gt"
	Gte      = "gte"
	Lt       = "lt"
	Lte      = "lte"
	Contains = "contains"
	Prefix   = "prefix"
	Exists   = "exists"
	In       = "in"
)

// Query describes a structured search over stored data.
// Pattern narrows by key (same wildcards as Search).
// Where filters are AND'd against the stored JSON document.
type Query struct {
	Pattern string   `json:"pattern"`
	Where   []Filter `json:"where,omitempty"`
}

// Filter is a single field predicate. Field uses dot-path notation
// to navigate nested JSON (e.g. "state.brightness").
type Filter struct {
	Field string `json:"field"`
	Op    string `json:"op"`
	Value any    `json:"value,omitempty"`
}
