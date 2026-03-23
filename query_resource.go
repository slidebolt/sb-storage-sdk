package storage

import (
	"encoding/json"
	"fmt"
)

// QueryRootKey stores shared query service metadata at the plugin root.
// Key form: sb-query
type QueryRootKey struct{}

func (QueryRootKey) Key() string { return "sb-query" }

// QueriesKey stores shared query collection metadata.
// Key form: sb-query.queries
type QueriesKey struct{}

func (QueriesKey) Key() string { return "sb-query.queries" }

type QueryRoot struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	Name string `json:"name"`
}

func (QueryRoot) Key() string { return QueryRootKey{}.Key() }

type Queries struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	Name string `json:"name"`
}

func (Queries) Key() string { return QueriesKey{}.Key() }

// QueryKey is the storage key for a named query definition.
// Key form: sb-query.queries.{name}
type QueryKey struct {
	Name string
}

func (k QueryKey) Key() string {
	return fmt.Sprintf("sb-query.queries.%s", k.Name)
}

// QueryDefinition is the canonical stored representation of a reusable query.
type QueryDefinition struct {
	Type  string `json:"type"`
	Name  string `json:"name"`
	Query Query  `json:"query"`
}

func (q QueryDefinition) Key() string {
	return QueryKey{Name: q.Name}.Key()
}

// EnsureQueryLayout writes the canonical query root and collection records.
func EnsureQueryLayout(store Storage) error {
	if err := store.Save(QueryRoot{
		ID:   "sb-query",
		Type: "service",
		Name: "sb-query",
	}); err != nil {
		return err
	}
	return store.Save(Queries{
		ID:   "queries",
		Type: "query-collection",
		Name: "Queries",
	})
}

func SaveQueryDefinition(store Storage, name string, query Query) error {
	return store.Save(QueryDefinition{
		Type:  "query",
		Name:  name,
		Query: query,
	})
}

func GetQueryDefinition(store Storage, name string) (QueryDefinition, error) {
	data, err := store.Get(QueryKey{Name: name})
	if err != nil {
		return QueryDefinition{}, err
	}
	var def QueryDefinition
	if err := json.Unmarshal(data, &def); err != nil {
		return QueryDefinition{}, fmt.Errorf("storage: parse query definition: %w", err)
	}
	return def, nil
}

func ResolveQueryRef(store Storage, name string) (Query, error) {
	def, err := GetQueryDefinition(store, name)
	if err != nil {
		return Query{}, err
	}
	return def.Query, nil
}

func ListQueryDefinitions(store Storage) ([]QueryDefinition, error) {
	entries, err := store.Search("sb-query.queries.*")
	if err != nil {
		return nil, err
	}
	defs := make([]QueryDefinition, 0, len(entries))
	for _, entry := range entries {
		var def QueryDefinition
		if err := json.Unmarshal(entry.Data, &def); err != nil {
			return nil, fmt.Errorf("storage: parse query definition %s: %w", entry.Key, err)
		}
		defs = append(defs, def)
	}
	return defs, nil
}
