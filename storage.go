// Package storage provides the client SDK for the SlideBolt storage service.
// Storage is a key-value store with dot-delimited hierarchical keys and
// wildcard search. All persistence is hidden behind this interface.
package storage

import (
	"encoding/json"
	"fmt"
	"time"

	messenger "github.com/slidebolt/sb-messenger-sdk"
)

const defaultTimeout = 5 * time.Second

// Keyed is implemented by types with a dot-delimited identity key.
type Keyed interface {
	Key() string
}

// StorageTarget controls how data is stored and whether it is queryable.
type StorageTarget string

const (
	// State is the normal target: data is indexed, queryable, and API-visible.
	// On raw-file APIs (ReadFile/SearchFiles/WriteFile/DeleteFile), this is the
	// raw main JSON file without merged profile/source sidecars.
	State StorageTarget = "state"
	// Profile is the user-owned sidecar target containing labels/meta/profile.
	Profile StorageTarget = "profile"
	// Source is the raw text sidecar target for script source (.lua files).
	Source StorageTarget = "source"
	// Private is the plugin-private durable target: data is persisted but never
	// indexed and never returned by Search or Query.
	Private StorageTarget = "private"
	// Internal is the plugin-private opaque target: data is persisted but never
	// indexed and never returned by Search or Query.
	Internal StorageTarget = "internal"
)

// Storage is the interface for storage operations.
// Save accepts any Keyed value and marshals it.
// Get returns raw JSON — caller unmarshals with domain types.
type Storage interface {
	Save(v Keyed) error
	Get(key Keyed) (json.RawMessage, error)
	Delete(key Keyed) error
	Search(pattern string) ([]Entry, error)
	SearchFiles(target StorageTarget, pattern string) ([]Entry, error)
	Query(q Query) ([]Entry, error)

	// WriteFile stores raw JSON under the given target namespace.
	// Use Private or Internal for plugin-private data that should not be
	// indexed or exposed to the query engine.
	WriteFile(target StorageTarget, key Keyed, data json.RawMessage) error
	// ReadFile retrieves raw JSON previously written with WriteFile.
	ReadFile(target StorageTarget, key Keyed) (json.RawMessage, error)
	// DeleteFile removes data written with WriteFile.
	DeleteFile(target StorageTarget, key Keyed) error

	SetPrivate(key Keyed, data json.RawMessage) error
	GetPrivate(key Keyed) (json.RawMessage, error)
	DeletePrivate(key Keyed) error

	SetInternal(key Keyed, data json.RawMessage) error
	GetInternal(key Keyed) (json.RawMessage, error)
	DeleteInternal(key Keyed) error

	// SetProfile writes user-owned fields (labels, meta, profile) to a
	// sidecar file that is never overwritten by Save(). The data parameter
	// should be a JSON object with any combination of "labels", "meta",
	// and "profile" keys.
	SetProfile(key Keyed, data json.RawMessage) error

	Close()
}

// Entry is a key-value pair returned by Search.
type Entry struct {
	Key  string          `json:"key"`
	Data json.RawMessage `json:"data"`
}

// --- Wire types (request/response over NATS) ---

type saveRequest struct {
	Key    string          `json:"key"`
	Data   json.RawMessage `json:"data"`
	Target StorageTarget   `json:"target,omitempty"`
}

type getRequest struct {
	Key    string        `json:"key"`
	Target StorageTarget `json:"target,omitempty"`
}

type deleteRequest struct {
	Key    string        `json:"key"`
	Target StorageTarget `json:"target,omitempty"`
}

type queryRequest struct {
	Pattern string        `json:"pattern"`
	Target  StorageTarget `json:"target,omitempty"`
	Where   []Filter      `json:"where,omitempty"`
}

type response struct {
	OK      bool            `json:"ok"`
	Data    json.RawMessage `json:"data,omitempty"`
	Entries []Entry         `json:"entries,omitempty"`
	Error   string          `json:"error,omitempty"`
}

// --- Client implementation ---

type client struct {
	msg messenger.Messenger
}

// Connect extracts the messenger payload from deps and returns
// a Storage client that communicates over NATS request/reply.
func Connect(deps map[string]json.RawMessage) (Storage, error) {
	msg, err := messenger.Connect(deps)
	if err != nil {
		return nil, fmt.Errorf("storage: %w", err)
	}
	return &client{msg: msg}, nil
}

// ConnectURL dials a NATS server directly and returns a Storage client.
func ConnectURL(url string) (Storage, error) {
	msg, err := messenger.ConnectURL(url)
	if err != nil {
		return nil, fmt.Errorf("storage: %w", err)
	}
	return &client{msg: msg}, nil
}

func (c *client) Save(v Keyed) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("storage: marshal: %w", err)
	}
	if len(data) == 0 || !json.Valid(data) {
		return fmt.Errorf("storage: marshal: invalid or empty JSON payload")
	}
	req, _ := json.Marshal(saveRequest{Key: v.Key(), Data: json.RawMessage(data)})
	return c.do("storage.save", req)
}

func (c *client) Get(key Keyed) (json.RawMessage, error) {
	req, _ := json.Marshal(getRequest{Key: key.Key()})
	resp, err := c.request("storage.get", req)
	if err != nil {
		return nil, err
	}
	return resp.Data, nil
}

func (c *client) Delete(key Keyed) error {
	req, _ := json.Marshal(deleteRequest{Key: key.Key()})
	return c.do("storage.delete", req)
}

func (c *client) Search(pattern string) ([]Entry, error) {
	req, _ := json.Marshal(queryRequest{Pattern: pattern})
	resp, err := c.request("storage.search", req)
	if err != nil {
		return nil, err
	}
	return resp.Entries, nil
}

func (c *client) SearchFiles(target StorageTarget, pattern string) ([]Entry, error) {
	req, _ := json.Marshal(queryRequest{Pattern: pattern, Target: target})
	resp, err := c.request("storage.search", req)
	if err != nil {
		return nil, err
	}
	return resp.Entries, nil
}

func (c *client) Query(q Query) ([]Entry, error) {
	req, _ := json.Marshal(queryRequest{Pattern: q.Pattern, Where: q.Where})
	resp, err := c.request("storage.query", req)
	if err != nil {
		return nil, err
	}
	return resp.Entries, nil
}

func (c *client) WriteFile(target StorageTarget, key Keyed, data json.RawMessage) error {
	req, _ := json.Marshal(saveRequest{Key: key.Key(), Data: data, Target: target})
	return c.do("storage.save", req)
}

func (c *client) ReadFile(target StorageTarget, key Keyed) (json.RawMessage, error) {
	req, _ := json.Marshal(getRequest{Key: key.Key(), Target: target})
	resp, err := c.request("storage.get", req)
	if err != nil {
		return nil, err
	}
	return resp.Data, nil
}

func (c *client) DeleteFile(target StorageTarget, key Keyed) error {
	req, _ := json.Marshal(deleteRequest{Key: key.Key(), Target: target})
	return c.do("storage.delete", req)
}

func (c *client) SetPrivate(key Keyed, data json.RawMessage) error {
	return c.WriteFile(Private, key, data)
}

func (c *client) GetPrivate(key Keyed) (json.RawMessage, error) {
	return c.ReadFile(Private, key)
}

func (c *client) DeletePrivate(key Keyed) error {
	return c.DeleteFile(Private, key)
}

func (c *client) SetInternal(key Keyed, data json.RawMessage) error {
	return c.WriteFile(Internal, key, data)
}

func (c *client) GetInternal(key Keyed) (json.RawMessage, error) {
	return c.ReadFile(Internal, key)
}

func (c *client) DeleteInternal(key Keyed) error {
	return c.DeleteFile(Internal, key)
}

func (c *client) SetProfile(key Keyed, data json.RawMessage) error {
	req, _ := json.Marshal(saveRequest{Key: key.Key(), Data: data})
	return c.do("storage.setprofile", req)
}

func (c *client) Close() {
	c.msg.Close()
}

// ClientFrom wraps an existing messenger connection as a Storage client.
// Used by server packages that manage their own messenger lifecycle.
func ClientFrom(msg messenger.Messenger) Storage {
	return &client{msg: msg}
}

func (c *client) do(subject string, data []byte) error {
	_, err := c.request(subject, data)
	return err
}

func (c *client) request(subject string, data []byte) (*response, error) {
	msg, err := c.msg.Request(subject, data, defaultTimeout)
	if err != nil {
		return nil, fmt.Errorf("storage: %s: %w", subject, err)
	}
	var resp response
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return nil, fmt.Errorf("storage: parse response: %w", err)
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("storage: %s", resp.Error)
	}
	return &resp, nil
}
