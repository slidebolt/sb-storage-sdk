package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	messenger "github.com/slidebolt/sb-messenger-sdk"
)

func TestSearchFilesUsesTargetedSearchRequest(t *testing.T) {
	msg, payload, err := messenger.MockWithPayload()
	if err != nil {
		t.Fatal(err)
	}
	defer msg.Close()

	respondStorage(t, msg, "storage.search", func(m *Message) response {
		var req queryRequest
		if err := json.Unmarshal(m.Data, &req); err != nil {
			t.Fatal(err)
		}
		if req.Target != Profile {
			t.Fatalf("target: got %q want %q", req.Target, Profile)
		}
		if req.Pattern != "plugin.>" {
			t.Fatalf("pattern: got %q want %q", req.Pattern, "plugin.>")
		}
		return response{
			OK: true,
			Entries: []Entry{
				{Key: "plugin.device.entity", Data: json.RawMessage(`{"labels":{"room":["kitchen"]}}`)},
			},
		}
	})

	client, err := Connect(map[string]json.RawMessage{"messenger": payload})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	entries, err := client.SearchFiles(Profile, "plugin.>")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Key != "plugin.device.entity" {
		t.Fatalf("unexpected entries: %+v", entries)
	}
}

type fakeBackupStore struct {
	searchByTarget map[StorageTarget][]Entry
	readByTarget   map[StorageTarget]map[string]json.RawMessage
	setProfileKeys []string
	setProfileData map[string]json.RawMessage
}

func newFakeBackupStore() *fakeBackupStore {
	return &fakeBackupStore{
		searchByTarget: make(map[StorageTarget][]Entry),
		readByTarget:   make(map[StorageTarget]map[string]json.RawMessage),
		setProfileData: make(map[string]json.RawMessage),
	}
}

func (f *fakeBackupStore) Save(v Keyed) error                     { panic("unexpected Save") }
func (f *fakeBackupStore) Get(key Keyed) (json.RawMessage, error) { panic("unexpected Get") }
func (f *fakeBackupStore) Delete(key Keyed) error                 { panic("unexpected Delete") }
func (f *fakeBackupStore) Search(pattern string) ([]Entry, error) { panic("unexpected Search") }
func (f *fakeBackupStore) Query(q Query) ([]Entry, error)         { panic("unexpected Query") }
func (f *fakeBackupStore) WriteFile(target StorageTarget, key Keyed, data json.RawMessage) error {
	panic("unexpected WriteFile")
}
func (f *fakeBackupStore) ReadFile(target StorageTarget, key Keyed) (json.RawMessage, error) {
	if byKey, ok := f.readByTarget[target]; ok {
		if data, ok := byKey[key.Key()]; ok {
			return data, nil
		}
	}
	return nil, errNotFound(key.Key())
}
func (f *fakeBackupStore) DeleteFile(target StorageTarget, key Keyed) error {
	panic("unexpected DeleteFile")
}
func (f *fakeBackupStore) SearchFiles(target StorageTarget, pattern string) ([]Entry, error) {
	return append([]Entry(nil), f.searchByTarget[target]...), nil
}
func (f *fakeBackupStore) SetPrivate(key Keyed, data json.RawMessage) error {
	panic("unexpected SetPrivate")
}
func (f *fakeBackupStore) GetPrivate(key Keyed) (json.RawMessage, error) {
	panic("unexpected GetPrivate")
}
func (f *fakeBackupStore) DeletePrivate(key Keyed) error { panic("unexpected DeletePrivate") }
func (f *fakeBackupStore) SetInternal(key Keyed, data json.RawMessage) error {
	panic("unexpected SetInternal")
}
func (f *fakeBackupStore) GetInternal(key Keyed) (json.RawMessage, error) {
	panic("unexpected GetInternal")
}
func (f *fakeBackupStore) DeleteInternal(key Keyed) error { panic("unexpected DeleteInternal") }
func (f *fakeBackupStore) SetProfile(key Keyed, data json.RawMessage) error {
	f.setProfileKeys = append(f.setProfileKeys, key.Key())
	f.setProfileData[key.Key()] = append(json.RawMessage(nil), data...)
	return nil
}
func (f *fakeBackupStore) Close() {}

func errNotFound(key string) error { return &notFoundError{key: key} }

type notFoundError struct{ key string }

func (e *notFoundError) Error() string { return "not found: " + e.key }

func TestPullProfilesWritesProdLayout(t *testing.T) {
	store := newFakeBackupStore()
	store.searchByTarget[Profile] = []Entry{
		{Key: "plugin-one.device-a.entity-a", Data: json.RawMessage(`{"labels":{"room":["kitchen"]}}`)},
		{Key: "plugin-two.device-b.entity-b", Data: json.RawMessage(`{"profile":{"name":"Fixture"}}`)},
	}

	dir := t.TempDir()
	if err := PullProfiles(store, PullProfilesOptions{OutDir: dir, Pattern: "plugin.>"}); err != nil {
		t.Fatal(err)
	}

	got := []string{}
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			rel, err := filepath.Rel(dir, path)
			if err != nil {
				return err
			}
			got = append(got, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(got)

	want := []string{
		"manifest.json",
		"plugin-one/device-a/entity-a/entity-a.profile.json",
		"plugin-two/device-b/entity-b/entity-b.profile.json",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("files:\n got %v\nwant %v", got, want)
	}
}

func TestPlanProfileRestoreReportsCreateUpdateAndUnchanged(t *testing.T) {
	store := newFakeBackupStore()
	store.readByTarget[Profile] = map[string]json.RawMessage{
		"plugin.device.same":   json.RawMessage(`{"labels":{"room":["kitchen"]}}`),
		"plugin.device.change": json.RawMessage(`{"labels":{"room":["bedroom"]}}`),
	}

	dir := t.TempDir()
	writeProfileFile(t, dir, "plugin.device.same", `{"labels":{"room":["kitchen"]}}`)
	writeProfileFile(t, dir, "plugin.device.change", `{"labels":{"room":["office"]}}`)
	writeProfileFile(t, dir, "plugin.device.new", `{"labels":{"room":["garage"]}}`)

	plan, err := PlanProfileRestore(store, ProfileRestoreOptions{SourceDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Operations) != 3 {
		t.Fatalf("operations: got %d want 3", len(plan.Operations))
	}

	got := map[string]RestoreAction{}
	for _, op := range plan.Operations {
		got[op.Key] = op.Action
	}
	if got["plugin.device.same"] != RestoreActionUnchanged {
		t.Fatalf("same action: got %q", got["plugin.device.same"])
	}
	if got["plugin.device.change"] != RestoreActionUpdate {
		t.Fatalf("change action: got %q", got["plugin.device.change"])
	}
	if got["plugin.device.new"] != RestoreActionCreate {
		t.Fatalf("new action: got %q", got["plugin.device.new"])
	}
}

func TestApplyProfileRestoreWritesChangedProfilesOnly(t *testing.T) {
	store := newFakeBackupStore()
	store.readByTarget[Profile] = map[string]json.RawMessage{
		"plugin.device.same": json.RawMessage(`{"labels":{"room":["kitchen"]}}`),
	}

	dir := t.TempDir()
	writeProfileFile(t, dir, "plugin.device.same", `{"labels":{"room":["kitchen"]}}`)
	writeProfileFile(t, dir, "plugin.device.new", `{"labels":{"room":["garage"]}}`)

	plan, err := ApplyProfileRestore(store, ProfileRestoreOptions{SourceDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Operations) != 2 {
		t.Fatalf("operations: got %d want 2", len(plan.Operations))
	}
	if len(store.setProfileKeys) != 1 || store.setProfileKeys[0] != "plugin.device.new" {
		t.Fatalf("set profile keys: got %v want [plugin.device.new]", store.setProfileKeys)
	}
	if string(store.setProfileData["plugin.device.new"]) != `{"labels":{"room":["garage"]}}` {
		t.Fatalf("set profile data: got %s", store.setProfileData["plugin.device.new"])
	}
}

func writeProfileFile(t *testing.T, root, key, body string) {
	t.Helper()
	p := profilePath(root, key)
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), 0644); err != nil {
		t.Fatal(err)
	}
}
