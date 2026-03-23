package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
)

type PullProfilesOptions struct {
	OutDir  string
	Pattern string
}

type ProfileRestoreOptions struct {
	SourceDir string
}

type RestoreAction string

const (
	RestoreActionCreate    RestoreAction = "create"
	RestoreActionUpdate    RestoreAction = "update"
	RestoreActionUnchanged RestoreAction = "unchanged"
)

type RestoreOperation struct {
	Key    string        `json:"key"`
	Action RestoreAction `json:"action"`
}

type RestorePlan struct {
	Operations []RestoreOperation `json:"operations"`
}

type manifest struct {
	Kind    string `json:"kind"`
	Pattern string `json:"pattern,omitempty"`
	Count   int    `json:"count"`
}

func PullProfiles(store Storage, opts PullProfilesOptions) error {
	if opts.OutDir == "" {
		return fmt.Errorf("pull profiles: out dir is required")
	}
	pattern := opts.Pattern
	if pattern == "" {
		pattern = ">"
	}
	entries, err := store.SearchFiles(Profile, pattern)
	if err != nil {
		return err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
	for _, entry := range entries {
		if err := writeProfile(opts.OutDir, entry.Key, entry.Data); err != nil {
			return err
		}
	}
	body, err := json.MarshalIndent(manifest{Kind: "sb-profile-backup", Pattern: pattern, Count: len(entries)}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(opts.OutDir, "manifest.json"), body, 0644)
}

func PlanProfileRestore(store Storage, opts ProfileRestoreOptions) (RestorePlan, error) {
	files, err := readProfilesFromDir(opts.SourceDir)
	if err != nil {
		return RestorePlan{}, err
	}
	ops := make([]RestoreOperation, 0, len(files))
	for _, file := range files {
		current, err := store.ReadFile(Profile, profileStoreKey(file.Key))
		action := RestoreActionCreate
		if err == nil {
			if jsonEqual(current, file.Data) {
				action = RestoreActionUnchanged
			} else {
				action = RestoreActionUpdate
			}
		}
		ops = append(ops, RestoreOperation{Key: file.Key, Action: action})
	}
	return RestorePlan{Operations: ops}, nil
}

func ApplyProfileRestore(store Storage, opts ProfileRestoreOptions) (RestorePlan, error) {
	plan, err := PlanProfileRestore(store, opts)
	if err != nil {
		return RestorePlan{}, err
	}
	files, err := readProfilesFromDir(opts.SourceDir)
	if err != nil {
		return RestorePlan{}, err
	}
	byKey := make(map[string]json.RawMessage, len(files))
	for _, file := range files {
		byKey[file.Key] = file.Data
	}
	for _, op := range plan.Operations {
		if op.Action == RestoreActionUnchanged {
			continue
		}
		if err := store.SetProfile(profileStoreKey(op.Key), byKey[op.Key]); err != nil {
			return RestorePlan{}, err
		}
	}
	return plan, nil
}

type profileFile struct {
	Key  string
	Data json.RawMessage
}

type profileStoreKey string

func (k profileStoreKey) Key() string { return string(k) }

func readProfilesFromDir(root string) ([]profileFile, error) {
	if root == "" {
		return nil, fmt.Errorf("profile restore: source dir is required")
	}
	var out []profileFile
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(path, ".profile.json") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !json.Valid(data) {
			return fmt.Errorf("profile restore: invalid json: %s", path)
		}
		key, err := profileKey(root, path)
		if err != nil {
			return err
		}
		out = append(out, profileFile{Key: key, Data: json.RawMessage(data)})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func writeProfile(root, key string, data json.RawMessage) error {
	p := profilePath(root, key)
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return err
	}
	pretty, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(p, pretty, 0644)
}

func profilePath(root, key string) string {
	parts := strings.Split(key, ".")
	elems := append([]string{root}, parts...)
	return filepath.Join(filepath.Join(elems...), parts[len(parts)-1]+".profile.json")
}

func profileKey(root, path string) (string, error) {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return "", err
	}
	trimmed := strings.TrimSuffix(rel, ".profile.json")
	parts := strings.Split(trimmed, string(filepath.Separator))
	if len(parts) < 2 || parts[len(parts)-1] != parts[len(parts)-2] {
		return "", fmt.Errorf("profile restore: invalid path: %s", path)
	}
	parts = parts[:len(parts)-1]
	return strings.Join(parts, "."), nil
}

func jsonEqual(a, b json.RawMessage) bool {
	var av any
	var bv any
	if json.Unmarshal(a, &av) != nil || json.Unmarshal(b, &bv) != nil {
		return false
	}
	return reflect.DeepEqual(av, bv)
}
