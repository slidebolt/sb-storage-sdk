package storage

import (
	"encoding/json"
	"testing"
	"time"

	messenger "github.com/slidebolt/sb-messenger-sdk"
)

type keyed string

func (k keyed) Key() string { return string(k) }

func respondStorage(t *testing.T, msg messenger.Messenger, subject string, handler func(*Message) response) {
	t.Helper()
	_, err := msg.Subscribe(subject, func(m *messenger.Message) {
		var out response
		out = handler((*Message)(m))
		data, err := json.Marshal(out)
		if err != nil {
			t.Error(err)
			return
		}
		if err := m.Respond(data); err != nil {
			t.Error(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := msg.Flush(); err != nil {
		t.Fatal(err)
	}
}

type Message = messenger.Message

func TestConnectSaveAndGet(t *testing.T) {
	msg, payload, err := messenger.MockWithPayload()
	if err != nil {
		t.Fatal(err)
	}
	defer msg.Close()

	respondStorage(t, msg, "storage.save", func(m *Message) response {
		var req saveRequest
		if err := json.Unmarshal(m.Data, &req); err != nil {
			t.Fatal(err)
		}
		if req.Key != "device.one" {
			t.Fatalf("save key: got %q want %q", req.Key, "device.one")
		}
		if string(req.Data) != `"value"` {
			t.Fatalf("save data: got %s want %s", string(req.Data), `"value"`)
		}
		return response{OK: true}
	})

	respondStorage(t, msg, "storage.get", func(m *Message) response {
		var req getRequest
		if err := json.Unmarshal(m.Data, &req); err != nil {
			t.Fatal(err)
		}
		if req.Key != "device.one" {
			t.Fatalf("get key: got %q want %q", req.Key, "device.one")
		}
		return response{OK: true, Data: json.RawMessage(`{"name":"Device 1"}`)}
	})

	client, err := Connect(map[string]json.RawMessage{"messenger": payload})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Save(rawValue{key: "device.one", data: json.RawMessage(`"value"`)}); err != nil {
		t.Fatal(err)
	}
	got, err := client.Get(keyed("device.one"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"name":"Device 1"}` {
		t.Fatalf("get data: got %s", string(got))
	}
}

func TestQueryReturnsEntriesAndServiceErrorsSurface(t *testing.T) {
	msg, payload, err := messenger.MockWithPayload()
	if err != nil {
		t.Fatal(err)
	}
	defer msg.Close()

	respondStorage(t, msg, "storage.query", func(m *Message) response {
		var req queryRequest
		if err := json.Unmarshal(m.Data, &req); err != nil {
			t.Fatal(err)
		}
		if req.Pattern != "plugin.dev.*" || len(req.Where) != 1 {
			t.Fatalf("unexpected query request: %+v", req)
		}
		return response{
			OK: true,
			Entries: []Entry{
				{Key: "plugin.dev.ent", Data: json.RawMessage(`{"power":true}`)},
			},
		}
	})

	client, err := Connect(map[string]json.RawMessage{"messenger": payload})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	entries, err := client.Query(Query{
		Pattern: "plugin.dev.*",
		Where:   []Filter{{Field: "power", Op: Eq, Value: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Key != "plugin.dev.ent" {
		t.Fatalf("unexpected entries: %+v", entries)
	}

	respondStorage(t, msg, "storage.delete", func(m *Message) response {
		return response{OK: false, Error: "boom"}
	})

	err = client.Delete(keyed("plugin.dev.ent"))
	if err == nil {
		t.Fatal("expected delete error")
	}
}

type rawValue struct {
	key  string
	data json.RawMessage
}

func (r rawValue) Key() string                  { return r.key }
func (r rawValue) MarshalJSON() ([]byte, error) { return r.data, nil }

func TestReadFileUsesTargetedGetRequest(t *testing.T) {
	msg, payload, err := messenger.MockWithPayload()
	if err != nil {
		t.Fatal(err)
	}
	defer msg.Close()

	respondStorage(t, msg, "storage.get", func(m *Message) response {
		var req getRequest
		if err := json.Unmarshal(m.Data, &req); err != nil {
			t.Fatal(err)
		}
		if req.Target != Internal {
			t.Fatalf("target: got %q want %q", req.Target, Internal)
		}
		return response{OK: true, Data: json.RawMessage(`{"raw":true}`)}
	})

	client, err := Connect(map[string]json.RawMessage{"messenger": payload})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	got, err := client.ReadFile(Internal, keyed("device.private"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"raw":true}` {
		t.Fatalf("data: got %s", string(got))
	}

	time.Sleep(10 * time.Millisecond)
}

func TestWriteAndSearchFilesUseTargetedRequests(t *testing.T) {
	msg, payload, err := messenger.MockWithPayload()
	if err != nil {
		t.Fatal(err)
	}
	defer msg.Close()

	respondStorage(t, msg, "storage.save", func(m *Message) response {
		var req saveRequest
		if err := json.Unmarshal(m.Data, &req); err != nil {
			t.Fatal(err)
		}
		if req.Target != Source {
			t.Fatalf("save target: got %q want %q", req.Target, Source)
		}
		if req.Key != "sb-script.scripts.party_time" {
			t.Fatalf("save key: got %q", req.Key)
		}
		if string(req.Data) != `"print(\"v1\")"` {
			t.Fatalf("save data: got %s", req.Data)
		}
		return response{OK: true}
	})

	respondStorage(t, msg, "storage.search", func(m *Message) response {
		var req queryRequest
		if err := json.Unmarshal(m.Data, &req); err != nil {
			t.Fatal(err)
		}
		if req.Target != Source {
			t.Fatalf("search target: got %q want %q", req.Target, Source)
		}
		if req.Pattern != "sb-script.scripts.*" {
			t.Fatalf("search pattern: got %q", req.Pattern)
		}
		return response{OK: true, Entries: []Entry{{Key: "sb-script.scripts.party_time", Data: json.RawMessage(`"print(\"v1\")"`)}}}
	})

	client, err := Connect(map[string]json.RawMessage{"messenger": payload})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.WriteFile(Source, keyed("sb-script.scripts.party_time"), json.RawMessage(`"print(\"v1\")"`)); err != nil {
		t.Fatal(err)
	}
	entries, err := client.SearchFiles(Source, "sb-script.scripts.*")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Key != "sb-script.scripts.party_time" {
		t.Fatalf("entries: %+v", entries)
	}
}

func TestPrivateHelpersUsePrivateTarget(t *testing.T) {
	msg, payload, err := messenger.MockWithPayload()
	if err != nil {
		t.Fatal(err)
	}
	defer msg.Close()

	respondStorage(t, msg, "storage.save", func(m *Message) response {
		var req saveRequest
		if err := json.Unmarshal(m.Data, &req); err != nil {
			t.Fatal(err)
		}
		if req.Target != Private {
			t.Fatalf("save target: got %q want %q", req.Target, Private)
		}
		if req.Key != "device.private" {
			t.Fatalf("save key: got %q", req.Key)
		}
		return response{OK: true}
	})

	respondStorage(t, msg, "storage.get", func(m *Message) response {
		var req getRequest
		if err := json.Unmarshal(m.Data, &req); err != nil {
			t.Fatal(err)
		}
		if req.Target != Private {
			t.Fatalf("get target: got %q want %q", req.Target, Private)
		}
		return response{OK: true, Data: json.RawMessage(`{"apiKey":"secret"}`)}
	})

	respondStorage(t, msg, "storage.delete", func(m *Message) response {
		var req deleteRequest
		if err := json.Unmarshal(m.Data, &req); err != nil {
			t.Fatal(err)
		}
		if req.Target != Private {
			t.Fatalf("delete target: got %q want %q", req.Target, Private)
		}
		return response{OK: true}
	})

	client, err := Connect(map[string]json.RawMessage{"messenger": payload})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.SetPrivate(keyed("device.private"), json.RawMessage(`{"apiKey":"secret"}`)); err != nil {
		t.Fatal(err)
	}
	got, err := client.GetPrivate(keyed("device.private"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"apiKey":"secret"}` {
		t.Fatalf("get data: got %s", string(got))
	}
	if err := client.DeletePrivate(keyed("device.private")); err != nil {
		t.Fatal(err)
	}
}

func TestQueryDefinitionHelpersRoundTrip(t *testing.T) {
	msg, payload, err := messenger.MockWithPayload()
	if err != nil {
		t.Fatal(err)
	}
	defer msg.Close()

	respondStorage(t, msg, "storage.save", func(m *Message) response {
		return response{OK: true}
	})

	respondStorage(t, msg, "storage.get", func(m *Message) response {
		var req getRequest
		if err := json.Unmarshal(m.Data, &req); err != nil {
			t.Fatal(err)
		}
		if req.Key != "sb-query.queries.rgb_lights" {
			t.Fatalf("get key: got %q", req.Key)
		}
		return response{OK: true, Data: json.RawMessage(`{
			"type":"query",
			"name":"rgb_lights",
			"query":{"pattern":"test.>","where":[{"field":"type","op":"eq","value":"light"}]}
		}`)}
	})

	respondStorage(t, msg, "storage.search", func(m *Message) response {
		var req queryRequest
		if err := json.Unmarshal(m.Data, &req); err != nil {
			t.Fatal(err)
		}
		if req.Pattern != "sb-query.queries.*" {
			t.Fatalf("search pattern: got %q", req.Pattern)
		}
		return response{OK: true, Entries: []Entry{
			{
				Key: "sb-query.queries.rgb_lights",
				Data: json.RawMessage(`{
					"type":"query",
					"name":"rgb_lights",
					"query":{"pattern":"test.>","where":[{"field":"type","op":"eq","value":"light"}]}
				}`),
			},
		}}
	})

	client, err := Connect(map[string]json.RawMessage{"messenger": payload})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	query := Query{
		Pattern: "test.>",
		Where:   []Filter{{Field: "type", Op: Eq, Value: "light"}},
	}
	if err := EnsureQueryLayout(client); err != nil {
		t.Fatal(err)
	}
	if err := SaveQueryDefinition(client, "rgb_lights", query); err != nil {
		t.Fatal(err)
	}

	def, err := GetQueryDefinition(client, "rgb_lights")
	if err != nil {
		t.Fatal(err)
	}
	if def.Name != "rgb_lights" || def.Type != "query" || def.Query.Pattern != "test.>" {
		t.Fatalf("unexpected definition: %+v", def)
	}

	resolved, err := ResolveQueryRef(client, "rgb_lights")
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Pattern != "test.>" || len(resolved.Where) != 1 {
		t.Fatalf("unexpected resolved query: %+v", resolved)
	}

	defs, err := ListQueryDefinitions(client)
	if err != nil {
		t.Fatal(err)
	}
	if len(defs) != 1 || defs[0].Name != "rgb_lights" {
		t.Fatalf("unexpected query definitions: %+v", defs)
	}
}
