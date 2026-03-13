package metadata

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestMetadataCommand_JSON(t *testing.T) {
	cmd := MetadataCommand{
		Type: CmdCreateTopic,
		CreateTopic: &CreateTopicCmd{
			Name:              "events",
			PartitionCount:    3,
			ReplicationFactor: 1,
			RetentionMs:       86400000,
			RetentionBytes:    0,
			CreatedAtMs:       1700000000000,
			ReplicaNodeIDs:    [][]uint64{{1}, {1}, {1}},
		},
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	s := string(data)

	if !strings.Contains(s, `"type"`) {
		t.Errorf("expected short alias \"type\" in JSON, got: %s", s)
	}
	if !strings.Contains(s, `"ct"`) {
		t.Errorf("expected short alias \"ct\" in JSON, got: %s", s)
	}

	// Nil fields must be omitted (omitempty).
	for _, absent := range []string{`"dt"`, `"atpc"`, `"atr"`, `"rn"`, `"apl"`, `"jcg"`, `"lcg"`, `"hcg"`, `"cco"`, `"rcg"`} {
		if strings.Contains(s, absent) {
			t.Errorf("expected absent field %s to be omitted, got: %s", absent, s)
		}
	}

	var got MetadataCommand
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Type != CmdCreateTopic {
		t.Errorf("type: got %q, want %q", got.Type, CmdCreateTopic)
	}
	if got.CreateTopic == nil {
		t.Fatal("CreateTopic payload missing after unmarshal")
	}
	if got.CreateTopic.Name != "events" {
		t.Errorf("name: got %q, want %q", got.CreateTopic.Name, "events")
	}
}

func TestResultEncoding(t *testing.T) {
	r := ErrorResult(ResultErrAlreadyExists, "topic exists")
	if r.Value != ResultErrAlreadyExists {
		t.Errorf("Value: got %d, want %d", r.Value, ResultErrAlreadyExists)
	}
	if string(r.Data) != "topic exists" {
		t.Errorf("Data: got %q, want %q", string(r.Data), "topic exists")
	}

	ok := OKResult()
	if ok.Value != ResultOK {
		t.Errorf("OKResult Value: got %d, want %d", ok.Value, ResultOK)
	}
}
