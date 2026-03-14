package partition

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	sm "github.com/lni/dragonboat/v4/statemachine"
)

// TestPartitionFSM_Determinism_AppendBatch applies 10 AppendBatch entries to two
// independent PartitionFSM instances and verifies byte-equal storage content.
func TestPartitionFSM_Determinism_AppendBatch(t *testing.T) {
	fsmA := openFSM(t)
	fsmB := openFSM(t)

	if _, err := fsmA.Open(nil); err != nil {
		t.Fatalf("fsmA.Open: %v", err)
	}
	if _, err := fsmB.Open(nil); err != nil {
		t.Fatalf("fsmB.Open: %v", err)
	}

	for i := 1; i <= 10; i++ {
		payload := makeBatch(t, fmt.Sprintf("msg-%d", i), int64(i*100))
		cmdA := append([]byte{CmdAppendBatch}, payload...)
		cmdB := append([]byte{CmdAppendBatch}, payload...)

		if _, err := fsmA.Update([]sm.Entry{{Index: uint64(i), Cmd: cmdA}}); err != nil {
			t.Fatalf("fsmA.Update[%d]: %v", i, err)
		}
		if _, err := fsmB.Update([]sm.Entry{{Index: uint64(i), Cmd: cmdB}}); err != nil {
			t.Fatalf("fsmB.Update[%d]: %v", i, err)
		}
	}

	dataA, _, err := fsmA.storage.Read(0, 1<<20)
	if err != nil {
		t.Fatalf("fsmA.Read: %v", err)
	}
	dataB, _, err := fsmB.storage.Read(0, 1<<20)
	if err != nil {
		t.Fatalf("fsmB.Read: %v", err)
	}

	if !bytes.Equal(dataA, dataB) {
		t.Fatalf("batch sequences differ: A=%d bytes, B=%d bytes", len(dataA), len(dataB))
	}
	if len(dataA) == 0 {
		t.Fatal("expected non-empty batch data")
	}
}

// TestPartitionFSM_Determinism_RetentionConfig applies a RetentionConfig command
// followed by an AppendBatch to two FSM instances and verifies byte-equal storage.
func TestPartitionFSM_Determinism_RetentionConfig(t *testing.T) {
	fsmA := openFSM(t)
	fsmB := openFSM(t)

	if _, err := fsmA.Open(nil); err != nil {
		t.Fatalf("fsmA.Open: %v", err)
	}
	if _, err := fsmB.Open(nil); err != nil {
		t.Fatalf("fsmB.Open: %v", err)
	}

	rcPayload, _ := json.Marshal(RetentionConfigPayload{RetentionMs: 3_600_000, RetentionBytes: 1 << 30})
	retCmdA := append([]byte{CmdRetentionConfig}, rcPayload...)
	retCmdB := append([]byte{CmdRetentionConfig}, rcPayload...)

	if _, err := fsmA.Update([]sm.Entry{{Index: 1, Cmd: retCmdA}}); err != nil {
		t.Fatalf("fsmA RetentionConfig: %v", err)
	}
	if _, err := fsmB.Update([]sm.Entry{{Index: 1, Cmd: retCmdB}}); err != nil {
		t.Fatalf("fsmB RetentionConfig: %v", err)
	}

	batchPayload := makeBatch(t, "after-retention-cfg", 5000)
	appendCmdA := append([]byte{CmdAppendBatch}, batchPayload...)
	appendCmdB := append([]byte{CmdAppendBatch}, batchPayload...)

	if _, err := fsmA.Update([]sm.Entry{{Index: 2, Cmd: appendCmdA}}); err != nil {
		t.Fatalf("fsmA AppendBatch: %v", err)
	}
	if _, err := fsmB.Update([]sm.Entry{{Index: 2, Cmd: appendCmdB}}); err != nil {
		t.Fatalf("fsmB AppendBatch: %v", err)
	}

	dataA, _, err := fsmA.storage.Read(0, 1<<20)
	if err != nil {
		t.Fatalf("fsmA.Read: %v", err)
	}
	dataB, _, err := fsmB.storage.Read(0, 1<<20)
	if err != nil {
		t.Fatalf("fsmB.Read: %v", err)
	}

	if !bytes.Equal(dataA, dataB) {
		t.Fatalf("storage data differs after RetentionConfig+AppendBatch: A=%d bytes, B=%d bytes", len(dataA), len(dataB))
	}
	if len(dataA) == 0 {
		t.Fatal("expected non-empty batch data after AppendBatch")
	}
}
