package partition

import (
	"testing"
)

func TestPartitionCommand_Wire(t *testing.T) {
	cmd := PartitionCommand{Type: CmdAppendBatch, Payload: []byte("abc")}
	got := cmd.Marshal()
	want := []byte{0x01, 'a', 'b', 'c'}
	if len(got) != len(want) {
		t.Fatalf("length: got %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("byte[%d]: got %#x, want %#x", i, got[i], want[i])
		}
	}
}
