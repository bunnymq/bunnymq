package client

// Record is a decoded message from a BunnyMQ partition.
type Record struct {
	Topic       string
	PartitionID int32
	Offset      int64
	Key         []byte
	Value       []byte
	Headers     map[string][]byte
	TimestampMs int64
}

// TP identifies a topic partition.
type TP struct {
	Topic       string
	PartitionID int32
}

// OffsetResetPolicy controls where consumption starts when no committed offset exists.
type OffsetResetPolicy int

const (
	OffsetResetLatest   OffsetResetPolicy = 0
	OffsetResetEarliest OffsetResetPolicy = 1
)
