package partition

const (
	CmdAppendBatch    uint8 = 0x01
	CmdRetentionConfig uint8 = 0x02
)

// PartitionCommand is the wire-encoded command for a partition shard.
// Byte 0 is the command type; bytes [1:] are the type-specific payload.
type PartitionCommand struct {
	Type    uint8
	Payload []byte
}

// Marshal encodes the command to its wire format: [Type] + Payload.
func (c PartitionCommand) Marshal() []byte {
	out := make([]byte, 1+len(c.Payload))
	out[0] = c.Type
	copy(out[1:], c.Payload)
	return out
}

type PartitionQueryType string

const (
	QueryRead            PartitionQueryType = "read"
	QueryReadByTime      PartitionQueryType = "read_by_time"
	QueryEarliestOffset  PartitionQueryType = "earliest_offset"
	QueryLatestOffset    PartitionQueryType = "latest_offset"
)

type PartitionQuery struct {
	Type        PartitionQueryType
	Offset      int64
	TimestampMs int64
	MaxBytes    int
}

type RetentionConfigPayload struct {
	RetentionMs    int64 `json:"retention_ms"`
	RetentionBytes int64 `json:"retention_bytes"`
}
