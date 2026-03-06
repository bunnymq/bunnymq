package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

var (
	ErrCRCMismatch = errors.New("CRC mismatch")

	crcTable = crc32.MakeTable(crc32.Castagnoli)
)

// RecordHeader is a key/value pair attached to a Record.
type RecordHeader struct {
	Key   []byte
	Value []byte
}

// Record is a single message within a batch.
// Key is nil when the record has no key.
type Record struct {
	TimestampMs int64
	Key         []byte
	Value       []byte
	Headers     []RecordHeader
}

// BatchHeader holds the 38-byte fixed prefix of every on-disk/on-wire batch.
type BatchHeader struct {
	BaseOffset    int64
	BatchLength   int32
	RecordCount   int32
	CRC32         uint32
	Attributes    int16
	BaseTimestamp int64
	MaxTimestamp  int64
}

// Batch is a decoded batch: header plus the decoded records.
type Batch struct {
	BatchHeader
	Records []Record
}

// EncodeBatch serialises records into the on-disk/on-wire batch format.
// base_offset is set to 0; Storage.Append overwrites bytes [0:8] with the
// real assigned offset before writing to disk (the CRC covers only records[],
// so the overwrite does not invalidate it).
func EncodeBatch(records []Record) ([]byte, error) {
	if len(records) == 0 {
		return nil, errors.New("records must not be empty")
	}

	baseTimestamp := records[0].TimestampMs
	maxTimestamp := records[len(records)-1].TimestampMs

	var recsBuf []byte
	for i, rec := range records {
		recsBuf = appendRecord(recsBuf, rec, baseTimestamp, int64(i))
	}

	batchLength := int32(38 + len(recsBuf))
	crc := crc32.Checksum(recsBuf, crcTable)

	out := make([]byte, 38+len(recsBuf))
	binary.BigEndian.PutUint64(out[0:8], 0)
	binary.BigEndian.PutUint32(out[8:12], uint32(batchLength))
	binary.BigEndian.PutUint32(out[12:16], uint32(len(records)))
	binary.BigEndian.PutUint32(out[16:20], crc)
	binary.BigEndian.PutUint16(out[20:22], 0)
	binary.BigEndian.PutUint64(out[22:30], uint64(baseTimestamp))
	binary.BigEndian.PutUint64(out[30:38], uint64(maxTimestamp))
	copy(out[38:], recsBuf)
	return out, nil
}

// DecodeBatch parses data as a single batch starting at byte 0, verifies the
// CRC-32C, and returns the decoded Batch.
func DecodeBatch(data []byte) (*Batch, error) {
	if len(data) < 38 {
		return nil, errors.New("batch too short")
	}

	bh := BatchHeader{
		BaseOffset:    int64(binary.BigEndian.Uint64(data[0:8])),
		BatchLength:   int32(binary.BigEndian.Uint32(data[8:12])),
		RecordCount:   int32(binary.BigEndian.Uint32(data[12:16])),
		CRC32:         binary.BigEndian.Uint32(data[16:20]),
		Attributes:    int16(binary.BigEndian.Uint16(data[20:22])),
		BaseTimestamp: int64(binary.BigEndian.Uint64(data[22:30])),
		MaxTimestamp:  int64(binary.BigEndian.Uint64(data[30:38])),
	}

	if int(bh.BatchLength) < 38 || int(bh.BatchLength) > len(data) {
		return nil, errors.New("batch data truncated")
	}

	recsData := data[38:bh.BatchLength]

	if crc32.Checksum(recsData, crcTable) != bh.CRC32 {
		return nil, ErrCRCMismatch
	}

	records := make([]Record, 0, bh.RecordCount)
	pos := 0
	for pos < len(recsData) {
		rec, newPos, err := decodeRecord(recsData, pos, bh.BaseTimestamp)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
		pos = newPos
	}

	return &Batch{BatchHeader: bh, Records: records}, nil
}

// DecodeNextBatch parses the batch at position pos within data, returning the
// decoded batch and the position immediately after it. Returns io.EOF when pos
// is at or beyond the end of data.
func DecodeNextBatch(data []byte, pos int) (*Batch, int, error) {
	if pos >= len(data) {
		return nil, pos, errors.New("no more batches")
	}

	batch, err := DecodeBatch(data[pos:])
	if err != nil {
		return nil, pos, err
	}

	return batch, pos + int(batch.BatchLength), nil
}

// appendRecord encodes one Record into buf and returns the extended buffer.
func appendRecord(buf []byte, rec Record, baseTimestamp int64, offsetDelta int64) []byte {
	var body []byte

	body = append(body, 0) // attributes int8

	tsDelta := rec.TimestampMs - baseTimestamp
	body = appendZigzagVarint(body, tsDelta)
	body = appendZigzagVarint(body, offsetDelta)

	if rec.Key == nil {
		body = appendZigzagVarint(body, -1)
	} else {
		body = appendZigzagVarint(body, int64(len(rec.Key)))
		body = append(body, rec.Key...)
	}

	body = appendUvarint(body, uint64(len(rec.Value)))
	body = append(body, rec.Value...)

	body = appendUvarint(body, uint64(len(rec.Headers)))
	for _, h := range rec.Headers {
		body = appendUvarint(body, uint64(len(h.Key)))
		body = append(body, h.Key...)
		body = appendUvarint(body, uint64(len(h.Value)))
		body = append(body, h.Value...)
	}

	buf = appendUvarint(buf, uint64(len(body)))
	buf = append(buf, body...)
	return buf
}

// decodeRecord parses one Record from data starting at pos.
// Returns the decoded Record and the position after it.
func decodeRecord(data []byte, pos int, baseTimestamp int64) (Record, int, error) {
	length, pos, err := decodeUvarint(data, pos)
	if err != nil {
		return Record{}, 0, err
	}

	end := pos + int(length)
	if end > len(data) {
		return Record{}, 0, errors.New("record extends beyond data")
	}

	// attributes int8
	if pos >= end {
		return Record{}, 0, errors.New("truncated record")
	}
	pos++ // skip attributes (always 0 in v1)

	tsDeltaRaw, pos, err := decodeUvarint(data, pos)
	if err != nil {
		return Record{}, 0, err
	}
	tsDelta := unzigzag(tsDeltaRaw)

	_, pos, err = decodeUvarint(data, pos) // offset_delta (not stored in Record)
	if err != nil {
		return Record{}, 0, err
	}

	keyLenRaw, pos, err := decodeUvarint(data, pos)
	if err != nil {
		return Record{}, 0, err
	}
	keyLen := unzigzag(keyLenRaw)

	var key []byte
	if keyLen >= 0 {
		if pos+int(keyLen) > end {
			return Record{}, 0, errors.New("key extends beyond record")
		}
		key = make([]byte, keyLen)
		copy(key, data[pos:pos+int(keyLen)])
		pos += int(keyLen)
	}

	valLen, pos, err := decodeUvarint(data, pos)
	if err != nil {
		return Record{}, 0, err
	}
	if pos+int(valLen) > end {
		return Record{}, 0, errors.New("value extends beyond record")
	}
	value := make([]byte, valLen)
	copy(value, data[pos:pos+int(valLen)])
	pos += int(valLen)

	headersCount, pos, err := decodeUvarint(data, pos)
	if err != nil {
		return Record{}, 0, err
	}

	headers := make([]RecordHeader, headersCount)
	for i := range headers {
		hKeyLen, newPos, err := decodeUvarint(data, pos)
		if err != nil {
			return Record{}, 0, err
		}
		pos = newPos
		if pos+int(hKeyLen) > end {
			return Record{}, 0, errors.New("header key extends beyond record")
		}
		hKey := make([]byte, hKeyLen)
		copy(hKey, data[pos:pos+int(hKeyLen)])
		pos += int(hKeyLen)

		hValLen, newPos, err := decodeUvarint(data, pos)
		if err != nil {
			return Record{}, 0, err
		}
		pos = newPos
		if pos+int(hValLen) > end {
			return Record{}, 0, errors.New("header value extends beyond record")
		}
		hVal := make([]byte, hValLen)
		copy(hVal, data[pos:pos+int(hValLen)])
		pos += int(hValLen)

		headers[i] = RecordHeader{Key: hKey, Value: hVal}
	}

	return Record{
		TimestampMs: baseTimestamp + tsDelta,
		Key:         key,
		Value:       value,
		Headers:     headers,
	}, end, nil
}

func appendUvarint(b []byte, v uint64) []byte {
	for v >= 0x80 {
		b = append(b, byte(v)|0x80)
		v >>= 7
	}
	return append(b, byte(v))
}

func appendZigzagVarint(b []byte, n int64) []byte {
	return appendUvarint(b, zigzag(n))
}

func zigzag(n int64) uint64 {
	return uint64((n << 1) ^ (n >> 63))
}

func unzigzag(n uint64) int64 {
	return int64((n >> 1) ^ -(n & 1))
}

func decodeUvarint(data []byte, pos int) (uint64, int, error) {
	var v uint64
	var shift uint
	for {
		if pos >= len(data) {
			return 0, 0, errors.New("truncated varint")
		}
		b := data[pos]
		pos++
		v |= uint64(b&0x7F) << shift
		if b < 0x80 {
			return v, pos, nil
		}
		shift += 7
		if shift >= 64 {
			return 0, 0, errors.New("varint overflow")
		}
	}
}
