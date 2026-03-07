package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/bunnymq/bunnymq/internal/config"
)

// recoverStorage enumerates .log files in dir, opens sealed segments read-only,
// and recovers the active segment via CRC scan. Returns the segment list and
// nextOffset derived from the log.
func recoverStorage(dir string, cfg *config.StorageConfig) ([]*SegmentStorage, int64, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil {
		return nil, 0, err
	}

	if len(matches) == 0 {
		seg, err := NewSegmentStorage(dir, 0, cfg)
		if err != nil {
			return nil, 0, err
		}
		return []*SegmentStorage{seg}, 0, nil
	}

	sort.Strings(matches)

	var segments []*SegmentStorage

	// Open sealed segments (all but the last).
	for _, path := range matches[:len(matches)-1] {
		base := filepath.Base(path)
		offset, err := strconv.ParseInt(base[:len(base)-4], 10, 64)
		if err != nil {
			return nil, 0, fmt.Errorf("invalid segment filename %s: %w", path, err)
		}
		seg, err := OpenSegmentStorage(dir, offset, cfg, true)
		if err != nil {
			return nil, 0, err
		}
		segments = append(segments, seg)
	}

	// Recover active segment.
	activePath := matches[len(matches)-1]
	base := filepath.Base(activePath)
	activeOffset, err := strconv.ParseInt(base[:len(base)-4], 10, 64)
	if err != nil {
		return nil, 0, fmt.Errorf("invalid segment filename %s: %w", activePath, err)
	}

	validPos, nextOffset, err := scanActiveLog(activePath, activeOffset)
	if err != nil {
		return nil, 0, err
	}

	// Truncate the log to the last valid byte if needed.
	if err := os.Truncate(activePath, validPos); err != nil {
		return nil, 0, err
	}

	// Rebuild indexes from scratch (crash may have left them inconsistent).
	basePath := segmentBasePath(dir, activeOffset)
	_ = os.Remove(basePath + ".index")
	_ = os.Remove(basePath + ".timeindex")

	offsetIdx, err := OpenOffsetIndex(basePath+".index", activeOffset, cfg.SegmentMaxBytes, cfg.IndexSampleBytes)
	if err != nil {
		return nil, 0, err
	}
	timeIdx, err := OpenTimeIndex(basePath+".timeindex", activeOffset, cfg.SegmentMaxBytes, cfg.IndexSampleBytes)
	if err != nil {
		_ = offsetIdx.Close()
		return nil, 0, err
	}

	// Open active log for appending (stat reflects truncated size).
	logSeg, err := OpenLogSegment(activePath, activeOffset, true)
	if err != nil {
		_ = offsetIdx.Close()
		_ = timeIdx.Close()
		return nil, 0, err
	}

	// Populate indexes by scanning the valid portion.
	var offEntries []offsetEntry
	var tEntries []timeEntry
	bytesSince := int64(0)

	_ = logSeg.ScanFrom(0, func(batchBytes []byte, pos int64) bool {
		batchBase := int64(binary.BigEndian.Uint64(batchBytes[0:8]))
		maxTimestamp := int64(binary.BigEndian.Uint64(batchBytes[30:38]))
		relOff := int32(batchBase - activeOffset)

		if bytesSince >= int64(cfg.IndexSampleBytes) {
			offEntries = append(offEntries, offsetEntry{relativeOffset: relOff, position: int32(pos)})
			tEntries = append(tEntries, timeEntry{timestampMs: maxTimestamp, relativeOffset: relOff})
			bytesSince = 0
		} else {
			bytesSince += int64(len(batchBytes))
		}
		return true
	})

	offsetIdx.Rebuild(offEntries)
	timeIdx.Rebuild(tEntries)

	activeSeg := &SegmentStorage{
		log:                 logSeg,
		offsetIdx:           offsetIdx,
		timeIdx:             timeIdx,
		baseOffset:          activeOffset,
		bytesSinceLastIndex: bytesSince,
		config:              cfg,
	}

	segments = append(segments, activeSeg)
	return segments, nextOffset, nil
}

// scanActiveLog scans logPath from byte 0 with CRC-32C verification. Returns
// validPos (first invalid byte) and nextOffset derived from valid batches.
func scanActiveLog(logPath string, baseOffset int64) (validPos int64, nextOffset int64, err error) {
	f, err := os.Open(logPath)
	if err != nil {
		return 0, baseOffset, err
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return 0, baseOffset, err
	}
	fileSize := info.Size()

	pos := int64(0)
	currOffset := baseOffset

	for pos < fileSize {
		hdr := make([]byte, 38)
		n, _ := f.ReadAt(hdr, pos)
		if n < 38 {
			break
		}

		batchLen := int(binary.BigEndian.Uint32(hdr[8:12]))
		if batchLen < 38 || pos+int64(batchLen) > fileSize {
			break
		}

		records := make([]byte, batchLen-38)
		n, _ = f.ReadAt(records, pos+38)
		if n < batchLen-38 {
			break
		}

		storedCRC := binary.BigEndian.Uint32(hdr[16:20])
		if crc32.Checksum(records, crcTable) != storedCRC {
			break
		}

		recCount := int64(binary.BigEndian.Uint32(hdr[12:16]))
		base := int64(binary.BigEndian.Uint64(hdr[0:8]))
		currOffset = base + recCount
		pos += int64(batchLen)
	}

	return pos, currOffset, nil
}

// rebuildSegmentIndexes closes and recreates the index files for seg, then
// repopulates them by scanning the log. Used by TruncateTo.
func rebuildSegmentIndexes(dir string, seg *SegmentStorage, cfg *config.StorageConfig) error {
	basePath := segmentBasePath(dir, seg.baseOffset)

	if err := seg.offsetIdx.Close(); err != nil {
		return err
	}
	if err := seg.timeIdx.Close(); err != nil {
		return err
	}
	_ = os.Remove(basePath + ".index")
	_ = os.Remove(basePath + ".timeindex")

	offsetIdx, err := OpenOffsetIndex(basePath+".index", seg.baseOffset, cfg.SegmentMaxBytes, cfg.IndexSampleBytes)
	if err != nil {
		return err
	}
	timeIdx, err := OpenTimeIndex(basePath+".timeindex", seg.baseOffset, cfg.SegmentMaxBytes, cfg.IndexSampleBytes)
	if err != nil {
		_ = offsetIdx.Close()
		return err
	}

	var offEntries []offsetEntry
	var tEntries []timeEntry
	bytesSince := int64(0)

	_ = seg.log.ScanFrom(0, func(batchBytes []byte, pos int64) bool {
		batchBase := int64(binary.BigEndian.Uint64(batchBytes[0:8]))
		maxTimestamp := int64(binary.BigEndian.Uint64(batchBytes[30:38]))
		relOff := int32(batchBase - seg.baseOffset)

		if bytesSince >= int64(cfg.IndexSampleBytes) {
			offEntries = append(offEntries, offsetEntry{relativeOffset: relOff, position: int32(pos)})
			tEntries = append(tEntries, timeEntry{timestampMs: maxTimestamp, relativeOffset: relOff})
			bytesSince = 0
		} else {
			bytesSince += int64(len(batchBytes))
		}
		return true
	})

	offsetIdx.Rebuild(offEntries)
	timeIdx.Rebuild(tEntries)

	seg.offsetIdx = offsetIdx
	seg.timeIdx = timeIdx
	seg.bytesSinceLastIndex = bytesSince
	return nil
}

// findBytePosForOffset scans the log to find the byte position of the batch
// whose base_offset equals offset.
func findBytePosForOffset(seg *SegmentStorage, offset int64) (int64, error) {
	if offset == seg.baseOffset {
		return 0, nil
	}
	pos := int64(0)
	logSize := seg.log.logSizeVal.Load()
	for pos < logSize {
		header, err := seg.log.ReadAt(pos, 12)
		if err != nil {
			break
		}
		batchBase := int64(binary.BigEndian.Uint64(header[0:8]))
		if batchBase == offset {
			return pos, nil
		}
		batchLen := int64(binary.BigEndian.Uint32(header[8:12]))
		if batchLen < 12 {
			break
		}
		pos += batchLen
	}
	return logSize, nil
}
