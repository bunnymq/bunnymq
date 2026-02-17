package storage

import (
	"fmt"
	"os"
)

type LogSegment struct {
	file *os.File
}

func NewLogSegment(dirPath string, startOffset int) (*LogSegment, error) {
	fileName := logFileName(dirPath, startOffset)
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("open %q file: %w", fileName, err)
	}

	return &LogSegment{
		file: file,
	}, nil
}

func (l *LogSegment) Close() error {
	return l.file.Close()
}

func (l *LogSegment) Append() {}

func (l *LogSegment) Read() {}
