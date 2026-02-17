package storage

import (
	"fmt"
	"os"
)

type Storage struct {
}

func New(dirPath string) (*Storage, error) {
	err := os.MkdirAll(dirPath, os.ModeDir)
	if err != nil {
		return nil, fmt.Errorf("mkdir: %w", err)
	}

	return &Storage{}, nil
}

func (s *Storage) Read(offset int, maxBytes int) {

}
