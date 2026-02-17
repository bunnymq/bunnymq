package storage

import (
	"path/filepath"
	"testing"

	"github.com/bunnymq/bunnymq/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestNewLogSegment(t *testing.T) {
	tempDir, clean := testutil.TempDir(t)
	defer clean()

	logSegment1, err := NewLogSegment(tempDir, 0)
	require.NoError(t, err)
	defer logSegment1.Close()

	testutil.FileExists(t, filepath.Join(tempDir, "000000000000000000.log"))

	logSegment2, err := NewLogSegment(tempDir, 0)
	require.NoError(t, err)
	defer logSegment2.Close()
}
