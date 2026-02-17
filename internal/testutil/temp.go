package testutil

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TempDir(t *testing.T) (string, func()) {
	tempDir, err := os.MkdirTemp("", "*")
	require.NoError(t, err)

	return tempDir, func() {
		err := os.RemoveAll(tempDir)
		assert.NoError(t, err)
	}
}

func FileExists(t *testing.T, path string) {
	_, err := os.Stat(path)
	require.Falsef(t, os.IsNotExist(err), "file %q must exist", path)
}
