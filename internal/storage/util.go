package storage

import (
	"path/filepath"
	"strconv"
	"strings"
)

func logFileName(dirPath string, offset int) string {
	return filepath.Join(dirPath, offsetFileName(offset)+logFileSuffix)
}

func indexFileName(dirPath string, offset int) string {
	return filepath.Join(dirPath, offsetFileName(offset)+indexFileSuffix)
}

func timeIndexFileName(dirPath string, offset int) string {
	return filepath.Join(dirPath, offsetFileName(offset)+timeIndexFileSuffix)
}

func offsetFileName(offset int) string {
	const length = 18
	base := strconv.Itoa(offset)
	return strings.Repeat("0", length-len(base)) + base
}
