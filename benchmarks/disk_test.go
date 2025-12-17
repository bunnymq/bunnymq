package benchmarks

import (
	"math/rand"
	"os"
	"testing"
)

func Benchmark(b *testing.B) {
	benchCases := []struct {
		name     string
		fileName string
		flag     int
		dataSize int
	}{
		{
			name:     "4096B_no_sync",
			fileName: "./benchmark_4096.data",
			flag:     os.O_CREATE | os.O_RDWR,
			dataSize: 4096,
		},
		{
			name:     "4096B_sync",
			fileName: "./benchmark_sync_4096.data",
			flag:     os.O_CREATE | os.O_RDWR | os.O_SYNC,
			dataSize: 4096,
		},
		{
			name:     "128B_no_sync",
			fileName: "./benchmark_128.data",
			flag:     os.O_CREATE | os.O_RDWR,
			dataSize: 128,
		},
		{
			name:     "128B_sync",
			fileName: "./benchmark_sync_128.data",
			flag:     os.O_CREATE | os.O_RDWR | os.O_SYNC,
			dataSize: 128,
		},
		{
			name:     "65536B_no_sync",
			fileName: "./benchmark_sync_65536.data",
			flag:     os.O_CREATE | os.O_RDWR,
			dataSize: 65536,
		},
		{
			name:     "65536B_sync",
			fileName: "./benchmark_sync_65536.data",
			flag:     os.O_CREATE | os.O_RDWR | os.O_SYNC,
			dataSize: 65536,
		},
	}

	for _, benchCase := range benchCases {
		b.Run(benchCase.name, func(b *testing.B) {
			file, err := os.OpenFile(benchCase.fileName, benchCase.flag, 0)
			if err != nil {
				b.Error(err)
			}

			defer func() {
				if err := file.Close(); err != nil {
					b.Error(err)
				}
				if err := os.Remove(benchCase.fileName); err != nil {
					b.Error(err)
				}
			}()

			rnd := rand.New(rand.NewSource(42))

			data := make([]byte, benchCase.dataSize)
			rnd.Read(data)

			b.Run("sequential_sync", func(b *testing.B) {
				file.Write(data)
				file.Sync()
			})

			b.Run("sequential_no_sync", func(b *testing.B) {
				file.Write(data)
			})

			file.Write(make([]byte, 100*4096))
			file.Sync()

			b.Run("random_sync", func(b *testing.B) {
				file.WriteAt(data, rand.Int63n(100*4096))
				file.Sync()
			})

			b.Run("random_no_sync", func(b *testing.B) {
				file.WriteAt(data, rand.Int63n(100*4096))
				file.Sync()
			})

			b.Run("pure_sync", func(b *testing.B) {
				file.Sync()
			})
		})
	}
}
