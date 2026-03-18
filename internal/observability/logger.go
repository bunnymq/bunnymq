package observability

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// NewLogger constructs a production zap.Logger with JSON output to stdout,
// RFC3339Nano timestamps keyed as "ts", and the given minimum level.
func NewLogger(level zapcore.Level, development bool) (*zap.Logger, error) {
	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(level)
	cfg.EncoderConfig.TimeKey = "ts"
	cfg.EncoderConfig.EncodeTime = zapcore.RFC3339NanoTimeEncoder
	return cfg.Build()
}

// NewNopLogger returns a no-op logger suitable for use in unit tests.
func NewNopLogger() *zap.Logger {
	return zap.NewNop()
}
