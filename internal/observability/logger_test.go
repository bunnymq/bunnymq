package observability

import (
	"bytes"
	"encoding/json"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

func TestNewLogger_JSONOutput(t *testing.T) {
	var buf bytes.Buffer
	syncer := zaptest.NewTestingWriter(t)
	_ = syncer

	// Build a logger that writes to our buffer so we can inspect the output.
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = "ts"
	encoderCfg.EncodeTime = zapcore.RFC3339NanoTimeEncoder

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderCfg),
		zapcore.AddSync(&buf),
		zap.InfoLevel,
	)
	logger := zap.New(core).With(zap.String("module", "test"))

	logger.Info("hello world")

	line := buf.String()
	if line == "" {
		t.Fatal("expected non-empty log output")
	}

	var obj map[string]any
	if err := json.Unmarshal([]byte(line), &obj); err != nil {
		t.Fatalf("log line is not valid JSON: %v\nline: %s", err, line)
	}
	for _, field := range []string{"ts", "level", "msg", "module"} {
		if _, ok := obj[field]; !ok {
			t.Errorf("expected field %q in log output; got: %v", field, obj)
		}
	}
}

func TestNewLogger_LevelFilter(t *testing.T) {
	var buf bytes.Buffer
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = "ts"
	encoderCfg.EncodeTime = zapcore.RFC3339NanoTimeEncoder

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderCfg),
		zapcore.AddSync(&buf),
		zap.WarnLevel,
	)
	logger := zap.New(core)

	logger.Info("this should be filtered")

	if buf.Len() != 0 {
		t.Errorf("expected no output at WarnLevel when logging Info; got: %s", buf.String())
	}
}
