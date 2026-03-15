package client

import (
	"crypto/tls"
	"time"
)

// Config holds fields shared by Producer, Consumer, and AdminClient.
type Config struct {
	BootstrapServers []string
	AuthToken        string
	TLS              *tls.Config
	RequestTimeout   time.Duration
	RetryPolicy      RetryPolicy
}

// RetryPolicy configures exponential backoff for retryable errors.
type RetryPolicy struct {
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	BackoffFactor  float64
}

// AcksMode controls how the broker acknowledges a produce request.
type AcksMode int32

const (
	// AcksAll waits for all in-sync replicas to acknowledge (acks=-1 equivalent).
	AcksAll AcksMode = 0
	// AcksZero fires and forgets; no acknowledgement from the broker.
	AcksZero AcksMode = 1
)
