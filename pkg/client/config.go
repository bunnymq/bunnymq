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
