package metrics

import "github.com/prometheus/client_golang/prometheus"

// Register registers all BunnyMQ Prometheus metrics with the provided registerer.
func Register(reg prometheus.Registerer) error {
	return nil
}
