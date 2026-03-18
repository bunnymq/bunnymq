#!/usr/bin/env bash
set -euo pipefail

# Ensure cluster is torn down even if tests fail.
trap 'docker-compose down -v' EXIT

docker-compose up -d --build
"$(dirname "$0")/wait-healthy.sh" 60
go test ./internal/integration/... -tags integration,docker -timeout 300s -v
