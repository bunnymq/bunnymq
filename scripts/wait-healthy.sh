#!/usr/bin/env bash
set -euo pipefail

TIMEOUT="${1:-60}"
DEADLINE=$(( $(date +%s) + TIMEOUT ))

echo "Waiting up to ${TIMEOUT}s for all services to be healthy..."

while true; do
    NOW=$(date +%s)
    if (( NOW >= DEADLINE )); then
        echo "Timeout after ${TIMEOUT}s — cluster not healthy."
        docker-compose logs
        exit 1
    fi

    # Count services that are healthy
    HEALTHY=$(docker-compose ps --format json 2>/dev/null \
        | grep -c '"Health":"healthy"' || true)

    if (( HEALTHY >= 3 )); then
        echo "All 3 services are healthy."
        exit 0
    fi

    sleep 2
done
