FROM golang:1.25-alpine AS builder
WORKDIR /build
COPY . .
RUN CGO_ENABLED=0 go build -o /bunnymq ./cmd/bunnymq

FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl wget netcat-openbsd iputils-ping dnsutils \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /bunnymq /bunnymq
RUN mkdir -p /data
ENTRYPOINT ["/bunnymq"]
