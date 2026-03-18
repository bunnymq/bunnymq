FROM golang:1.25-alpine AS builder
WORKDIR /build
COPY . .
RUN CGO_ENABLED=0 go build -o /bunnymq ./cmd/bunnymq

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /bunnymq /bunnymq
ENTRYPOINT ["/bunnymq"]
