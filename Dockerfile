# Build Stage
FROM golang:1.26 AS builder

WORKDIR /app

COPY go.mod .
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o mycache ./cmd/server

# Runtime Stage
FROM alpine:latest

WORKDIR /root

COPY --from=builder /app/mycache .

RUN mkdir -p /data

EXPOSE 6380

CMD ["./mycache"]