# syntax=docker/dockerfile:1

# Builder stage
FROM golang:1.22-bullseye AS builder

WORKDIR /src

# Cache deps
COPY go.mod go.sum ./
RUN go mod download

# Copy sources
COPY . .

# Build broker (mbd)
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /app/mbd ./cmd/mbd

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app

RUN useradd -r -u 10001 wave && mkdir -p /data && chown -R wave:wave /data

COPY --from=builder /app/mbd /app/mbd

VOLUME ["/data"]

EXPOSE 7912 1883 8090

USER wave

ENTRYPOINT ["/app/mbd"]
CMD ["-data-dir=/data", "-bind=:7912", "-mqtt=:1883", "-http=:8090"]
