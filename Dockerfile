# Multi-stage Dockerfile for Tracker Auto-Assigner (Open-Core)
# Stage 1: Build static binary
FROM golang:1.25-alpine AS builder

RUN apk add --no-cache ca-certificates git tzdata

WORKDIR /src

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build static binary with optimizations
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w -extldflags '-static'" \
    -trimpath \
    -o /bin/tracker-assigner \
    ./cmd/tracker-assigner

# Stage 2: Minimal runtime image
FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S appgroup && adduser -S appuser -G appgroup \
    && mkdir -p /app /data /etc/tracker-assigner \
    && chown -R appuser:appgroup /app /data /etc/tracker-assigner

WORKDIR /app
COPY --from=builder /bin/tracker-assigner /app/tracker-assigner

USER appuser

EXPOSE 8080

VOLUME ["/data"]

ENTRYPOINT ["/app/tracker-assigner"]
CMD ["-config", "/etc/tracker-assigner/config.yaml"]
