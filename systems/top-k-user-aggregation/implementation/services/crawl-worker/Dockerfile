FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod ./
COPY go.sum* ./
COPY . .
RUN go mod tidy
RUN CGO_ENABLED=0 go build -o crawl-worker .

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/crawl-worker .

ENV REDIS_ADDR=redis:6379
ENV KAFKA_BROKER=kafka:9092

CMD ["./crawl-worker"]
