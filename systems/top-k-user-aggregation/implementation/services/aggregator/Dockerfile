FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod ./
COPY go.sum* ./
COPY . .
RUN go mod tidy
RUN CGO_ENABLED=0 go build -o aggregator .

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/aggregator .

ENV KAFKA_BROKER=kafka:9092
ENV CASSANDRA_HOSTS=cassandra
ENV CONSUMER_GROUP=aggregator
ENV FLUSH_INTERVAL=30s

CMD ["./aggregator"]
