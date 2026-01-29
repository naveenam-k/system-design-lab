FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod ./
COPY go.sum* ./
COPY . .
RUN go mod tidy
RUN CGO_ENABLED=0 go build -o api-server .

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/api-server .

ENV CASSANDRA_HOSTS=cassandra
ENV REDIS_ADDR=redis:6379
ENV PORT=8080
ENV CACHE_TTL=1h

EXPOSE 8080

CMD ["./api-server"]
