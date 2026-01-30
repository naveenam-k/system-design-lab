FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY go.sum* ./
COPY . .
RUN go mod tidy
RUN CGO_ENABLED=0 go build -o crawl-scheduler .

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/crawl-scheduler .
CMD ["./crawl-scheduler"]
