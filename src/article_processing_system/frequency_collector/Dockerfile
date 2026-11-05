FROM golang:1.24-alpine3.23 AS builder

RUN mkdir -p /build
WORKDIR /build

COPY go.mod go.sum ./
COPY common ./common
COPY article_processing_system/frequency_collector ./article_processing_system/frequency_collector

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/frequency_collector ./article_processing_system/frequency_collector

FROM busybox:latest
COPY --from=builder /build/bin/frequency_collector /frequency_collector
ENTRYPOINT ["./frequency_collector"]
