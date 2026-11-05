FROM golang:1.24-alpine3.23 AS builder

RUN mkdir -p /build
WORKDIR /build

COPY go.mod go.sum ./
COPY common ./common
COPY article_processing_system/top_aggregator ./article_processing_system/top_aggregator

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/top_aggregator ./article_processing_system/top_aggregator

FROM busybox:latest
COPY --from=builder /build/bin/top_aggregator /top_aggregator
ENTRYPOINT ["./top_aggregator"]
