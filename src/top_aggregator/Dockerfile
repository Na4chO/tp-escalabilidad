FROM golang:1.24-alpine3.23 AS builder

RUN mkdir -p /build
WORKDIR /build

COPY go.mod ./
COPY common ./common
COPY server ./top_aggregator

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/top_aggregator ./top_aggregator

FROM busybox:latest
COPY --from=builder /build/bin/top_aggregator /top_aggregator
ENTRYPOINT ["./top_aggregator"]
