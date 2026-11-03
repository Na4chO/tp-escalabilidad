FROM golang:1.24-alpine3.23 AS builder

RUN mkdir -p /build
WORKDIR /build

COPY go.mod ./
COPY common ./common
COPY image_fetcher ./image_fetcher

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/image_fetcher ./image_fetcher

FROM busybox:latest
COPY --from=builder /build/bin/image_fetcher /image_fetcher
ENTRYPOINT ["./image_fetcher"]
