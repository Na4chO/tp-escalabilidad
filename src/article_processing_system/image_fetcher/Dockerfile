FROM golang:1.24-alpine3.23 AS builder

RUN mkdir -p /build
WORKDIR /build

COPY go.mod go.sum ./
COPY common ./common
COPY article_processing_system/image_fetcher ./article_processing_system/image_fetcher

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/image_fetcher ./article_processing_system/image_fetcher

FROM busybox:latest
# CA certificates for the HTTPS requests to Wikimedia
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /build/bin/image_fetcher /image_fetcher
ENTRYPOINT ["./image_fetcher"]
