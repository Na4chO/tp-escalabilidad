FROM golang:1.24-alpine3.23 AS builder

RUN mkdir -p /build
WORKDIR /build

COPY go.mod go.sum ./
COPY common ./common
COPY article_processing_system/metadata_filter ./article_processing_system/metadata_filter

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/metadata_filter ./article_processing_system/metadata_filter

FROM busybox:latest
COPY --from=builder /build/bin/metadata_filter /metadata_filter
ENTRYPOINT ["./metadata_filter"]
