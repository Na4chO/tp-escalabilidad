FROM golang:1.24-alpine3.23 AS builder

RUN mkdir -p /build
WORKDIR /build

COPY go.mod go.sum ./
COPY common ./common
COPY article_processing_system/server ./article_processing_system/server

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/server ./article_processing_system/server

FROM busybox:latest
COPY --from=builder /build/bin/server /server
ENTRYPOINT ["./server"]
