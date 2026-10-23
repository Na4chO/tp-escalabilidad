FROM golang:1.24-alpine3.23 AS builder

RUN mkdir -p /build
WORKDIR /build

COPY go.mod ./
COPY common ./common
COPY server ./server

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/server ./server

FROM busybox:latest
COPY --from=builder /build/bin/server /server
ENTRYPOINT ["./server"]
