FROM golang:1.24-alpine3.23 AS builder

RUN mkdir -p /build
WORKDIR /build

COPY go.mod go.sum ./
COPY common ./common
COPY abstractwordcounter ./abstractwordcounter

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/abstractwordcounter ./abstractwordcounter

FROM busybox:latest
COPY --from=builder /build/bin/abstractwordcounter /abstractwordcounter
ENTRYPOINT ["./abstractwordcounter"]
