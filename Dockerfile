FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN go test ./... && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /watchtower ./cmd/watchtower

FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S watchtower \
    && adduser -S -G watchtower watchtower \
    && mkdir -p /data \
    && chown watchtower:watchtower /data
USER watchtower
COPY --from=build /watchtower /usr/local/bin/watchtower
EXPOSE 8080 3001
VOLUME ["/data"]
ENTRYPOINT ["watchtower"]
