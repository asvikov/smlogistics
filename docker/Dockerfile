FROM golang:1.25-alpine AS builder

RUN CGO_ENABLED=0 \
    go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@v4.19.1

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS=linux
#ARG TARGETARCH=amd64
#in arg: GOARCH=${TARGETARCH}

RUN CGO_ENABLED=0 GOOS=${TARGETOS} \
    go build -ldflags="-s -w" -o /notificationd ./cmd/notificationd

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
COPY --from=builder /notificationd .
COPY --from=builder /go/bin/migrate /usr/local/bin/migrate
COPY migrations/ ./migrations/

USER nobody:nogroup
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["./notificationd"]
