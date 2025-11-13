FROM golang:1.25 AS builder

WORKDIR /src
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY go.mod go.sum ./
RUN go mod download

COPY . .
ENV CGO_ENABLED=0
ENV GOOS=linux
ENV GOARCH=amd64
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "-s -w" -o /gdrive-migrate

FROM scratch
COPY --from=builder /gdrive-migrate /usr/local/bin/gdrive-migrate
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

# replace with personalized volume mounts as needed
ENV APP_HOME=/app
WORKDIR ${APP_HOME}

ENTRYPOINT ["/usr/local/bin/gdrive-migrate"]
