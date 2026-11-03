FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/outbox ./app/cmd

FROM alpine:3.22
COPY --from=build /out/outbox /usr/local/bin/outbox
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/outbox"]
