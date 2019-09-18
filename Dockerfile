ARG GO_VERSION=1.13

FROM golang:${GO_VERSION}-alpine

RUN apk update
RUN apk add --no-cache build-base git

WORKDIR /chiv

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .