ARG GO_VERSION=1.12

FROM golang:${GO_VERSION}-alpine

RUN apk update
RUN apk add --no-cache \
    build-base \
    make \
    git

WORKDIR /chiv

COPY go.mod .
COPY go.sum .
COPY Makefile .

RUN make setup

COPY . .