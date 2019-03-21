ARG GO_VERSION=1.12

FROM golang:${GO_VERSION}-alpine

RUN apk update
RUN apk add --no-cache \
    build-base \
    make \
    git

WORKDIR /chiv
COPY . .

RUN make setup