#!/usr/bin/env sh

docker-compose -f ./test/docker-compose.yml up --exit-code-from test --build