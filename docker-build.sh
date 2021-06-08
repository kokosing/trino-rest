#!/usr/bin/env bash

set -euo pipefail

VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
TAG=nineinchnick/trino-rest:$VERSION

docker build \
    -t "$TAG" \
    --build-arg VERSION="$VERSION" \
    .

docker push "$TAG"
