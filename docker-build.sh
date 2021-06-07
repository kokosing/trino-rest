#!/usr/bin/env bash

set -euo pipefail

VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)

docker build \
    -t trino-rest:$VERSION \
    --build-arg VERSION=$VERSION \
    .
