#!/usr/bin/env bash

set -euo pipefail

VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
TAG=nineinchnick/trino-rest:$VERSION

connectors=("$@")

if [ ${#connectors} == 0 ]; then
    connectors=(github slack twitter)
fi

opts=()
for c in "${connectors[@]}"; do
    opts+=(-e "${c^^}"_TOKEN -v "$(pwd)/catalog/$c.properties:/etc/trino/catalog/$c.properties")
done

docker run \
    --name trino-rest \
    -d \
    -p8080:8080 \
    --tmpfs /etc/trino/catalog \
    "${opts[@]}" \
    "$TAG"
