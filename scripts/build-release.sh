#!/bin/bash

# ./scripts/build-release.sh 0.0.0

set -e
VERSION=$1

mvn clean package

echo "Building release ${VERSION}"

INPUT=stringify-json-smt-${VERSION}-jar-with-deps.jar
OUTPUT=stringify-json-smt-${VERSION}.tar.gz

cd target
tar cfz "$OUTPUT" "$INPUT"
printf "Input file:\n$(realpath ${INPUT})\nBuilt into:\n$(realpath ${OUTPUT})"
cd ..