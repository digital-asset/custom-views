#!/usr/bin/env bash

set -euo pipefail

dir="$( cd -- "${BASH_SOURCE[0]%/*}" &> /dev/null && pwd )"
cd $dir

if [ "$CI" != "true" ]; then
  echo "This script is meant to be run on CI only."
  exit 1
fi

commit_date=$(git log -n1 --format=%cd --date=format:%Y%m%d HEAD)
number_of_commits=$(git rev-list --count HEAD)
commit_sha_8=$(git log -n1 --format=%h --abbrev=8 HEAD)
# use the same base number as daml we depend on
base=$(git show HEAD:build.sbt | grep -Po '(?<=DamlVersion = ")[0-9]+\.[0-9]+\.[0-9]+')
snapshot="$base-snapshot-$commit_date-$number_of_commits-s$commit_sha_8"

VERSION=$snapshot sbt ci-release

deploy=$(git log -n1 --format="%(trailers:key=deploy,valueonly)" HEAD)

if [ "$deploy" = "true" ]; then
  tag=$(git log -n1 --format="%(trailers:key=tag,valueonly)" HEAD)
  VERSION=$tag sbt ci-release
fi
