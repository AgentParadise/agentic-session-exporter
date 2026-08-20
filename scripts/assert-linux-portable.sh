#!/usr/bin/env bash
#
# Assert that a Linux binary carries no libc floor, and prove it by running it
# on the oldest base we support.
#
# This exists because v0.2.0 published Linux binaries that could not run on
# Debian 12. They were built for *-unknown-linux-gnu on a runner whose glibc
# had moved to 2.39; nothing in the repo changed, the floor moved underneath
# it. The only check that would have caught it ran after the image was already
# pushed and signed.
#
# It is a SCRIPT rather than inline workflow steps because three workflows
# (ci, release-gate, release) must agree about what a shippable Linux binary
# is. They had already drifted once - the gate went on building glibc targets
# after the release moved to musl - and a divergence there is invisible until
# a release breaks.
#
# Usage: assert-linux-portable.sh <binary>...
set -euo pipefail

if [ "$#" -eq 0 ]; then
  echo "::error::no binaries given; refusing to pass vacuously"
  exit 1
fi

# The consuming workspace image is built on Debian 12, so that is the floor.
# Running on the CI runner proves nothing: its glibc is NEWER than the floor,
# which is exactly how the original break passed its smoke test.
BASE_IMAGE="${PORTABLE_BASE_IMAGE:-debian:12-slim}"

status=0
for bin in "$@"; do
  if [ ! -f "$bin" ]; then
    echo "::error::$bin does not exist"
    status=1
    continue
  fi

  # A dynamic executable names an interpreter (PT_INTERP) and its shared
  # libraries (DT_NEEDED). A truly static one has neither. Checked with readelf
  # rather than `file`, whose human-readable wording varies by architecture -
  # x86_64 musl reports "static-pie linked" while aarch64 reports "statically
  # linked", and matching on prose is how a guard silently stops guarding.
  if readelf -l "$bin" 2>/dev/null | grep -q 'INTERP'; then
    echo "::error::$bin declares a program interpreter, so it is dynamically linked"
    readelf -l "$bin" 2>/dev/null | grep -A1 'INTERP' || true
    status=1
    continue
  fi

  if readelf -d "$bin" 2>/dev/null | grep -q 'NEEDED'; then
    echo "::error::$bin declares shared library dependencies"
    readelf -d "$bin" 2>/dev/null | grep 'NEEDED' || true
    status=1
    continue
  fi

  # Static per the headers is necessary but not sufficient: it still has to
  # execute. This is the check that reproduces the consumer.
  if ! docker run --rm -v "$(cd "$(dirname "$bin")" && pwd):/portable:ro" \
        "$BASE_IMAGE" "/portable/$(basename "$bin")" --version; then
    echo "::error::$bin does not run on $BASE_IMAGE"
    status=1
    continue
  fi

  echo "ok: $bin is static and runs on $BASE_IMAGE"
done

exit "$status"
