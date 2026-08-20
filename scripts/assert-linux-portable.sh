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

# readelf is the whole basis of the linkage check. If it is missing, every
# check below would find no INTERP and no NEEDED and report success, which is
# the exact fail-open behaviour this script exists to prevent.
if ! command -v readelf > /dev/null 2>&1; then
  echo "::error::readelf is not installed; cannot verify linkage"
  exit 1
fi

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
  # Capture the output and CHECK THE EXIT STATUS before searching it. With the
  # readelf call inside `if !`, `set -e` does not fire, stderr is discarded,
  # and a readelf that failed to run looks identical to one that found no
  # INTERP - so the guard would pass a dynamically linked binary. An absent
  # result and a failed lookup must not be the same thing.
  if ! program_headers="$(readelf -lW "$bin" 2>&1)"; then
    echo "::error::readelf could not read program headers from $bin"
    echo "$program_headers"
    status=1
    continue
  fi
  if grep -q 'INTERP' <<< "$program_headers"; then
    echo "::error::$bin declares a program interpreter, so it is dynamically linked"
    grep -A1 'INTERP' <<< "$program_headers" || true
    status=1
    continue
  fi

  if ! dynamic_section="$(readelf -dW "$bin" 2>&1)"; then
    echo "::error::readelf could not read the dynamic section of $bin"
    echo "$dynamic_section"
    status=1
    continue
  fi
  if grep -q 'NEEDED' <<< "$dynamic_section"; then
    echo "::error::$bin declares shared library dependencies"
    grep 'NEEDED' <<< "$dynamic_section" || true
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
