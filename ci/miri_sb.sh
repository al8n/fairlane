#!/bin/bash
set -euo pipefail

# Miri under Stacked Borrows, on one target.
#
# This script and `ci/miri_tb.sh` are deliberate near-duplicates: they differ in `MIRIFLAGS` and in
# nothing else, and are two files so that each workflow runs exactly one aliasing model.
#
# Usage: ci/miri_sb.sh <TARGET>

if [ -z "${1:-}" ]; then
  echo "Error: TARGET is not provided" >&2
  exit 1
fi

TARGET="$1"

# A C compiler for the target. `cargo miri test` builds the dev-dependencies for TARGET, and a
# build script among them (criterion's `alloca` compiles C) needs one. Miri itself links nothing.
if [ "$(uname)" = "Linux" ]; then
  packages=""
  case "$TARGET" in
    aarch64-unknown-linux-gnu) packages="gcc-aarch64-linux-gnu" ;;
    i686-unknown-linux-gnu) packages="gcc-multilib" ;;
    powerpc64-unknown-linux-gnu) packages="gcc-powerpc64-linux-gnu" ;;
    s390x-unknown-linux-gnu) packages="gcc-s390x-linux-gnu" ;;
    riscv64gc-unknown-linux-gnu) packages="gcc-riscv64-linux-gnu" ;;
  esac
  if [ -n "$packages" ]; then
    sudo apt-get update
    sudo apt-get install -y --no-install-recommends "$packages"
  fi
fi

rustup toolchain install nightly --profile minimal --component miri --component rust-src
rustup override set nightly
cargo miri setup

export MIRIFLAGS="-Zmiri-strict-provenance -Zmiri-disable-isolation -Zmiri-symbolic-alignment-check"

# fairlane's unit tests: the ring, the remaining-capacity word, the reserves and the permits, and
# the channel's smoke tests. Its only `unsafe` is the ring's, and every test that reaches it is a
# unit test. Suites that only repeat the sequential laws over many random seeds carry
# `#[cfg_attr(miri, ignore)]`: interpreting them re-proves what one seed proves. The derive and the
# bench member hold no `unsafe`.
cargo miri test -p fairlane --lib --target "$TARGET" --all-features
