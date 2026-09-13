#!/bin/bash
set -euo pipefail

# A release build replaces these two values with its immutable URL and checksum.
archive_url='@ARCHIVE_URL@'
archive_sha256='@ARCHIVE_SHA256@'
if [[ $(uname -s) != Darwin || $(uname -m) != arm64 ]]; then
  echo 'This release requires an Apple Silicon Mac.' >&2
  exit 1
fi
if [[ "$archive_url" == '@ARCHIVE_URL@' ]]; then
  echo 'Use install-t3-fork.sh from a built GitHub release, not this template.' >&2
  exit 1
fi
tmp=$(mktemp -d "${TMPDIR:-/tmp}/t3-fork-install.XXXXXX")
trap 'rm -rf "$tmp"' EXIT
curl --fail --location --proto '=https' --tlsv1.2 "$archive_url" -o "$tmp/release.tar.gz"
actual=$(shasum -a 256 "$tmp/release.tar.gz")
if [[ "${actual%% *}" != "$archive_sha256" ]]; then
  echo 'Download checksum mismatch. Nothing installed.' >&2
  exit 1
fi
tar -xzf "$tmp/release.tar.gz" -C "$tmp"
"$tmp/t3-fork/runtime/bin/node" "$tmp/t3-fork/install.mjs"
