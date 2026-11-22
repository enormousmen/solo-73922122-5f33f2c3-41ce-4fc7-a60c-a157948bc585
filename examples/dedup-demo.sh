#!/usr/bin/env bash
# Demo: block-wise upload dedup + last-byte divergence disabling a key.
#
#   1. builder-1 commits image + symbols          -> key is a miss (1 of 2)
#   2. builder-2 uploads the SAME bytes           -> server reports
#      "deduplicated": true (only the staging row is updated, no temp file,
#      no blob write), then commits                  -> key is a hit
#   3. builder-3 uploads an image differing only in its FINAL byte and the
#      identical symbols file (also deduplicated)   -> commit disables the key
#
# Usage:  ./examples/dedup-demo.sh [BASE_URL]     (default http://localhost:8080)
set -euo pipefail

BASE="${1:-http://localhost:8080}"
KEY="demo-dedup-1.0.0"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

pp() {  # pretty-print JSON if a formatter is available
  if command -v jq >/dev/null; then jq; else cat; fi
}
upload() { # key build kind file
  curl -sS -X PUT "$BASE/v1/keys/$1/builds/$2/files/$3" \
    -H 'Content-Type: application/octet-stream' --data-binary @"$4"
}
commit() { curl -sS -X POST "$BASE/v1/keys/$1/builds/$2/commit"; pp; }
status() { curl -sS "$BASE/v1/keys/$1"; pp; }

echo "### inputs: an image and a debug-symbols file"
printf 'FIRMWARE-IMAGE\0' > "$WORK/image.bin"
head -c $((1024 * 1024)) /dev/zero >> "$WORK/image.bin"
printf 'DEBUG-SYMBOLS\0' > "$WORK/symbols.bin"
head -c $((256 * 1024)) /dev/zero >> "$WORK/symbols.bin"
echo "image:   $(stat -c%s "$WORK/image.bin") bytes"
echo "symbols: $(stat -c%s "$WORK/symbols.bin") bytes"

echo; echo "### builder-1: normal upload (no committed copy to compare with)"
upload "$KEY" builder-1 image   "$WORK/image.bin";   echo
upload "$KEY" builder-1 symbols "$WORK/symbols.bin"; echo
commit "$KEY" builder-1
echo "--- key status (1 of 2 machines; dedup already works below this threshold):"
status "$KEY"

echo; echo "### builder-2: byte-identical uploads -> deduplicated:true (zero writes)"
upload "$KEY" builder-2 image   "$WORK/image.bin";   echo
upload "$KEY" builder-2 symbols "$WORK/symbols.bin"; echo
commit "$KEY" builder-2
echo "--- key status (two machines agree):"
status "$KEY"

echo; echo "### builder-3: image differs ONLY in the last byte"
cp "$WORK/image.bin" "$WORK/image-tail.bin"
LAST=$(( $(stat -c%s "$WORK/image-tail.bin") - 1 ))
printf '\x01' | dd of="$WORK/image-tail.bin" bs=1 seek="$LAST" count=1 \
  conv=notrunc status=none
cmp -l "$WORK/image.bin" "$WORK/image-tail.bin" || true
echo "--- upload the divergent image (must be fully staged, not deduplicated):"
upload "$KEY" builder-3 image "$WORK/image-tail.bin"; echo
echo "--- identical symbols are still deduplicated:"
upload "$KEY" builder-3 symbols "$WORK/symbols.bin"; echo
echo "--- commit detects the conflict and disables the key:"
commit "$KEY" builder-3
echo "--- key status (disabled, rebuild hinted, both digests listed):"
status "$KEY"
echo "--- fetch attempt now:"
code=$(curl -sS -o "$WORK/fetch.json" -w '%{http_code}' "$BASE/v1/keys/$KEY/files/image")
pp < "$WORK/fetch.json"
echo "HTTP $code"
