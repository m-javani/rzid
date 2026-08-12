#!/usr/bin/env bash
set -euo pipefail

# cargo clean

cargo build  --release

strip --strip-all target/release/rzid

upx --best --lzma target/release/rzid

ls -lh target/release/rzid

cp target/release/rzid .
