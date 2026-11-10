#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
: "${PROTOC:=protoc}"
[[ "$("$PROTOC" --version)" == "libprotoc 33.5" ]] || { echo "protoc 33.5 required" >&2; exit 1; }
[[ "$(protoc-gen-go --version)" == "protoc-gen-go v1.36.10" ]] || { echo "protoc-gen-go v1.36.10 required" >&2; exit 1; }
include_args=()
if [[ -n "${PROTOC_INCLUDE:-}" ]]; then
  include_args=(-I "$PROTOC_INCLUDE")
elif [[ -f /opt/homebrew/include/google/protobuf/timestamp.proto ]]; then
  include_args=(-I /opt/homebrew/include)
elif [[ -f /usr/local/include/google/protobuf/timestamp.proto ]]; then
  include_args=(-I /usr/local/include)
fi
mkdir -p gen/{java,python,ruby,go}
"$PROTOC" -I contracts/proto "${include_args[@]}" --java_out=gen/java --python_out=gen/python \
  --ruby_out=gen/ruby --go_out=gen/go --go_opt=paths=source_relative \
  --descriptor_set_out=contracts/notification.desc --include_imports \
  contracts/proto/relayguard/v1/notification.proto
