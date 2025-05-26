#!/bin/sh

# Usage: ./fetch_genesis_config.sh [RPC_URL]
# Or set RPC_URL env var

RPC_URL="${1:-${RPC_URL:-https://rpc.mainnet.fastnear.com}}"
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

RESPONSE=$(curl -s -X POST "$RPC_URL" \
  -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":"dontcare","method":"EXPERIMENTAL_genesis_config"}')

echo "$RESPONSE" | jq '.result' > "$PROJECT_ROOT/genesis_config.json"
echo "Saved genesis config to $PROJECT_ROOT/genesis_config.json"