# Scripts

This folder contains utility scripts for the project.

## Available Scripts

### `fetch_genesis_config.sh`

Fetches the NEAR genesis configuration from a specified RPC endpoint and saves it as `genesis_config.json` in the project root.

**Usage:**
```sh
./fetch_genesis_config.sh [RPC_URL]
```
- `RPC_URL` (optional): The NEAR RPC endpoint to fetch the genesis config from.
  If not provided, you can set the `RPC_URL` environment variable, or it will default to `https://rpc.mainnet.fastnear.com`.

**Example:**
```sh
./fetch_genesis_config.sh https://rpc.mainnet.fastnear.com
```

The resulting `genesis_config.json` will be saved in the root of the project.