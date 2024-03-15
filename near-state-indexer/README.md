NEAR State Indexer
==================

Near State Indexer based on [NEAR Indexer Framework](https://github.com/nearprotocol/nearcore/tree/master/chain/indexer)
Near State Indexer is only designed to track the end of the network. 
It is not designed to index historical data.  
It is assumed that in a bet with near_state_indexer the [state-indexer](../state-indexer/README.md) will always be running.

## How to set up and test NEAR State Indexer

### localnet

To run the NEAR Indexer connected to a network we need to have configs and keys prepopulated. To generate configs for localnet do the following

```bash
$ cargo run --release -- --home ~/.near/localnet init
```
The above commands should initialize necessary configs and keys to run localnet in `~/.near/localnet`.

```bash
$ cargo run --release -- --home ~/.near/localnet/ run
```
After the node is started, you should see logs of every block produced in your localnet.

### testnet / betanet

To run the NEAR Indexer connected to testnet or betanet we need to have configs and keys prepopulated, you can get them with the NEAR Indexer Example like above with a little change. Follow the instructions below to run non-validating node (leaving account ID empty).

```bash
$ cargo run --release -- --home-dir ~/.near/testnet init --chain-id testnet --download
```

The above code will download the official genesis config and generate necessary configs. You can replace `testnet` in the command above to different network ID `betanet`.

**NB!** According to changes in `nearcore` config generation we don't fill all the necessary fields in the config file. While this issue is open <https://github.com/nearprotocol/nearcore/issues/3156> you need to download config you want and replace the generated one manually.
- [testnet config.json](https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore-deploy/testnet/config.json)
- [betanet config.json](https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore-deploy/betanet/config.json)
- [mainnet config.json](https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore-deploy/mainnet/config.json)

Replace `config.json` in your `--home` (e.g. `~/.near/testnet/config.json`) with downloaded one.

Configs for the specified network are in the `--home` provided folder. We need to ensure that NEAR Indexer follows all the necessary shards, so `"tracked_shards"` parameters in `~/.near/testnet/config.json` needs to be configured properly. For example, with a single shared network, you just add the shard #0 to the list:

```text
...
"tracked_shards": [0],
...
```

After that we can run NEAR Indexer.

```bash
$ cargo run --release -- --home-dir ~/.near/testnet run
```
