# state-indexer

The indexer built on top of Lake Framework that watches the network and stores the `StateChanges` into the Storage (ScyllaDB) using the designed data schemas.

## Create `.env` file in the project root

```
INDEXER_ID=state-indexer-1
SCYLLA_URL=127.0.0.1:9042
SCYLLA_USER=cassandra
SCYLLA_PASSWORD=cassandra
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
AWS_DEFAULT_REGION=eu-central-1
```

## Set up local ScyllaDb

```
$ docker run --name some-scylla -p 9042:9042 --hostname some-scylla -d scylladb/scylla --smp 1
```

You can find the schema definition in the `src/configs.rs` file. There is a `migrate` function that is being called on every start. It will create necessary tables if they don't exist.
For state-indexer we are using keyspace `state_indexer` by default.
### cqlsh

In order to connect to the ScyllaDB cluster and run some queries directly you can use `cqlsh` like so:

```
docker exec -it some-scylla cqlsh

use state_indexer;
```

## Build state-indexer and run

```
$ env RUST_LOG="state_indexer=debug" cargo run --release -- testnet from-interruption
```
