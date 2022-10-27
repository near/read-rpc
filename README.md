# json-rpc-100x

### Compile

```bash
$ cargo build --release
```

### Run

#### Logging
To run an instance of Jaeger, you can use Docker:
```bash
$ docker run -d -p6831:6831/udp -p6832:6832/udp -p16686:16686 -p14268:14268 jaegertracing/all-in-one:latest
```
You can visit the Jaeger page by going to http://localhost:16686

#### Run RPC service 
```bash
$ ./target/release/json-rpc-100x
```

* mainnet https://rpc.mainnet.near.org
* testnet https://rpc.testnet.near.org
* betanet https://rpc.betanet.near.org (may be unstable)
* localnet http://localhost:3030

### NEAR RPC API
```asm
https://docs.near.org/api/rpc/introduction
```
