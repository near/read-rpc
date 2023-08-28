# Performance test of Read RPC and Archival RPC

```bash
Read RPC (success/total)	Archival RPC (success/total)
-------------------------------------------
945 ms (25/30)		975 ms (30/30)		cold chunks
929 ms (30/30)		1433 ms (30/30)		hot chunks
-------------------------------------------
1637 ms (36/36)		3666 ms (47/47)		cold transactions
1857 ms (55/55)		8177 ms (36/42)		hot transactions
-------------------------------------------
7201 ms (36/36)		11149 ms (47/47)	cold accounts
6627 ms (55/55)		19401 ms (42/42)	hot accounts
-------------------------------------------
18477 ms (30/30)	23256 ms (30/30)	cold function calls
8269 ms (30/30)		23549 ms (30/30)	hot function calls
-------------------------------------------
```

### The goals of this repo is to:
- Know the estimate of how long does the real query execute
- See the difference between timings for "fresh" (last 5 epochs) and archival queries
- Compare Read RPC and Archival RPC
- Find the critical places in both solutions and prioritize improving them

### Things that weren't the goals:
- Make honest perf testing with millions of synthetic queries - reliability should be checked separately if needed
- Cover all the methods. I collected the stats about methods popularity and covered the most popular ones

## .env

```bash
READ_RPC_URL=https://your_read_rpc_url
NEAR_RPC_URL=http://your_near_rpc_url
QUERIES_COUNT_PER_COMMAND=30
```

Think twice while using `https://archival-rpc.mainnet.near.org` as the value for `NEAR_RPC_URL`, it's not one machine, it's a set of machines. Be sure what you are benchmarking.


`QUERIES_COUNT_PER_COMMAND` is the most interesting parameter. `30` is the default value, and it means that _around_ 30 queries will be done for each method, separately for archival (cold) and fresh (hot) part. The same queries are made simultaneously.  
If we put the value bigger than 100, it gives unbearable load for both solutions, and the numbers degrade sufficiently.

Also, check the region where you are running. If one service runs in Europe, and the other is in the US, it's better to run test twice from both regions and manually compare the results.
