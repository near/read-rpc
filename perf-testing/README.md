# Performance test of Read RPC and Archival RPC

```bash
Read RPC (success/total)	Archival RPC (success/total)
-------------------------------------------
1172 ms (14/20)		1105 ms (20/20)		cold chunks
1183 ms (20/20)		1442 ms (20/20)		hot chunks
-------------------------------------------
6586 ms (11/11)		3457 ms (31/31)		cold transactions
4069 ms (29/29)		6644 ms (31/31)		hot transactions
-------------------------------------------
3575 ms (11/11)		10459 ms (31/31)	cold accounts
1887 ms (29/29)		11663 ms (31/31)	hot accounts
-------------------------------------------
11640 ms (18/18)	18407 ms (18/18)	cold function calls
1825 ms (18/18)		11758 ms (18/18)	hot function calls
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
If we put there value bigger than 100, it gives unbearable load for both solutions, and the numbers degrade sufficiently.