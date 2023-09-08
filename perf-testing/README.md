# Performance test of Read RPC and Archival RPC

```
Read RPC (success/total)	Archival RPC (success/total)
-------------------------------------------
667 ms (20/20)		3400 ms (20/20)		cold chunks
790 ms (20/20)		3016 ms (20/20)		hot chunks
-------------------------------------------
121 ms (23/23)		1976 ms (26/26)		cold transactions
218 ms (148/148)	6251 ms (59/174)	hot transactions
-------------------------------------------
321 ms (23/23)		7158 ms (26/26)		cold accounts
520 ms (148/148)	10069 ms (174/174)	hot accounts
-------------------------------------------
2851 ms (18/18)		21062 ms (18/18)	cold function calls
2944 ms (18/18)		20794 ms (18/18)	hot function calls
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
