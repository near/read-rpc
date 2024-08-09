# logic-state-indexer

A library that contains all the business logic for the state-indexer. In the ReadRPC project we have two types of the `state-indexer`:
* `state-indexer` based on the NEAR Lake data (easier to maintain, but with a slightly bigger delay). **Doesn't provide the optimistic data**
* `near-state-indexer` based on the `nearcore` (harder to maintain, but with a smaller delay). **Provides the optimistic data**

Both of them does the same thing with some differences for `near-state-indexer` that can provide the optimistic data.

This library aims to reduce the copy-paste in our code-base and introduce a single point where we can change the logic for the state-indexer if necessary.