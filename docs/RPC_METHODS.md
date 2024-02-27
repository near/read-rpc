| **Method**                      | **status** | **Note**                                                                    |
|---------------------------------|------------|-----------------------------------------------------------------------------|
| block                           | Included   |                                                                             |
| chunk                           | Included   |                                                                             |
| query.view_account              | Included   |                                                                             |
| query.view_state                | Included   |                                                                             |
| view_state_paginated            | Included   | Custom method. See details [here](../docs/CUSTOM_RPC_METHODS.md)            |
| query.view_code                 | Included   |                                                                             |
| query.view_access_key           | Included   |                                                                             |
| query.call_function             | Included   |                                                                             |
| query.view_access_key_list      | Proxy      | Planned. It will be implemented in the future.                              |
| gas_price                       | Included   |                                                                             |
| tx                              | Included   |                                                                             |
| EXPERIMENTAL_tx_status          | Included   |                                                                             |
| broadcast_tx_commit             | Proxy      | PROXY_ONLY. Immediately proxy to a real RPC.                                |
| broadcast_tx_async              | Proxy      | PROXY_ONLY. Immediately proxy to a real RPC.                                |
| EXPETIMENTAL_receipt            | Included   |                                                                             |
| EXPERIMENTAL_changes            | Included   |                                                                             |
| EXPERIMENTAL_changes_in_block   | Included   |                                                                             |
| network_info                    | Proxy      | PROXY_ONLY. Immediately proxy to a real RPC.                                |
| status                          | Included   |                                                                             |
| health                          | Included   | Health includes the info about the syncing state of the node of rpc-server. |
| light_client_proof              | Proxy      |                                                                             |
| next_light_client_block         | Proxy      |                                                                             |
| validators                      | Included   |                                                                             |
| EXPERIMENTAL_validators_ordered | Proxy      |                                                                             |
| EXPERIMENTAL_genesis_config     | Included   | Cache it on the start.                                                      |
| EXPERIMENTAL_protocol_config    | Included   |                                                                             |