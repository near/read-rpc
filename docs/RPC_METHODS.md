| **Method**                        | **status**    | **Note**                                                                    |
|-----------------------------------|---------------|-----------------------------------------------------------------------------|
| view_state_paginated              | Included      | Custom method. See details [here](../docs/CUSTOM_RPC_METHODS.md)            |
| view_receipt_record               | Included      | Custom method. See details [here](../docs/CUSTOM_RPC_METHODS.md)            |
| query.view_account                | Included      |                                                                             |
| query.view_code                   | Included      |                                                                             |
| query.view_state                  | Included      |                                                                             |
| query.call_function               | Included      |                                                                             |
| query.view_access_key             | Included      |                                                                             |
| query.view_access_key_list        | Proxy         | Planned. It will be implemented in the future.                              |
| block                             | Included      |                                                                             |
| broadcast_tx_async                | Proxy         | PROXY_ONLY. Immediately proxy to a real RPC.                                |
| broadcast_tx_commit               | Proxy         | PROXY_ONLY. Immediately proxy to a real RPC.                                |
| chunk                             | Included      |                                                                             |
| gas_price                         | Included      |                                                                             |
| health                            | Included      | Health includes the info about the syncing state of the node of rpc-server. |
| light_client_proof                | Proxy         |                                                                             |
| next_light_client_block           | Proxy         |                                                                             |
| network_info                      | Proxy         | PROXY_ONLY. Immediately proxy to a real RPC.                                |
| status                            | Included      |                                                                             |
| send_tx                           | Proxy         | PROXY_ONLY. Immediately proxy to a real RPC.                                |
| tx                                | Included      |                                                                             |
| validators                        | Included      |                                                                             |
| client_config                     | Unimplemented |                                                                             |
| EXPERIMENTAL_changes              | Included      |                                                                             |
| EXPERIMENTAL_changes_in_block     | Included      |                                                                             |
| EXPERIMENTAL_genesis_config       | Included      | Cache it on the start.                                                      |
| EXPERIMENTAL_light_client_proof   | Proxy         |                                                                             |
| EXPERIMENTAL_protocol_config      | Included      |                                                                             |
| EXPETIMENTAL_receipt              | Included      |                                                                             |
| EXPERIMENTAL_tx_status            | Included      |                                                                             |
| EXPERIMENTAL_validators_ordered   | Proxy         |                                                                             |
| EXPERIMENTAL_maintenance_windows  | Unimplemented |                                                                             |
| EXPERIMENTAL_split_storage_info   | Unimplemented |                                                                             |
