# view_state_paginated

The `view_state_paginated` method is a custom method that allows you to view the state of a contract in a paginated way.

## How to use it

First request send without `next_page_token` to get the first page of the state. Then, if the response contains `next_page_token`, you can send the next request with the `next_page_token` to get the next page of the state.

### Example:

Request:

```json
{
  "jsonrpc": "2.0",
  "id": "dontcare",
  "method": "view_state_paginated",
  "params": {
    "block_id": 112457314,
    "account_id": "social.near"
  }
}
```
Response:

```json
{
  "jsonrpc": "2.0",
  "result": {
    "values": [
      {
        "key": "some_key1",
        "value": "some_value2"
      },
      {
        "key": "some_key2",
        "value": "some_value2"
      },
      ...
    ],
    "block_height": 112457314,
    "block_hash": "3hALqxyuTEmMsCx7rAjqXw9Fu8TKYosbmhx6uTEXM2dh",
    "next_page_token": "some_long_next_page_token_string_cd0eddf8acdff8b7010000fffff00002"
  },
  "id": "dontcare"
}
```
Next page request:

```json
{
  "jsonrpc": "2.0",
  "id": "dontcare",
  "method": "view_state_paginated",
  "params": {
    "block_id": 112457314,
    "account_id": "social.near",
    "next_page_token": "some_long_next_page_token_string_cd0eddf8acdff8b7010000fffff00002"
  }
}
```
Next page response:

```json
{
  "jsonrpc": "2.0",
  "result": {
    "values": [
      {
        "key": "some_key3",
        "value": "some_value3"
      },
      {
        "key": "some_key4",
        "value": "some_value4"
      },
      ...
    ],
    "block_height": 112457314,
    "block_hash": "3hALqxyuTEmMsCx7rAjqXw9Fu8TKYosbmhx6uTEXM2dh",
    "next_page_token": "another_some_long_next_page_token_string_aa0bc7f347342560101010051432cc1"
  },
  "id": "dontcare"
}
```
In the last page response `next_page_token` field will be `null`.

# view_receipt_record

The `view_receipt_record` method is a custom method that allows you to view the record of the receipt by its ID.

## How to use it
### Example

Request:
```json
{
  "jsonrpc": "2.0",
  "id": "dontcare",
  "method": "view_receipt_record",
  "params": {
    "receipt_id": "6aB1XxfnhuQ83FWHb5xyqssGnaD5CUQgxHpbAVJFRrPe"
  }
}
```
Response:
```json
{
  "id": "dontcare",
  "jsonrpc": "2.0",
  "result": {
    "block_height": 118875440,
    "parent_transaction_hash": "6iJgcM5iZrWuhG4ZpUyX6ivtMQUho2S1JRdBYdY7Y7vX",
    "receipt_id": "6aB1XxfnhuQ83FWHb5xyqssGnaD5CUQgxHpbAVJFRrPe",
    "shard_id": 0
  }
}
```
