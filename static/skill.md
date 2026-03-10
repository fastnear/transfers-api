# Transfers API Skill

Query NEAR token transfers (native NEAR and fungible tokens) for any account.

## Endpoint

```
POST https://transfers.main.fastnear.com/v0/transfers
Content-Type: application/json
```

## Request Body

```json
{
  "account_id": "intents.near",
  "resume_token": null,
  "from_timestamp_ms": null,
  "to_timestamp_ms": null,
  "limit": 100,
  "desc": false
}
```

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `account_id` | string | yes | — | NEAR account ID to query |
| `resume_token` | string | no | null | Opaque pagination token from a previous response |
| `from_timestamp_ms` | integer | no | null | Start of time range (milliseconds, inclusive) |
| `to_timestamp_ms` | integer | no | null | End of time range (milliseconds) |
| `limit` | integer | no | 100 | Results per page (1–100) |
| `desc` | boolean | no | false | `true` for newest-first, `false` for oldest-first |

## Response

```json
{
  "transfers": [
    {
      "block_height": "136699069",
      "block_timestamp": "1736470996594518067",
      "transaction_id": "ABcd1234...",
      "receipt_id": "EFgh5678...",
      "action_index": 0,
      "log_index": null,
      "transfer_index": 42,
      "signer_id": "alice.near",
      "predecessor_id": "alice.near",
      "receipt_account_id": "wrap.near",
      "account_id": "intents.near",
      "other_account_id": "alice.near",
      "asset_id": "wrap.near",
      "asset_type": "Ft",
      "amount": "1000000000000000000000000",
      "method_name": "ft_transfer_call",
      "transfer_type": "Ft",
      "human_amount": 1.0,
      "usd_amount": 5.23,
      "start_of_block_balance": "50000000000000000000000000",
      "end_of_block_balance": "51000000000000000000000000"
    }
  ],
  "resume_token": "7594641293647473196415950063"
}
```

- `resume_token` is `null` when there are no more results.
- `amount` is negative for outgoing transfers, positive for incoming.
- `asset_type` is `"Near"` for native NEAR or `"Ft"` for fungible tokens.
- `block_height`, `block_timestamp`, `amount`, `start_of_block_balance`, and `end_of_block_balance` are serialized as strings to preserve precision.
- `transaction_id`, `action_index`, `log_index`, `other_account_id`, `method_name`, `human_amount`, `usd_amount`, `start_of_block_balance`, and `end_of_block_balance` may be `null`.

## Pagination

To paginate, pass the `resume_token` from the previous response into the next request. Continue until `resume_token` is `null`.

```json
{"account_id": "intents.near", "limit": 100, "desc": true, "resume_token": "7594641293647473196415950063"}
```

## Examples

Get the 10 most recent transfers:

```bash
curl -X POST https://transfers.main.fastnear.com/v0/transfers \
  -H "Content-Type: application/json" \
  -d '{"account_id": "intents.near", "limit": 10, "desc": true}'
```

Get transfers in a time window:

```bash
curl -X POST https://transfers.main.fastnear.com/v0/transfers \
  -H "Content-Type: application/json" \
  -d '{"account_id": "intents.near", "from_timestamp_ms": 1768265220000, "to_timestamp_ms": 1768265226000}'
```

## Error Responses

- **400 Bad Request** — invalid or missing `account_id`, bad parameter types.
  ```json
  {"error": "Bad request", "message": "..."}
  ```
- **500 Internal Server Error** — database failure.
  ```json
  "Internal server error (Clickhouse)"
  ```
