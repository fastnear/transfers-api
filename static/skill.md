# Transfers API Skill

Query NEAR token transfers for an account. The public endpoint is currently mainnet-only.

## Endpoint

```
POST https://transfers.main.fastnear.com/v0/transfers
Content-Type: application/json
```

## Request Body

```json
{
  "account_id": "root.near",
  "resume_token": null,
  "from_timestamp_ms": null,
  "to_timestamp_ms": null,
  "limit": 100,
  "desc": false,
  "min_amount": null,
  "min_human_amount": null,
  "min_usd_amount": null,
  "asset_id": null,
  "direction": null,
  "ignore_system": false
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
| `min_amount` | string | no | null | Minimum absolute amount in token units |
| `min_human_amount` | number | no | null | Minimum absolute amount after applying token decimals |
| `min_usd_amount` | number | no | null | Minimum absolute USD value |
| `asset_id` | string | no | null | Filter by asset ID |
| `direction` | string | no | null | `"sender"` for outgoing, `"receiver"` for incoming |
| `ignore_system` | boolean | no | false | Skip transfers where `other_account_id` is `"system"` |

## Response

```json
{
  "resume_token": "7628255855001253730542972112",
  "transfers": [
    {
      "account_id": "root.near",
      "action_index": null,
      "amount": "5000000",
      "asset_id": "nep245:intents.near:nep141:aleo-usdcx.omft.near",
      "asset_type": "Mt",
      "block_height": "193835648",
      "block_timestamp": "1776091720676341497",
      "end_of_block_balance": "5000000",
      "human_amount": 5.0,
      "log_index": 0,
      "method_name": "ft_on_transfer",
      "other_account_id": null,
      "predecessor_id": "aleo-usdcx.omft.near",
      "receipt_account_id": "intents.near",
      "receipt_id": "6eoQ8fdNzRAJLzzrwuVvWMpYajfZNpm8usnkKd77wb2L",
      "signer_id": "bridge-mng.near",
      "start_of_block_balance": "0",
      "transaction_id": "8qN5FKndxkKhc8VwF1QoJT3eawUokZaWJKzVpfoXzr9L",
      "transfer_index": 290000,
      "transfer_type": "MtTransfer",
      "usd_amount": 4.998825
    }
  ]
}
```

- `resume_token` is `null` when there are no more results.
- `amount` is negative for outgoing transfers and positive for incoming transfers.
- `block_height`, `block_timestamp`, `amount`, `start_of_block_balance`, and `end_of_block_balance` are serialized as strings to preserve precision.
- `transaction_id`, `action_index`, `log_index`, `other_account_id`, `method_name`, `human_amount`, `usd_amount`, `start_of_block_balance`, and `end_of_block_balance` may be `null`.

## Pagination

To paginate, pass the `resume_token` from the previous response into the next request. Continue until `resume_token` is `null`.

```json
{"account_id": "root.near", "limit": 10, "desc": true, "resume_token": "7628255855001253730542972112"}
```

## Examples

Get the 10 most recent transfers:

```bash
curl -X POST https://transfers.main.fastnear.com/v0/transfers \
  -H "Content-Type: application/json" \
  -d '{"account_id": "root.near", "limit": 10, "desc": true}'
```

Get transfers in a time window:

```bash
curl -X POST https://transfers.main.fastnear.com/v0/transfers \
  -H "Content-Type: application/json" \
  -d '{"account_id": "root.near", "from_timestamp_ms": 1776091720000, "to_timestamp_ms": 1776091730000}'
```

Filter by asset, direction, and minimum amount:

```bash
curl -X POST https://transfers.main.fastnear.com/v0/transfers \
  -H "Content-Type: application/json" \
  -d '{"account_id": "root.near", "asset_id": "nep245:intents.near:nep141:aleo-usdcx.omft.near", "direction": "receiver", "min_amount": "5000000", "limit": 10, "desc": true}'
```

Filter by USD value, ignoring system transfers:

```bash
curl -X POST https://transfers.main.fastnear.com/v0/transfers \
  -H "Content-Type: application/json" \
  -d '{"account_id": "root.near", "min_usd_amount": 1, "ignore_system": true, "desc": true}'
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
