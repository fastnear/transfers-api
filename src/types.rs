use clickhouse::Row;
use near_primitives::types::AccountId;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Sender,
    Receiver,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "openapi", schemars(deny_unknown_fields))]
pub struct TransfersInput {
    /// NEAR account to query transfers for (the signer or receiver, depending on `direction`).
    #[cfg_attr(feature = "openapi", schemars(with = "String"))]
    pub account_id: AccountId,
    /// Opaque pagination token returned as `resume_token` on a prior page; omit for the first page.
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[cfg_attr(feature = "openapi", schemars(with = "Option<String>"))]
    pub resume_token: Option<u128>,
    /// Inclusive lower bound on block timestamp in milliseconds since the Unix epoch.
    #[cfg_attr(feature = "openapi", schemars(range(min = 0)))]
    pub from_timestamp_ms: Option<u64>,
    /// Exclusive upper bound on block timestamp in milliseconds since the Unix epoch.
    #[cfg_attr(feature = "openapi", schemars(range(min = 0)))]
    pub to_timestamp_ms: Option<u64>,
    /// Maximum number of transfer rows to return in one page (1–100).
    #[cfg_attr(feature = "openapi", schemars(range(min = 1, max = 100)))]
    pub limit: Option<usize>,
    /// When true, sort newest-first; when false or omitted, sort oldest-first.
    pub desc: Option<bool>,
    /// Minimum absolute transfer amount in the asset's base units (e.g. yoctoNEAR), stringified u128.
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[cfg_attr(feature = "openapi", schemars(with = "Option<String>"))]
    pub min_amount: Option<u128>,
    /// Minimum transfer amount in human-readable units (decimals already applied).
    pub min_human_amount: Option<f64>,
    /// Minimum transfer amount in USD-equivalent at time of transfer.
    pub min_usd_amount: Option<f64>,
    /// Asset identifier such as `native:near` for NEAR or `<contract_id>` for fungible tokens.
    pub asset_id: Option<String>,
    /// Restrict to transfers where the account acts as `sender` or `receiver`; omit for both sides.
    #[cfg_attr(
        feature = "openapi",
        schemars(schema_with = "nullable_direction_schema")
    )]
    pub direction: Option<Direction>,
    /// When true, hide system transfers (validator rewards, implicit account creation, refunds).
    pub ignore_system: Option<bool>,
}

#[serde_as]
#[derive(Debug, Clone, Row, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "openapi", schemars(deny_unknown_fields))]
pub struct TransferRow {
    #[serde_as(serialize_as = "DisplayFromStr", deserialize_as = "_")]
    #[cfg_attr(feature = "openapi", schemars(with = "String"))]
    pub block_height: u64,
    #[serde_as(serialize_as = "DisplayFromStr", deserialize_as = "_")]
    #[cfg_attr(feature = "openapi", schemars(with = "String"))]
    pub block_timestamp: u64,
    pub transaction_id: Option<String>,
    pub receipt_id: String,
    pub action_index: Option<u16>,
    pub log_index: Option<u16>,
    pub transfer_index: u32,
    pub signer_id: String,
    pub predecessor_id: String,
    pub receipt_account_id: String,
    pub account_id: String,
    pub other_account_id: Option<String>,
    pub asset_id: String,
    pub asset_type: String,
    #[serde_as(serialize_as = "DisplayFromStr", deserialize_as = "_")]
    #[cfg_attr(feature = "openapi", schemars(with = "String"))]
    pub amount: i128,
    pub method_name: Option<String>,
    pub transfer_type: String,
    pub human_amount: Option<f64>,
    pub usd_amount: Option<f64>,
    #[serde_as(serialize_as = "Option<DisplayFromStr>", deserialize_as = "_")]
    #[cfg_attr(feature = "openapi", schemars(with = "Option<String>"))]
    pub start_of_block_balance: Option<u128>,
    #[serde_as(serialize_as = "Option<DisplayFromStr>", deserialize_as = "_")]
    #[cfg_attr(feature = "openapi", schemars(with = "Option<String>"))]
    pub end_of_block_balance: Option<u128>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "openapi", schemars(deny_unknown_fields))]
pub struct TransfersResponse {
    pub transfers: Vec<TransferRow>,
    #[cfg_attr(feature = "openapi", schemars(required))]
    #[cfg_attr(
        feature = "openapi",
        schemars(schema_with = "required_nullable_string_schema")
    )]
    pub resume_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "openapi", schemars(deny_unknown_fields))]
pub struct ApiError {
    pub error: String,
    pub message: String,
}

#[cfg(feature = "openapi")]
fn nullable_direction_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
    let mut schema = <Direction as schemars::JsonSchema>::json_schema(generator);
    schema
        .ensure_object()
        .insert("nullable".into(), true.into());
    schema
}

#[cfg(feature = "openapi")]
fn required_nullable_string_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
    let mut schema = schemars::json_schema!({
        "type": "string"
    });
    schema
        .ensure_object()
        .insert("nullable".into(), true.into());
    schema
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{TransferRow, TransfersResponse};

    #[test]
    fn transfers_response_preserves_existing_wire_shape() {
        let response = TransfersResponse {
            transfers: vec![TransferRow {
                block_height: 176826522,
                block_timestamp: 1768265226000,
                transaction_id: Some("tx-hash".to_string()),
                receipt_id: "receipt-hash".to_string(),
                action_index: Some(0),
                log_index: None,
                transfer_index: 4,
                signer_id: "intents.near".to_string(),
                predecessor_id: "intents.near".to_string(),
                receipt_account_id: "wrap.near".to_string(),
                account_id: "intents.near".to_string(),
                other_account_id: Some("ref-finance.near".to_string()),
                asset_id: "native:near".to_string(),
                asset_type: "Near".to_string(),
                amount: -1_000_000_000_000_000_000_000_000,
                method_name: Some("ft_transfer_call".to_string()),
                transfer_type: "Near".to_string(),
                human_amount: Some(1.0),
                usd_amount: Some(5.24),
                start_of_block_balance: Some(4_200_000_000_000_000_000_000_000),
                end_of_block_balance: Some(3_190_000_000_000_000_000_000_000),
            }],
            resume_token: Some("7594641293647473196415950063".to_string()),
        };

        let serialized = serde_json::to_value(response).unwrap();

        assert_eq!(
            serialized,
            json!({
                "transfers": [
                    {
                        "block_height": "176826522",
                        "block_timestamp": "1768265226000",
                        "transaction_id": "tx-hash",
                        "receipt_id": "receipt-hash",
                        "action_index": 0,
                        "log_index": null,
                        "transfer_index": 4,
                        "signer_id": "intents.near",
                        "predecessor_id": "intents.near",
                        "receipt_account_id": "wrap.near",
                        "account_id": "intents.near",
                        "other_account_id": "ref-finance.near",
                        "asset_id": "native:near",
                        "asset_type": "Near",
                        "amount": "-1000000000000000000000000",
                        "method_name": "ft_transfer_call",
                        "transfer_type": "Near",
                        "human_amount": 1.0,
                        "usd_amount": 5.24,
                        "start_of_block_balance": "4200000000000000000000000",
                        "end_of_block_balance": "3190000000000000000000000"
                    }
                ],
                "resume_token": "7594641293647473196415950063"
            })
        );
    }
}
