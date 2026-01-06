use clickhouse::Row;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};

#[serde_as]
#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct TransferRow {
    #[serde_as(serialize_as = "DisplayFromStr", deserialize_as = "_")]
    pub block_height: u64,
    #[serde_as(serialize_as = "DisplayFromStr", deserialize_as = "_")]
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
    pub amount: i128,
    pub method_name: Option<String>,
    pub transfer_type: String,
    pub human_amount: Option<f64>,
    pub usd_amount: Option<f64>,
    #[serde_as(serialize_as = "Option<DisplayFromStr>", deserialize_as = "_")]
    pub start_of_block_balance: Option<u128>,
    #[serde_as(serialize_as = "Option<DisplayFromStr>", deserialize_as = "_")]
    pub end_of_block_balance: Option<u128>,
}
