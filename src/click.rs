use crate::*;

use clickhouse::{Client, Row};
use serde::de::DeserializeOwned;
use std::env;

const CLICKHOUSE_TABLE_NAME: &str = "account_transfers";
pub const MAX_TIMESTAMP: u64 = (u32::MAX / 2) as u64 * 10u64.pow(9);

#[derive(Clone)]
pub struct ClickDB {
    pub client: Client,
}

impl ClickDB {
    pub fn new() -> Self {
        Self {
            client: establish_connection(),
        }
    }

    pub async fn max(&self, column: &str, table: &str) -> clickhouse::error::Result<BlockHeight> {
        let block_height = self
            .client
            .query(&format!("SELECT max({}) FROM {}", column, table))
            .fetch_one::<u64>()
            .await?;
        Ok(block_height)
    }

    pub async fn verify_connection(&self) -> clickhouse::error::Result<()> {
        self.client.query("SELECT 1").execute().await?;
        Ok(())
    }

    pub async fn read_rows<T>(&self, query: &str) -> clickhouse::error::Result<Vec<T>>
    where
        T: Row + DeserializeOwned,
    {
        let rows = self.client.query(query).fetch_all::<T>().await?;
        Ok(rows)
    }

    pub async fn get_transfers(
        &self,
        account_id: &AccountId,
        resume_from: Option<(u64, u32)>,
        from_timestamp_ms: Option<u64>,
        to_timestamp_ms: Option<u64>,
        limit: usize,
        desc: bool,
        min_amount: Option<u128>,
        min_human_amount: Option<f64>,
        min_usd_amount: Option<f64>,
        asset_id: Option<String>,
        direction: Option<crate::api::v0::Direction>,
        ignore_system: bool,
    ) -> clickhouse::error::Result<Vec<TransferRow>> {
        let from_timestamp = from_timestamp_ms
            .map(|v| v.saturating_mul(1_000_000))
            .unwrap_or(0)
            .min(MAX_TIMESTAMP) as f64
            / 1e9;
        let to_timestamp = to_timestamp_ms
            .map(|v| v.saturating_mul(1_000_000))
            .unwrap_or(MAX_TIMESTAMP)
            .min(MAX_TIMESTAMP) as f64
            / 1e9;
        let resume_clause = if let Some((timestamp_ns, transfer_index)) = resume_from {
            let timestamp_ns = (timestamp_ns as f64) / 1e9;
            if desc {
                format!(
                    "AND (block_timestamp < {} OR (block_timestamp = {} AND transfer_index < {}))",
                    timestamp_ns, timestamp_ns, transfer_index
                )
            } else {
                format!(
                    "AND (block_timestamp > {} OR (block_timestamp = {} AND transfer_index > {}))",
                    timestamp_ns, timestamp_ns, transfer_index
                )
            }
        } else {
            "".to_string()
        };
        let min_amount_clause = match min_amount {
            Some(v) => format!("AND abs(amount) >= {v}"),
            None => "".to_string(),
        };
        let min_human_amount_clause = match min_human_amount {
            Some(v) => format!("AND abs(human_amount) >= {v}"),
            None => "".to_string(),
        };
        let min_usd_amount_clause = match min_usd_amount {
            Some(v) => format!("AND abs(usd_amount) >= {v}"),
            None => "".to_string(),
        };
        let asset_id_clause = if asset_id.is_some() {
            "AND asset_id = ?".to_string()
        } else {
            "".to_string()
        };
        let direction_clause = match direction {
            Some(crate::api::v0::Direction::Sender) => "AND amount < 0".to_string(),
            Some(crate::api::v0::Direction::Receiver) => "AND amount > 0".to_string(),
            None => "".to_string(),
        };
        let ignore_system_clause = if ignore_system {
            "AND other_account_id != 'system'".to_string()
        } else {
            "".to_string()
        };
        let order = if desc { "DESC" } else { "ASC" };
        let query = format!(
            r#"
                SELECT
                    *
                FROM
                    {CLICKHOUSE_TABLE_NAME}
                WHERE
                    account_id = ?
                    AND block_timestamp >= ?
                    AND block_timestamp < ?
                    {resume_clause}
                    {min_amount_clause}
                    {min_human_amount_clause}
                    {min_usd_amount_clause}
                    {asset_id_clause}
                    {direction_clause}
                    {ignore_system_clause}
                ORDER BY
                    block_timestamp {order},
                    transfer_index {order}
                LIMIT {limit}
            "#
        );
        let mut query_builder = self.client.query(&query);
        query_builder = query_builder
            .bind(&account_id)
            .bind(from_timestamp)
            .bind(to_timestamp);
        if let Some(ref asset_id) = asset_id {
            query_builder = query_builder.bind(asset_id);
        }
        query_builder.fetch_all().await
    }
}

fn establish_connection() -> Client {
    Client::default()
        .with_url(env::var("DATABASE_URL").unwrap())
        .with_user(env::var("DATABASE_USER").unwrap())
        .with_password(env::var("DATABASE_PASSWORD").unwrap())
        .with_database(env::var("DATABASE_DATABASE").unwrap())
}
