use crate::*;
use std::fmt;

use actix_web::{post, ResponseError};
use actix_web::{web, HttpRequest};
use serde::Deserialize;

use serde_json::json;

const TARGET_API: &str = "api";
const MAX_TRANSFERS_LIMIT: usize = 100;

#[allow(unused)]
#[derive(Debug)]
enum ServiceError {
    ClickhouseError(clickhouse::error::Error),
    ArgumentError(String),
}

impl From<clickhouse::error::Error> for ServiceError {
    fn from(error: clickhouse::error::Error) -> Self {
        ServiceError::ClickhouseError(error)
    }
}

impl fmt::Display for ServiceError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ServiceError::ClickhouseError(ref err) => write!(f, "Database Error: {:?}", err),
            ServiceError::ArgumentError(ref err) => write!(f, "Argument Error: {}", err),
        }
    }
}

impl ResponseError for ServiceError {
    fn error_response(&self) -> HttpResponse {
        match *self {
            ServiceError::ClickhouseError(ref err) => {
                tracing::error!(target: TARGET_API, "Clickhouse error: {:?}", err);
                HttpResponse::InternalServerError().json("Internal server error (Clickhouse)")
            }
            ServiceError::ArgumentError(ref err) => {
                tracing::error!(target: TARGET_API, "Argument error: {}", err);
                HttpResponse::BadRequest().json(json!({
                    "error": "Bad request",
                    "message": err,
                }))
            }
        }
    }
}

pub mod v0 {
    use super::*;
    use crate::click::MAX_TIMESTAMP;
    use serde_with::{serde_as, DisplayFromStr};

    #[serde_as]
    #[derive(Debug, Deserialize)]
    pub struct TransfersInput {
        pub account_id: AccountId,
        #[serde_as(as = "Option<DisplayFromStr>")]
        pub resume_token: Option<u128>,
        pub from_timestamp_ms: Option<u64>,
        pub to_timestamp_ms: Option<u64>,
        pub limit: Option<usize>,
        pub desc: Option<bool>,
    }

    #[post("/transfers")]
    pub async fn get_transfers_by_account(
        _request: HttpRequest,
        input: web::Json<TransfersInput>,
        app_state: web::Data<AppState>,
    ) -> Result<impl Responder, ServiceError> {
        let input: TransfersInput = input.into_inner();
        let limit = input
            .limit
            .unwrap_or(MAX_TRANSFERS_LIMIT)
            .min(MAX_TRANSFERS_LIMIT)
            .max(1);
        let desc = input.desc.unwrap_or(false);
        let resume_from = input.resume_token.map(|v| {
            let transfer_index = (v & 0xFFFFFFFF) as u32;
            let timestamp_ms = (v >> 32).min(MAX_TIMESTAMP as _) as u64;
            (timestamp_ms, transfer_index)
        });

        let transfers = app_state
            .click_db
            .get_transfers(
                &input.account_id,
                resume_from,
                input.from_timestamp_ms,
                input.to_timestamp_ms,
                limit,
                desc,
            )
            .await?;
        let resume_token = if transfers.len() == limit {
            let last_transfer = transfers.last().unwrap();
            let time_ns = last_transfer.block_timestamp as u128;
            let token = time_ns << 32 | (last_transfer.transfer_index as u128);
            Some(token.to_string())
        } else {
            None
        };

        tracing::info!(
            target: TARGET_API,
            "Fetched {} transfers for account {}",
            transfers.len(),
            input.account_id
        );

        Ok(web::Json(json!({
            "transfers": transfers,
            "resume_token": resume_token,
        })))
    }
}
