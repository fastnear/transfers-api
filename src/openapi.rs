use anyhow::Result;
use fastnear_openapi_generator::{
    build_service_doc, write_or_check_yaml, AggregateOperationSpec, ApiInfo, ApiServer,
    HttpMethod, NamedExample, RequestBodySpec, ResponseContent, ResponseSpec, SchemaRegistry,
};
use serde_json::json;

use crate::types::{ApiError, TransfersInput, TransfersResponse};

const API_VERSION: &str = "3.0.3";
const SERVICE_INFO: ApiInfo<'static> = ApiInfo {
    title: "Transfers API",
    version: API_VERSION,
    description: "Account-centric transfer queries for native NEAR and fungible tokens.",
    servers: &[ApiServer {
        url: "https://transfers.main.fastnear.com",
        description: "Mainnet",
    }],
};

pub fn generate(check: bool) -> Result<()> {
    let output_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("openapi");

    let mut registry = SchemaRegistry::openapi3();
    let transfers_input = registry.schema_ref::<TransfersInput>();
    let transfers_response = registry.schema_ref::<TransfersResponse>();
    let api_error = registry.schema_ref::<ApiError>();
    let components = registry.into_components();

    let service_doc = build_service_doc(
        &SERVICE_INFO,
        vec![AggregateOperationSpec {
            slug: "transfers",
            title: "Transfers API - Query Transfers",
            path: "/v0/transfers",
            method: HttpMethod::Post,
            operation_id: "get_transfers_by_account",
            summary: "Query transfers for an account",
            description: "Fetch transfer rows for one account with optional direction, asset, amount, and time filters.",
            tags: &["transfers"],
            parameters: vec![],
            request_body: Some(RequestBodySpec::Json {
                schema: transfers_input,
                required: true,
                example: None,
                examples: vec![NamedExample {
                    name: "recent_near_transfers",
                    summary: Some("Recent incoming NEAR transfers"),
                    value: json!({
                        "account_id": "intents.near",
                        "asset_id": "near",
                        "direction": "receiver",
                        "min_amount": "1000000000000000000000000",
                        "limit": 10,
                        "desc": true
                    }),
                }],
            }),
            responses: vec![
                ResponseSpec {
                    status: "200",
                    description: "Transfer rows for the requested account",
                    content: Some(ResponseContent::Json {
                        schema: transfers_response,
                        example: Some(json!({
                            "transfers": [
                                {
                                    "block_height": "176826522",
                                    "block_timestamp": "1768265226000",
                                    "transaction_id": "9Qz1h7qTjYb6w3VpsL4Qu9Q2qY7Lf8S3Hy4J6WZ7fUPJ",
                                    "receipt_id": "2T8mJ5vL8PkYc3Rm6bNZBvA7k8mHh3oJmM4t9g8vP3Qc",
                                    "action_index": 0,
                                    "log_index": null,
                                    "transfer_index": 4,
                                    "signer_id": "intents.near",
                                    "predecessor_id": "intents.near",
                                    "receipt_account_id": "wrap.near",
                                    "account_id": "intents.near",
                                    "other_account_id": "ref-finance.near",
                                    "asset_id": "near",
                                    "asset_type": "Near",
                                    "amount": "-1000000000000000000000000",
                                    "method_name": "ft_transfer_call",
                                    "transfer_type": "Near",
                                    "human_amount": 1,
                                    "usd_amount": 5.24,
                                    "start_of_block_balance": "4200000000000000000000000",
                                    "end_of_block_balance": "3190000000000000000000000"
                                }
                            ],
                            "resume_token": "7594641293647473196415950063"
                        })),
                        examples: vec![],
                    }),
                },
                ResponseSpec {
                    status: "400",
                    description: "Invalid request body",
                    content: Some(ResponseContent::Json {
                        schema: api_error,
                        example: None,
                        examples: vec![],
                    }),
                },
                ResponseSpec {
                    status: "500",
                    description: "ClickHouse query failure",
                    content: Some(ResponseContent::Json {
                        schema: json!({"type": "string"}),
                        example: None,
                        examples: vec![],
                    }),
                },
            ],
        }],
        components,
    );

    write_or_check_yaml(output_root.join("openapi.yaml"), &service_doc, check)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use fastnear_openapi_generator::SchemaRegistry;

    use crate::types::{TransfersInput, TransfersResponse};

    #[test]
    fn transfers_input_keeps_stringified_token_fields_in_schema() {
        let mut registry = SchemaRegistry::openapi3();
        registry.schema_ref::<TransfersInput>();
        let components = registry.into_components();
        let input = &components["TransfersInput"]["properties"];

        assert_eq!(input["resume_token"]["type"], "string");
        assert_eq!(input["resume_token"]["nullable"], true);
        assert_eq!(input["min_amount"]["type"], "string");
        assert_eq!(input["min_amount"]["nullable"], true);
        assert_eq!(input["account_id"]["type"], "string");
    }

    #[test]
    fn transfer_row_keeps_stringified_amount_fields_in_schema() {
        let mut registry = SchemaRegistry::openapi3();
        registry.schema_ref::<TransfersResponse>();
        let components = registry.into_components();
        let row = &components["TransferRow"]["properties"];

        assert_eq!(row["block_height"]["type"], "string");
        assert_eq!(row["block_timestamp"]["type"], "string");
        assert_eq!(row["amount"]["type"], "string");
        assert_eq!(row["start_of_block_balance"]["type"], "string");
        assert_eq!(row["start_of_block_balance"]["nullable"], true);
        assert_eq!(row["end_of_block_balance"]["type"], "string");
        assert_eq!(row["end_of_block_balance"]["nullable"], true);
    }

    #[test]
    fn direction_schema_preserves_wire_enum_values() {
        let mut registry = SchemaRegistry::openapi3();
        registry.schema_ref::<TransfersInput>();
        let components = registry.into_components();
        let direction = &components["TransfersInput"]["properties"]["direction"];

        assert_eq!(direction["type"], "string");
        assert_eq!(direction["nullable"], true);
        assert_eq!(direction["enum"][0], "sender");
        assert_eq!(direction["enum"][1], "receiver");
    }

    #[test]
    fn transfers_response_keeps_resume_token_required_and_nullable() {
        let mut registry = SchemaRegistry::openapi3();
        registry.schema_ref::<TransfersResponse>();
        let components = registry.into_components();
        let response = &components["TransfersResponse"];

        assert!(response["required"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value == "resume_token"));
        assert_eq!(response["properties"]["resume_token"]["type"], "string");
        assert_eq!(response["properties"]["resume_token"]["nullable"], true);
    }
}
