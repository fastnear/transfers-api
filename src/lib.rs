pub mod api;
pub mod cache;
pub mod click;
#[cfg(feature = "openapi")]
pub mod openapi;
pub mod types;

use actix_web::{HttpResponse, Responder};

use crate::click::ClickDB;

#[derive(Clone)]
pub struct AppState {
    pub click_db: ClickDB,
}

pub async fn greet() -> impl Responder {
    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(include_str!("../static/index.html"))
}

pub async fn skill() -> impl Responder {
    HttpResponse::Ok()
        .content_type("text/markdown; charset=utf-8")
        .body(include_str!("../static/skill.md"))
}
