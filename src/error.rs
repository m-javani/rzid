use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RzError {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, RzError>;

impl IntoResponse for RzError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            RzError::Config(_) => (StatusCode::BAD_REQUEST, self.to_string()),
            RzError::Io(_) => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
            RzError::Json(_) => (StatusCode::BAD_REQUEST, self.to_string()),
        };

        let body = Json(json!({
            "error": message,
        }));

        (status, body).into_response()
    }
}
