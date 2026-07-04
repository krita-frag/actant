use std::sync::Arc;
use thiserror::Error;

/// Actant 分布式任务编排引擎的错误类型。
///
/// # 错误链保留
///
/// 包装底层错误的变体使用 `#[from]`，使 `?` 操作符保留完整错误链（可通过
/// [`std::error::Error::source`] 访问）。携带 `String` 的变体用于无类型化
/// 源错误的人类可读消息。
///
/// 显式包装错误时，优先使用 `?` 或 `From`，而非 `ActantError::Some(format!("{}", e))`，
/// 以保留错误链。
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ActantError {
    #[error("storage error: {0}")]
    Storage(String),

    #[error("storage I/O error: {0}")]
    StorageIo(#[from] std::io::Error),

    /// 包装 `heed::Error` 并保留错误链。
    #[error("storage backend error: {0}")]
    Heed(#[from] heed::Error),

    #[error("network error: {0}")]
    Network(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    /// 包装 `postcard::Error` 并保留错误链。
    #[error("postcard serialization error: {0}")]
    Postcard(#[from] postcard::Error),

    #[error("actor error: {0}")]
    Actor(String),

    #[error("workflow error: {0}")]
    Workflow(String),

    #[error("task error: {0}")]
    Task(String),

    #[error("worker error: {0}")]
    Worker(String),

    #[error("configuration error: {0}")]
    Config(String),

    /// 指标管道初始化或采集失败。
    #[error("metrics error: {0}")]
    Metrics(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("already exists: {0}")]
    AlreadyExists(String),

    #[error("timeout: {0}")]
    Timeout(String),

    #[error("cancelled: {0}")]
    Cancelled(String),

    #[error("invalid state: {0}")]
    InvalidState(String),

    #[error("internal error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, ActantError>;

impl From<Arc<std::io::Error>> for ActantError {
    fn from(e: Arc<std::io::Error>) -> Self {
        match Arc::try_unwrap(e) {
            Ok(err) => ActantError::StorageIo(err),
            Err(arc) => ActantError::StorageIo(std::io::Error::new(arc.kind(), arc.to_string())),
        }
    }
}
