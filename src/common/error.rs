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

impl ActantError {
    /// 返回该错误的稳定 kind 字符串（snake_case）。
    ///
    /// kind 映射以 `ActorErrorEnvelope::from(&ActantError)` 为单一真相来源
    /// （编译器强制穷尽），与 Python `actant.exceptions` 的 kind 表对齐。
    /// 用于 emit 聚合错误等需要跨语言保真 kind 的场景：调用方将 kind 编码进
    /// 错误消息前缀（见 [`format_error_kind`]），Python 侧
    /// `decode_error_kind` 解析前缀重建对应异常子类。
    pub fn kind_str(&self) -> &'static str {
        crate::common::model::ActorErrorEnvelope::from(self)
            .kind
            .as_str()
    }
}

impl From<Arc<std::io::Error>> for ActantError {
    fn from(e: Arc<std::io::Error>) -> Self {
        match Arc::try_unwrap(e) {
            Ok(err) => ActantError::StorageIo(err),
            Err(arc) => ActantError::StorageIo(std::io::Error::new(arc.kind(), arc.to_string())),
        }
    }
}

/// 跨语言错误类型保留协议：在 error 字符串前缀编码 kind。
///
/// Python 侧 ``actant.exceptions.decode_error_kind`` 解析此前缀重建对应
/// Python 异常子类（如 ``timeout`` → ``ActantTimeoutError``）。
///
/// 格式：``[actant:KIND] message``
///
/// # 示例
///
/// ```
/// # use actant::common::format_error_kind;
/// assert_eq!(
///     format_error_kind("timeout", "task timed out after 5000ms"),
///     "[actant:timeout] task timed out after 5000ms",
/// );
/// ```
pub fn format_error_kind(kind: &str, message: &str) -> String {
    format!("[actant:{kind}] {message}")
}

#[cfg(test)]
#[path = "../../tests/rust/unit/common/error.rs"]
mod tests;
