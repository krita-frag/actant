//! Unit tests extracted from `src/common/config.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

use super::*;

#[test]
fn validate_accepts_empty_signing_key() {
    let config = ActantConfig {
        payload_signing_key: Vec::new(),
        ..Default::default()
    };
    assert!(
        config.validate().is_ok(),
        "empty signing key should disable signing"
    );
}

#[test]
fn validate_accepts_non_empty_signing_key() {
    let config = ActantConfig {
        payload_signing_key: b"shared-secret".to_vec(),
        ..Default::default()
    };
    assert!(config.validate().is_ok());
}

#[test]
fn failover_validate_accepts_default() {
    assert!(FailoverConfig::default().validate().is_ok());
}

#[test]
fn failover_validate_rejects_zero_heartbeat() {
    let cfg = FailoverConfig {
        heartbeat_interval_ms: 0,
        ..FailoverConfig::default()
    };
    let err = cfg.validate().unwrap_err();
    assert!(format!("{err}").contains("heartbeat_interval_ms"));
}

#[test]
fn failover_validate_rejects_failure_le_heartbeat() {
    let default = FailoverConfig::default();
    let cfg = FailoverConfig {
        failure_timeout_ms: default.heartbeat_interval_ms,
        ..default
    };
    let err = cfg.validate().unwrap_err();
    assert!(format!("{err}").contains("failure_timeout_ms"));
}

#[test]
fn failover_validate_rejects_lease_le_failure() {
    let default = FailoverConfig::default();
    let cfg = FailoverConfig {
        lease_duration_ms: default.failure_timeout_ms,
        ..default
    };
    let err = cfg.validate().unwrap_err();
    assert!(format!("{err}").contains("split-brain"));
}

#[test]
fn failover_validate_rejects_zero_check_interval() {
    let cfg = FailoverConfig {
        lease_expiry_check_interval_secs: 0,
        ..FailoverConfig::default()
    };
    let err = cfg.validate().unwrap_err();
    assert!(format!("{err}").contains("lease_expiry_check_interval_secs"));
}

// ─────────────────────────────────────────────────────────────────────────────
// 配置字段读取点门禁
//
// 判据：`ActantConfig` 序列化后出现的每个字段名，必须在生产代码里以
// `.字段名` 的形式被读取至少一次。只声明、`validate()` 通过、核心从不读取的
// 字段会让本用例变红。
//
// 历史案例：`WorkflowConfig::default_timeout_ms` 能被 Python 设置、能通过
// 校验，但核心没有任何读取点——暴露给用户的开关实际无效。
// ─────────────────────────────────────────────────────────────────────────────

/// 豁免表：每条都必须写明「为什么它没有核心读取点却是有效的」。
///
/// 这张表不是挡箭牌。新增条目等于承认该字段当前无效或需要特殊论证；
/// 新出现的无效字段不在表内，照样让门禁变红。
const READER_EXEMPT: &[(&str, &str)] = &[
    (
        "require_payload_signing",
        // RuntimeBuilder::build 在装配前调用 validate，这里是唯一且真实的消费点。
        "只在 ActantConfig::validate 中读取；validate 由 builder 在 build 前强制调用",
    ),
    (
        "default_timeout_ms",
        // 暴露给 Python（workflow_default_timeout_ms）但核心从不读：S2c 要么接线为
        // 工作流默认超时，要么连同 PyO3 暴露面一起删除。
        "TODO(S2c)：当前无生产读取点，待接线或删除",
    ),
];

/// 递归收集配置的字段路径（如 `failover.lease_duration_ms`）。
fn collect_config_paths(value: &serde_json::Value, prefix: &str, out: &mut Vec<String>) {
    if let serde_json::Value::Object(map) = value {
        for (key, child) in map {
            let path = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{prefix}.{key}")
            };
            collect_config_paths(child, &path, out);
            out.push(path);
        }
    }
}

fn collect_rs_files(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rs_files(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

/// 截断到测试模块声明之前：测试里读字段不算生产读取点。
///
/// 只认「`#[cfg(test)]` 紧跟 `mod` 或 `#[path]`」的形态。文件中间的
/// `#[cfg(test)] pub fn helper()` 属于生产代码，截断它会漏掉真实的读取点
/// （`network.rs:64` 的 `test_stub` 就曾让第 539 行的读取被误判为不存在）。
fn production_prefix(src: &str) -> &str {
    const MARKER: &str = "#[cfg(test)]";
    let mut from = 0;
    while let Some(offset) = src[from..].find(MARKER) {
        let hit = from + offset;
        let rest = src[hit + MARKER.len()..].trim_start();
        if rest.starts_with("mod ") || rest.starts_with("#[path") {
            return &src[..hit];
        }
        from = hit + MARKER.len();
    }
    src
}

/// 判断源码是否出现 `.字段名` 形式的读取。
///
/// 要求点号后紧跟字段名且其后不是标识符字符，避免 `.mode` 命中 `.model`。
fn reads_field(src: &str, field: &str) -> bool {
    let needle = format!(".{field}");
    let mut from = 0;
    while let Some(offset) = src[from..].find(&needle) {
        let hit = from + offset;
        let tail = &src[hit + needle.len()..];
        match tail.chars().next() {
            Some(c) if c.is_alphanumeric() || c == '_' => from = hit + needle.len(),
            _ => return true,
        }
    }
    false
}

#[test]
fn reads_field_requires_identifier_boundary() {
    assert!(reads_field("config.mode", "mode"));
    assert!(
        !reads_field("config.model", "mode"),
        "`.model` 不是 `.mode` 的读取"
    );
    assert!(
        !reads_field("let mode: u8 = 1;", "mode"),
        "结构体字面量赋值不是读取"
    );
    assert!(
        !reads_field("self.sync_mode", "mode"),
        "`.sync_mode` 不应算作 `.mode`"
    );
    assert!(reads_field("self.sync_mode", "sync_mode"));
}

#[test]
fn production_prefix_only_cuts_real_test_modules() {
    let src = "#[cfg(test)]\n\
               pub fn helper() {}\n\
               fn real() { c.listen_ip; }\n\
               #[cfg(test)]\n\
               mod tests;\n";
    let cut = production_prefix(src);
    assert!(
        cut.contains("real()"),
        "文件中间的 cfg(test) 辅助函数不得截断生产代码"
    );
    assert!(!cut.contains("mod tests"), "测试模块必须被截掉");

    let only_tests = "#[cfg(test)]\n#[path = \"x.rs\"]\nmod tests;\n";
    assert_eq!(production_prefix(only_tests).trim(), "");
}

#[test]
fn every_config_field_has_a_production_reader() {
    let value = serde_json::to_value(ActantConfig::default()).expect("配置应可序列化");
    let mut paths = Vec::new();
    collect_config_paths(&value, "", &mut paths);
    assert!(paths.len() > 30, "字段采集异常，仅 {} 条", paths.len());

    let mut fields: Vec<&str> = paths.iter().filter_map(|p| p.rsplit('.').next()).collect();
    fields.sort_unstable();
    fields.dedup();

    let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("定位 workspace 根");
    let mut files = Vec::new();
    for krate in ["actant-core", "actant-common"] {
        collect_rs_files(
            &workspace.join("crates").join(krate).join("src"),
            &mut files,
        );
    }
    assert!(
        files.len() > 20,
        "源码扫描范围异常，仅 {} 个文件（路径推导错了？）",
        files.len()
    );

    // 排除定义处：`config.rs` 里的结构体声明与 `Default` 实现都是赋值而非读取。
    let sources: Vec<String> = files
        .iter()
        .filter(|p| p.file_name().and_then(|n| n.to_str()) != Some("config.rs"))
        .map(|p| {
            let src = std::fs::read_to_string(p)
                .unwrap_or_else(|e| panic!("读取 {} 失败: {e}", p.display()));
            production_prefix(&src).to_owned()
        })
        .collect();

    let unread: Vec<&str> = fields
        .iter()
        .copied()
        .filter(|f| !READER_EXEMPT.iter().any(|(name, _)| name == f))
        .filter(|f| !sources.iter().any(|src| reads_field(src, f)))
        .collect();

    assert!(
        unread.is_empty(),
        "以下配置字段没有任何生产读取点（接线或删除，勿留空声明）：\n  {}",
        unread.join("\n  ")
    );
}
