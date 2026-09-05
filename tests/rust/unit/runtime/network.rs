//! Unit tests extracted from `src/runtime/network.rs`.
//! Compiled via `#[path]` attribute — retains `super::` access to private items.

use super::*;

#[test]
fn test_peer_allowed_empty() {
    let allowed = build_allowed_peer_ids(&[]);
    assert!(peer_allowed(&allowed, "any"));
}

#[test]
fn test_peer_allowed_specific() {
    let allowed = build_allowed_peer_ids(&["abc".to_string()]);
    assert!(peer_allowed(&allowed, "abc"));
    assert!(!peer_allowed(&allowed, "def"));
}

#[test]
fn test_build_allowed_peer_ids_trims_and_filters_empty() {
    let allowed = build_allowed_peer_ids(&[
        "  peer-a  ".to_string(),
        "".to_string(),
        "peer-b".to_string(),
        "   ".to_string(),
    ]);
    assert_eq!(allowed.len(), 2);
    assert!(allowed.contains("peer-a"));
    assert!(allowed.contains("peer-b"));
}

#[test]
fn test_discovery_from_name_known_modes() {
    let cfg = NetworkConfig::default();
    assert!(discovery_from_name(discovery_mode::NONE, &cfg).is_ok());
    assert!(discovery_from_name(discovery_mode::LOCAL, &cfg).is_ok());
    assert!(discovery_from_name(discovery_mode::MDNS, &cfg).is_ok());
    assert!(discovery_from_name(discovery_mode::DNS, &cfg).is_ok());
    assert!(discovery_from_name(discovery_mode::RELAY, &cfg).is_ok());
}

#[test]
fn test_discovery_from_name_unknown_mode() {
    let cfg = NetworkConfig::default();
    let err = discovery_from_name("unknown-mode", &cfg).unwrap_err();
    assert!(matches!(err, ActantError::Config(_)));
    let msg = err.to_string();
    assert!(msg.contains("unknown discovery mode"));
}

#[test]
fn test_parse_endpoint_addr_empty() {
    let err = parse_endpoint_addr("").unwrap_err();
    assert!(matches!(err, ActantError::Config(_)));
}

#[test]
fn test_parse_endpoint_addr_invalid_format() {
    let err = parse_endpoint_addr("not-an-addr").unwrap_err();
    assert!(matches!(err, ActantError::Config(_)));
}

#[test]
fn test_parse_endpoint_addr_shorthand_invalid_node_id() {
    let err = parse_endpoint_addr("bad@127.0.0.1:1").unwrap_err();
    assert!(matches!(err, ActantError::Config(_)));
}

#[test]
fn test_parse_endpoint_addr_shorthand_invalid_socket() {
    let err =
        parse_endpoint_addr("0000000000000000000000000000000000000000000000000000000000000000@bad")
            .unwrap_err();
    assert!(matches!(err, ActantError::Config(_)));
}

#[tokio::test]
async fn test_send_direct_request_returns_timeout_for_unreachable_peer() {
    let config = NetworkConfig {
        discovery_mode: crate::common::DiscoveryMode::parse(discovery_mode::NONE).unwrap(),
        listen_ip: "127.0.0.1".into(),
        listen_port: 0,
        direct_request_timeout_ms: 100,
        ..NetworkConfig::default()
    };

    let manager = NetworkManager::new(NodeId::from("test-node"), config)
        .await
        .expect("create manager");

    let unreachable_addr =
        "0000000000000000000000000000000000000000000000000000000000000000@127.0.0.1:1";
    let request = DirectRequest::QueryWorkflowState {
        workflow_id: WorkflowId::from("wf-1"),
        requesting_node: NodeId::from("test-node"),
    };

    let result = manager.send_direct_request(unreachable_addr, request).await;
    match result {
        Err(ActantError::Timeout(_)) => { /* expected */ }
        Err(ActantError::Network(msg)) => {
            tracing::debug!("got Network error (acceptable): {}", msg);
        }
        other => panic!("expected Timeout or Network error, got {:?}", other),
    }
}

#[tokio::test]
async fn test_network_manager_listen_addresses_roundtrip() {
    let config = NetworkConfig {
        discovery_mode: crate::common::DiscoveryMode::parse(discovery_mode::NONE).unwrap(),
        listen_ip: "127.0.0.1".into(),
        listen_port: 0,
        direct_request_timeout_ms: 100,
        ..NetworkConfig::default()
    };

    let manager = NetworkManager::new(NodeId::from("test-node"), config)
        .await
        .expect("create manager");

    let addrs = manager.listen_addresses().expect("listen addresses");
    assert!(!addrs.endpoint_addr.is_empty());
    assert!(!addrs.endpoint_id.is_empty());

    // endpoint_addr 应能被 parse_endpoint_addr 解析。
    let parsed = parse_endpoint_addr(&addrs.endpoint_addr);
    assert!(
        parsed.is_ok(),
        "endpoint_addr should be parseable: {}",
        addrs.endpoint_addr
    );
}

#[tokio::test]
async fn test_network_manager_node_id_and_peer_id() {
    let config = NetworkConfig {
        discovery_mode: crate::common::DiscoveryMode::parse(discovery_mode::NONE).unwrap(),
        listen_ip: "127.0.0.1".into(),
        listen_port: 0,
        direct_request_timeout_ms: 100,
        ..NetworkConfig::default()
    };

    let manager = NetworkManager::new(NodeId::from("test-node"), config)
        .await
        .expect("create manager");

    assert_eq!(manager.node_id().as_str(), "test-node");
    assert!(!manager.local_peer_id().is_empty());
}

// ───────────────────────── DirectResponseChannel ─────────────────────────

#[tokio::test]
async fn direct_response_channel_test_stub_returns_none_on_take() {
    let channel = DirectResponseChannel::test_stub();
    assert!(channel.take().is_none(), "test_stub should have no stream");
}

#[tokio::test]
async fn direct_response_channel_send_response_on_stub_returns_error() {
    let channel = DirectResponseChannel::test_stub();
    let response = DirectResponse::Error {
        message: "test".into(),
    };
    let result = channel.send_response(response).await;
    assert!(result.is_err(), "send_response on stub should return error");
    if let Err(ActantError::Network(msg)) = result {
        assert!(msg.contains("already consumed"));
    } else {
        panic!("expected Network error");
    }
}

// ───────────────────────── Discovery trait ─────────────────────────

#[test]
fn no_discovery_name() {
    assert_eq!(NoDiscovery.name(), discovery_mode::NONE);
}

#[test]
fn local_discovery_name() {
    assert_eq!(LocalDiscovery.name(), discovery_mode::LOCAL);
}

#[test]
fn mdns_discovery_name() {
    assert_eq!(MdnsDiscovery.name(), discovery_mode::MDNS);
}

#[test]
fn boxed_discovery_delegates_name() {
    let boxed = BoxedDiscovery::new(NoDiscovery);
    assert_eq!(boxed.name(), discovery_mode::NONE);

    let boxed = BoxedDiscovery::new(LocalDiscovery);
    assert_eq!(boxed.name(), discovery_mode::LOCAL);

    let boxed = BoxedDiscovery::new(MdnsDiscovery);
    assert_eq!(boxed.name(), discovery_mode::MDNS);
}

// ───────────────────────── is_registered / registered_names ─────────────────────────

#[test]
fn is_registered_returns_true_for_builtin_modes() {
    assert!(is_registered(discovery_mode::NONE));
    assert!(is_registered(discovery_mode::LOCAL));
    assert!(is_registered(discovery_mode::MDNS));
    assert!(is_registered(discovery_mode::DNS));
    assert!(is_registered(discovery_mode::RELAY));
}

#[test]
fn is_registered_returns_false_for_unknown_mode() {
    assert!(!is_registered("unknown"));
    assert!(!is_registered(""));
    assert!(!is_registered("DNS-over-HTTPS"));
}

#[test]
fn registered_names_contains_all_builtin_modes() {
    let names = registered_names();
    assert_eq!(names.len(), 5);
    assert!(names.contains(&discovery_mode::NONE.to_string()));
    assert!(names.contains(&discovery_mode::LOCAL.to_string()));
    assert!(names.contains(&discovery_mode::MDNS.to_string()));
    assert!(names.contains(&discovery_mode::DNS.to_string()));
    assert!(names.contains(&discovery_mode::RELAY.to_string()));
}

// ───────────────────────── discovery_from_name ─────────────────────────

#[test]
fn discovery_from_name_none_returns_no_discovery() {
    let cfg = NetworkConfig::default();
    let d = discovery_from_name(discovery_mode::NONE, &cfg).unwrap();
    assert_eq!(d.name(), discovery_mode::NONE);
}

#[test]
fn discovery_from_name_local_returns_local_discovery() {
    let cfg = NetworkConfig::default();
    let d = discovery_from_name(discovery_mode::LOCAL, &cfg).unwrap();
    assert_eq!(d.name(), discovery_mode::LOCAL);
}

#[test]
fn discovery_from_name_mdns_returns_mdns_discovery() {
    let cfg = NetworkConfig::default();
    let d = discovery_from_name(discovery_mode::MDNS, &cfg).unwrap();
    assert_eq!(d.name(), discovery_mode::MDNS);
}

#[test]
fn discovery_from_name_dns_returns_dns_discovery() {
    let cfg = NetworkConfig::default();
    let d = discovery_from_name(discovery_mode::DNS, &cfg).unwrap();
    assert_eq!(d.name(), discovery_mode::DNS);
}

#[test]
fn discovery_from_name_dns_uses_custom_origin_domain() {
    let cfg = NetworkConfig {
        dns_origin_domain: "actant.internal.example.com".to_string(),
        ..Default::default()
    };
    let d = discovery_from_name(discovery_mode::DNS, &cfg).unwrap();
    assert_eq!(d.name(), discovery_mode::DNS);
}

#[test]
fn discovery_from_name_relay_returns_relay_discovery() {
    let cfg = NetworkConfig::default();
    let d = discovery_from_name(discovery_mode::RELAY, &cfg).unwrap();
    assert_eq!(d.name(), discovery_mode::RELAY);
}

#[test]
fn discovery_from_name_case_sensitive() {
    let cfg = NetworkConfig::default();
    // 大写不应匹配
    assert!(discovery_from_name("NONE", &cfg).is_err());
    assert!(discovery_from_name("Local", &cfg).is_err());
}

// ───────────────────────── topic_id_from_str ─────────────────────────

#[test]
fn topic_id_from_str_is_deterministic() {
    let t1 = topic_id_from_str("test-topic");
    let t2 = topic_id_from_str("test-topic");
    assert_eq!(t1, t2, "same topic string should produce same TopicId");
}

#[test]
fn topic_id_from_str_different_for_different_topics() {
    let t1 = topic_id_from_str("topic-a");
    let t2 = topic_id_from_str("topic-b");
    assert_ne!(t1, t2, "different topics should produce different TopicIds");
}

// ───────────────────────── parse_endpoint_addr valid cases ─────────────────────────

#[test]
fn parse_endpoint_addr_valid_shorthand() {
    // 使用合法的 64 字符 hex endpoint_id
    let valid_id = "0000000000000000000000000000000000000000000000000000000000000000";
    let addr_str = format!("{}@127.0.0.1:8080", valid_id);
    let result = parse_endpoint_addr(&addr_str);
    assert!(result.is_ok(), "valid shorthand should parse: {:?}", result);
}

#[test]
fn parse_endpoint_addr_trims_whitespace() {
    let valid_id = "0000000000000000000000000000000000000000000000000000000000000000";
    let addr_str = format!("  {}@127.0.0.1:8080  ", valid_id);
    let result = parse_endpoint_addr(&addr_str);
    assert!(result.is_ok(), "whitespace should be trimmed");
}

// ───────────────────────── DirectRequest / DirectResponse serialization ─────────────────────────

#[test]
fn direct_request_task_result_serialization_roundtrip() {
    let req = DirectRequest::TaskResult {
        workflow_id: WorkflowId::from("wf-ser"),
        task_id: TaskId::from("t-ser"),
        task_name: "serial_test".to_string(),
        outcome: WireTaskOutcome::Completed(vec![1, 2]),
        worker_node: NodeId::from("node-A"),
    };
    let bytes = postcard::to_allocvec(&req).expect("serialize");
    let decoded: DirectRequest = postcard::from_bytes(&bytes).expect("deserialize");
    match decoded {
        DirectRequest::TaskResult {
            workflow_id,
            task_id,
            task_name,
            outcome,
            worker_node,
        } => {
            assert_eq!(workflow_id.as_str(), "wf-ser");
            assert_eq!(task_id.as_str(), "t-ser");
            assert_eq!(task_name, "serial_test");
            match outcome {
                WireTaskOutcome::Completed(data) => assert_eq!(data, vec![1, 2]),
                _ => panic!("expected Completed outcome"),
            }
            assert_eq!(worker_node.as_str(), "node-A");
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn direct_request_dispatch_task_serialization_roundtrip() {
    let task = crate::common::TaskDefinition {
        id: TaskId::from("t-disp"),
        name: "dispatch_test".to_string(),
        payload: vec![1, 2, 3],
        workflow_id: Some(WorkflowId::from("wf-disp")),
        target_node: None,
        origin_node: None,
        retry_policy: None,
        priority: 5,
        timeout_ms: Some(30000),
        attempt: 0,
        enqueued_at_ms: 12345,
        target_endpoint_addr: None,
        origin_endpoint_addr: None,
    };
    let req = DirectRequest::DispatchTask { task };
    let bytes = postcard::to_allocvec(&req).expect("serialize");
    let decoded: DirectRequest = postcard::from_bytes(&bytes).expect("deserialize");
    match decoded {
        DirectRequest::DispatchTask { task } => {
            assert_eq!(task.id.as_str(), "t-disp");
            assert_eq!(task.name, "dispatch_test");
            assert_eq!(task.payload, vec![1, 2, 3]);
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn direct_response_task_result_ack_serialization_roundtrip() {
    let resp = DirectResponse::TaskResultAck { accepted: true };
    let bytes = postcard::to_allocvec(&resp).expect("serialize");
    let decoded: DirectResponse = postcard::from_bytes(&bytes).expect("deserialize");
    match decoded {
        DirectResponse::TaskResultAck { accepted } => {
            assert!(accepted);
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn direct_response_dispatch_ack_serialization_roundtrip() {
    let resp = DirectResponse::DispatchAck { accepted: false };
    let bytes = postcard::to_allocvec(&resp).expect("serialize");
    let decoded: DirectResponse = postcard::from_bytes(&bytes).expect("deserialize");
    match decoded {
        DirectResponse::DispatchAck { accepted } => {
            assert!(!accepted);
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn direct_response_workflow_state_serialization_roundtrip() {
    let resp = DirectResponse::WorkflowState {
        dag: Some(vec![0xAB]),
        execution: None,
        pending: Some(vec![0xCD, 0xEF]),
    };
    let bytes = postcard::to_allocvec(&resp).expect("serialize");
    let decoded: DirectResponse = postcard::from_bytes(&bytes).expect("deserialize");
    match decoded {
        DirectResponse::WorkflowState {
            dag,
            execution,
            pending,
        } => {
            assert_eq!(dag, Some(vec![0xAB]));
            assert!(execution.is_none());
            assert_eq!(pending, Some(vec![0xCD, 0xEF]));
        }
        _ => panic!("wrong variant"),
    }
}

#[test]
fn direct_response_error_serialization_roundtrip() {
    let resp = DirectResponse::Error {
        message: "something went wrong".into(),
    };
    let bytes = postcard::to_allocvec(&resp).expect("serialize");
    let decoded: DirectResponse = postcard::from_bytes(&bytes).expect("deserialize");
    match decoded {
        DirectResponse::Error { message } => {
            assert_eq!(message, "something went wrong");
        }
        _ => panic!("wrong variant"),
    }
}

// ───────────────────────── NetworkMessage ─────────────────────────

#[test]
fn network_message_debug_format() {
    let msg = NetworkMessage {
        topic: "test".to_string(),
        data: vec![1, 2, 3],
    };
    let debug_str = format!("{:?}", msg);
    assert!(debug_str.contains("test"));
    assert!(debug_str.contains("NetworkMessage"));
}

// ───────────────────────── peer_allowed edge cases ─────────────────────────

#[test]
fn peer_allowed_trims_whitespace() {
    let allowed = build_allowed_peer_ids(&["peer-a".to_string()]);
    assert!(peer_allowed(&allowed, "  peer-a  "));
}

#[test]
fn peer_allowed_empty_string_in_allowed_set() {
    // build_allowed_peer_ids 过滤空字符串后只剩 "real-peer"
    let allowed = build_allowed_peer_ids(&["".to_string(), "real-peer".to_string()]);
    assert_eq!(allowed.len(), 1); // 空字符串被过滤
    assert!(!peer_allowed(&allowed, "anything")); // 非 allowed 列表中的 peer 被拒绝
    assert!(peer_allowed(&allowed, "real-peer")); // allowed 列表中的 peer 被允许
}

// ───────────────────────── NetworkManager subscribe & broadcast ─────────────────────────

#[tokio::test]
async fn network_manager_subscribe_to_topic_succeeds() {
    let config = NetworkConfig {
        discovery_mode: crate::common::DiscoveryMode::parse(discovery_mode::NONE).unwrap(),
        listen_ip: "127.0.0.1".into(),
        listen_port: 0,
        direct_request_timeout_ms: 100,
        ..NetworkConfig::default()
    };

    let manager = NetworkManager::new(NodeId::from("test-sub"), config)
        .await
        .expect("create manager");

    let result = manager.subscribe("test-topic").await;
    assert!(result.is_ok(), "subscribe should succeed: {:?}", result);

    // 再次订阅同一 topic 也应成功（幂等）
    let result = manager.subscribe("test-topic").await;
    assert!(result.is_ok(), "re-subscribe should succeed");
}

#[tokio::test]
async fn network_manager_broadcast_after_subscribe_succeeds() {
    let config = NetworkConfig {
        discovery_mode: crate::common::DiscoveryMode::parse(discovery_mode::NONE).unwrap(),
        listen_ip: "127.0.0.1".into(),
        listen_port: 0,
        direct_request_timeout_ms: 100,
        ..NetworkConfig::default()
    };

    let manager = NetworkManager::new(NodeId::from("test-bcast"), config)
        .await
        .expect("create manager");

    // 必须先订阅才能广播
    manager.subscribe("broadcast-topic").await.unwrap();
    let result = manager.broadcast("broadcast-topic", vec![1, 2, 3]).await;
    assert!(
        result.is_ok(),
        "broadcast after subscribe should succeed: {:?}",
        result
    );
}

#[tokio::test]
async fn network_manager_broadcast_without_subscribe_returns_error() {
    let config = NetworkConfig {
        discovery_mode: crate::common::DiscoveryMode::parse(discovery_mode::NONE).unwrap(),
        listen_ip: "127.0.0.1".into(),
        listen_port: 0,
        direct_request_timeout_ms: 100,
        ..NetworkConfig::default()
    };

    let manager = NetworkManager::new(NodeId::from("test-nosub"), config)
        .await
        .expect("create manager");

    // 未订阅时广播应返回错误
    let result = manager.broadcast("unsub-topic", vec![1, 2, 3]).await;
    assert!(result.is_err(), "broadcast without subscribe should fail");
}

// ───────────────────────── DirectRequest serialization (remaining variants) ─────────────────────────

#[test]
fn direct_request_query_workflow_state_serialization_roundtrip() {
    let req = DirectRequest::QueryWorkflowState {
        workflow_id: WorkflowId::from("wf-q"),
        requesting_node: NodeId::from("node-q"),
    };
    let bytes = postcard::to_allocvec(&req).expect("serialize");
    let decoded: DirectRequest = postcard::from_bytes(&bytes).expect("deserialize");
    match decoded {
        DirectRequest::QueryWorkflowState {
            workflow_id,
            requesting_node,
        } => {
            assert_eq!(workflow_id.as_str(), "wf-q");
            assert_eq!(requesting_node.as_str(), "node-q");
        }
        _ => panic!("wrong variant"),
    }
}

// ───────────────────────── DirectResponseChannel::send_error ─────────────────────────

#[tokio::test]
async fn direct_response_channel_send_error_on_stub_does_not_panic() {
    let channel = DirectResponseChannel::test_stub();
    // send_error 内部捕获错误并仅记录日志，不应 panic
    channel.send_error("test error message").await;
}

// ───────────────────────── peer_allowed / build_allowed_peer_ids edge cases ─────────────────────────

#[test]
fn peer_allowed_trims_input_whitespace() {
    let allowed = build_allowed_peer_ids(&["  peer-x  ".to_string()]);
    // peer_allowed 内部对输入 trim
    assert!(peer_allowed(&allowed, "  peer-x  "));
    assert!(peer_allowed(&allowed, "peer-x"));
}

#[test]
fn build_allowed_peer_ids_empty_input_returns_empty_set() {
    let allowed = build_allowed_peer_ids(&[]);
    assert!(allowed.is_empty());
    // 空 set 表示允许所有 peer
    assert!(peer_allowed(&allowed, "anyone"));
}

// ───────────────────────── topic_id_from_str determinism ─────────────────────────

#[test]
fn topic_id_from_str_same_input_produces_same_id() {
    let t1 = topic_id_from_str("deterministic-topic");
    let t2 = topic_id_from_str("deterministic-topic");
    assert_eq!(t1, t2);
}

// ───────────────────────── DirectProtocolHandler construction ─────────────────────────

#[test]
fn direct_protocol_handler_clone_preserves_fields() {
    let event_tx = tokio::sync::mpsc::channel::<DirectEvent>(16).0;
    let handler = DirectProtocolHandler {
        event_tx: event_tx.clone(),
        allowed_peer_ids: Arc::new(build_allowed_peer_ids(&["peer-1".to_string()])),
        semaphore: Arc::new(tokio::sync::Semaphore::new(4)),
        max_message_size: 1024,
    };
    let cloned = handler.clone();
    assert_eq!(cloned.max_message_size, 1024);
    assert_eq!(cloned.allowed_peer_ids.len(), 1);
}

#[test]
fn direct_event_request_variant_constructs() {
    let channel = DirectResponseChannel::test_stub();
    let event = DirectEvent::Request {
        peer_id: "peer-test".to_string(),
        request: DirectRequest::QueryWorkflowState {
            workflow_id: WorkflowId::from("wf-ev"),
            requesting_node: NodeId::from("node-ev"),
        },
        channel,
    };
    // 仅验证可构造且 Debug 可打印（DirectResponseChannel 实现 Debug）
    assert!(format!("{:?}", event).contains("peer-test"));
}

// ───────────────────────── NetworkMessage / ListenAddresses / PeerId ─────────────────────────

#[test]
fn peer_id_wraps_string() {
    let p = PeerId("node-abc".to_string());
    assert_eq!(p.0, "node-abc");
}

#[test]
fn listen_addresses_constructs_with_fields() {
    let la = ListenAddresses {
        endpoint_id: "ep-1".to_string(),
        relay_url: Some("https://relay".to_string()),
        direct_addrs: vec!["127.0.0.1:8080".to_string()],
        endpoint_addr: "ep-1@127.0.0.1:8080".to_string(),
    };
    assert_eq!(la.endpoint_id, "ep-1");
    assert!(la.relay_url.is_some());
    assert_eq!(la.direct_addrs.len(), 1);
}

// ───────────────────────── NetworkManager additional methods ─────────────────────────

fn test_network_config() -> NetworkConfig {
    NetworkConfig {
        discovery_mode: crate::common::DiscoveryMode::parse(discovery_mode::NONE).unwrap(),
        listen_ip: "127.0.0.1".into(),
        listen_port: 0,
        direct_request_timeout_ms: 100,
        ..NetworkConfig::default()
    }
}

#[tokio::test]
async fn network_manager_discover_peers_empty_by_default() {
    let manager = NetworkManager::new(NodeId::from("test-peers"), test_network_config())
        .await
        .expect("create manager");

    let peers = manager.discover_peers().await.expect("discover_peers");
    assert!(peers.is_empty());
}

#[tokio::test]
async fn network_manager_add_gossip_peer_invalid_id_returns_error() {
    let manager = NetworkManager::new(NodeId::from("test-add-peer"), test_network_config())
        .await
        .expect("create manager");

    let result = manager.add_gossip_peer("not-a-valid-endpoint-id").await;
    assert!(result.is_err(), "invalid endpoint id should fail");
    if let Err(ActantError::Network(msg)) = result {
        assert!(msg.contains("invalid endpoint id"));
    } else {
        panic!("expected Network error");
    }
}

#[tokio::test]
async fn network_manager_dial_invalid_address_returns_error() {
    let manager = NetworkManager::new(NodeId::from("test-dial"), test_network_config())
        .await
        .expect("create manager");

    let result = manager.dial("not-an-address").await;
    assert!(result.is_err(), "invalid address should fail");
}

#[tokio::test]
async fn network_manager_send_direct_request_invalid_address_returns_error() {
    let manager = NetworkManager::new(NodeId::from("test-req"), test_network_config())
        .await
        .expect("create manager");

    let request = DirectRequest::QueryWorkflowState {
        workflow_id: WorkflowId::from("wf-1"),
        requesting_node: NodeId::from("test-req"),
    };

    let result = manager.send_direct_request("bad-address", request).await;
    assert!(result.is_err(), "invalid peer address should fail");
    if let Err(ActantError::Network(msg)) = result {
        assert!(msg.contains("invalid peer address"));
    } else {
        panic!("expected Network error");
    }
}

#[tokio::test]
async fn network_manager_send_direct_response_on_stub_channel_returns_error() {
    let manager = NetworkManager::new(NodeId::from("test-resp"), test_network_config())
        .await
        .expect("create manager");

    let channel = DirectResponseChannel::test_stub();
    let response = DirectResponse::Error {
        message: "test".into(),
    };
    let result = manager.send_direct_response(channel, response).await;
    assert!(result.is_err(), "stub channel should fail");
}

#[tokio::test]
async fn network_manager_shutdown_succeeds() {
    let manager = NetworkManager::new(NodeId::from("test-shutdown"), test_network_config())
        .await
        .expect("create manager");

    let result = manager.shutdown().await;
    assert!(result.is_ok(), "shutdown should succeed: {:?}", result);
}

// ───────────────────────── parse_endpoint_addr additional cases ─────────────────────────

#[test]
fn parse_endpoint_addr_whitespace_only_returns_error() {
    let err = parse_endpoint_addr("   ").unwrap_err();
    assert!(matches!(err, ActantError::Config(_)));
}

#[test]
fn parse_endpoint_addr_valid_full_hex_roundtrip() {
    // 构造一个有效的 EndpointAddr：全零 node_id + 本地地址
    let valid_id = "0000000000000000000000000000000000000000000000000000000000000000";
    let addr = iroh::EndpointAddr::new(valid_id.parse().unwrap())
        .with_ip_addr("127.0.0.1:8080".parse().unwrap());
    let bytes = postcard::to_allocvec(&addr).unwrap();
    let hex = data_encoding::HEXLOWER.encode(&bytes);

    let result = parse_endpoint_addr(&hex);
    assert!(result.is_ok(), "hex-encoded endpoint addr should parse");
}

#[test]
fn parse_endpoint_addr_invalid_hex_decodes_but_not_postcard() {
    // 合法 hex 但内容不是 EndpointAddr
    let hex = data_encoding::HEXLOWER.encode(b"not-an-endpoint-addr");
    let err = parse_endpoint_addr(&hex).unwrap_err();
    assert!(matches!(err, ActantError::Config(_)));
}

// ───────────────────────── NetworkManager error paths ─────────────────────────

#[tokio::test]
async fn network_manager_new_with_invalid_listen_ip_returns_error() {
    let mut config = test_network_config();
    config.listen_ip = "not-an-ip".into();
    let result = NetworkManager::new(NodeId::from("test-bad-ip"), config).await;
    assert!(result.is_err(), "invalid listen_ip should fail");
    if let Err(ActantError::Config(msg)) = result {
        assert!(msg.contains("invalid listen_ip"));
    } else {
        panic!("expected Config error");
    }
}

#[tokio::test]
async fn network_manager_new_with_invalid_discovery_mode_returns_error() {
    let mut config = test_network_config();
    config.discovery_mode = crate::common::DiscoveryMode::new_unchecked("no-such-mode");
    let result = NetworkManager::new(NodeId::from("test-bad-discovery"), config).await;
    assert!(result.is_err(), "invalid discovery mode should fail");
}

// ───────────────────────── route_direct_request ─────────────────────────

fn stub_direct_event(payload_len: usize) -> DirectEvent {
    // route_direct_request 按请求序列化后的字节数判定超限，
    // 用等长 workflow_id 使 payload_len 真实反映序列化大小。
    DirectEvent::Request {
        peer_id: "peer-under-test".to_string(),
        request: DirectRequest::QueryWorkflowState {
            workflow_id: crate::common::WorkflowId::from("w".repeat(payload_len)),
            requesting_node: NodeId::from("caller"),
        },
        channel: DirectResponseChannel::test_stub(),
    }
}

/// 超尺寸请求应被拒绝且回送错误（stub channel 上表现为仅日志），不进入转发通道。
#[tokio::test]
async fn route_direct_request_rejects_oversize() {
    let (tx, rx) = mpsc::channel::<NetworkEvent>(4);
    let outcome = route_direct_request(&tx, 64, stub_direct_event(256)).await;
    assert_eq!(outcome, DirectRouteOutcome::RejectedOversize);
    assert!(rx.is_empty(), "rejected request must not be forwarded");
}

/// event channel 有空位时请求正常转发。
#[tokio::test]
async fn route_direct_request_forwards_within_limit() {
    let (tx, mut rx) = mpsc::channel::<NetworkEvent>(4);
    let outcome = route_direct_request(&tx, 1024, stub_direct_event(16)).await;
    assert_eq!(outcome, DirectRouteOutcome::Forwarded);
    match rx.recv().await {
        Some(NetworkEvent::DirectRequest { peer_id, .. }) => {
            assert_eq!(peer_id, "peer-under-test");
        }
        other => panic!("expected DirectRequest event, got {:?}", other.is_some()),
    }
}

/// event channel 已满时请求被丢弃并回错（不静默丢弃导致对端等超时）。
#[tokio::test]
async fn route_direct_request_reports_channel_full() {
    let (tx, mut rx) = mpsc::channel::<NetworkEvent>(1);
    // 占满唯一槽位。
    tx.send(NetworkEvent::Message(NetworkMessage {
        topic: "filler".to_string(),
        data: vec![],
    }))
    .await
    .expect("fill channel");

    let outcome = route_direct_request(&tx, 1024, stub_direct_event(16)).await;
    assert_eq!(outcome, DirectRouteOutcome::ChannelFull);

    // 排空后通道内不应混入该直连请求。
    let drained = rx.recv().await;
    assert!(
        matches!(drained, Some(NetworkEvent::Message(_))),
        "only the filler event should be in the channel"
    );
    assert!(rx.is_empty());
}

/// 并发订阅同一 topic：所有调用成功，且订阅表恰好只有一条记录
/// （check-then-insert 在写锁内原子完成后不会重复 gossip.subscribe）。
#[tokio::test]
async fn network_manager_concurrent_subscribe_is_idempotent() {
    let manager = NetworkManager::new(NodeId::from("test-sub-race"), test_network_config())
        .await
        .expect("create manager");

    const N: usize = 8;
    // join_all 并发 poll 各订阅 future（不 spawn，避免 'static 借用约束），
    // 足以触发 check-then-insert 的并发窗口。
    let futures: Vec<_> = (0..N).map(|_| manager.subscribe("race-topic")).collect();
    for result in futures::future::join_all(futures).await {
        result.expect("subscribe ok");
    }

    let subs = manager.topic_subscriptions.read().await;
    assert_eq!(
        subs.len(),
        1,
        "exactly one subscription should be registered"
    );
    assert!(subs.contains_key("race-topic"));
}
