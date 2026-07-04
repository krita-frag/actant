use crate::common::ActorId;

#[derive(Debug, Clone)]
#[allow(clippy::enum_variant_names)]
#[non_exhaustive]
pub enum SupervisionEvent {
    ActorStarted { actor_id: ActorId },
    ActorFailed { actor_id: ActorId, error: String },
    ActorStopped { actor_id: ActorId },
}

pub(crate) struct SupervisionState {
    pub(crate) event_tx: tokio::sync::broadcast::Sender<SupervisionEvent>,
}

impl SupervisionState {
    pub fn with_capacity(capacity: usize) -> Self {
        let (event_tx, _) = tokio::sync::broadcast::channel(capacity);
        Self { event_tx }
    }

    pub fn emit(&self, event: SupervisionEvent) {
        if let Err(e) = self.event_tx.send(event) {
            tracing::trace!("supervision event send failed (no receivers): {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emit_delivers_to_subscriber() {
        // broadcast channel 在无 receiver 时 emit 静默失败，
        // 必须先 subscribe 再 emit 才能接收到事件。
        let state = SupervisionState::with_capacity(16);
        let mut rx = state.event_tx.subscribe();

        state.emit(SupervisionEvent::ActorStarted {
            actor_id: ActorId("a1".into()),
        });

        let event = rx.try_recv().expect("should receive emitted event");
        match event {
            SupervisionEvent::ActorStarted { actor_id } => {
                assert_eq!(actor_id.0, "a1");
            }
            _ => panic!("expected ActorStarted, got {:?}", event),
        }
    }

    #[test]
    fn emit_without_subscribers_is_silent_noop() {
        // 无 receiver 时 emit 不应 panic — 日志仅 trace 级别记录。
        let state = SupervisionState::with_capacity(16);
        state.emit(SupervisionEvent::ActorStarted {
            actor_id: ActorId("a1".into()),
        });
    }

    #[test]
    fn multiple_subscribers_all_receive_event() {
        let state = SupervisionState::with_capacity(16);
        let mut rx1 = state.event_tx.subscribe();
        let mut rx2 = state.event_tx.subscribe();

        state.emit(SupervisionEvent::ActorFailed {
            actor_id: ActorId("a1".into()),
            error: "boom".into(),
        });

        assert!(rx1.try_recv().is_ok());
        assert!(rx2.try_recv().is_ok());
    }

    #[test]
    fn subscriber_receives_events_in_order() {
        let state = SupervisionState::with_capacity(16);
        let mut rx = state.event_tx.subscribe();

        state.emit(SupervisionEvent::ActorStarted {
            actor_id: ActorId("a1".into()),
        });
        state.emit(SupervisionEvent::ActorStopped {
            actor_id: ActorId("a1".into()),
        });

        let first = rx.try_recv().unwrap();
        let second = rx.try_recv().unwrap();
        assert!(matches!(first, SupervisionEvent::ActorStarted { .. }));
        assert!(matches!(second, SupervisionEvent::ActorStopped { .. }));
    }
}
