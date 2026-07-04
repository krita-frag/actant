use async_trait::async_trait;

use crate::common::{ActorId, ActorMessage, ActorMessageResult, ActorStatus, Result};

#[async_trait]
pub trait Actor: Send + Sync + 'static {
    fn actor_type(&self) -> &str;

    async fn handle_message(&mut self, msg: ActorMessage) -> Result<ActorMessageResult>;

    fn save_state(&self) -> Result<Vec<u8>> {
        Ok(vec![])
    }

    fn load_state(&mut self, _state: &[u8]) -> Result<()> {
        Ok(())
    }

    fn supports_state_persistence(&self) -> bool {
        false
    }

    async fn on_start(&mut self) -> Result<()> {
        Ok(())
    }

    async fn on_stop(&mut self) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Actor for Box<dyn Actor> {
    fn actor_type(&self) -> &str {
        (**self).actor_type()
    }

    async fn handle_message(&mut self, msg: ActorMessage) -> Result<ActorMessageResult> {
        (**self).handle_message(msg).await
    }

    fn save_state(&self) -> Result<Vec<u8>> {
        (**self).save_state()
    }

    fn load_state(&mut self, state: &[u8]) -> Result<()> {
        (**self).load_state(state)
    }

    fn supports_state_persistence(&self) -> bool {
        (**self).supports_state_persistence()
    }

    async fn on_start(&mut self) -> Result<()> {
        (**self).on_start().await
    }

    async fn on_stop(&mut self) -> Result<()> {
        (**self).on_stop().await
    }
}

pub struct ActorContext {
    pub actor_id: ActorId,
    pub status: ActorStatus,
}

impl ActorContext {
    pub fn new(actor_id: ActorId) -> Self {
        Self {
            actor_id,
            status: ActorStatus::Created,
        }
    }

    pub fn transition(&mut self, new_status: ActorStatus) -> Result<()> {
        let valid = matches!(
            (&self.status, &new_status),
            (ActorStatus::Created, ActorStatus::Running)
                | (ActorStatus::Running, ActorStatus::Failed)
                | (ActorStatus::Failed, ActorStatus::Running)
                | (ActorStatus::Running, ActorStatus::Stopped)
        );

        if !valid {
            return Err(crate::common::ActantError::Actor(format!(
                "invalid state transition: {:?} -> {:?}",
                self.status, new_status
            )));
        }

        self.status = new_status;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_context_starts_in_created_state() {
        let ctx = ActorContext::new(ActorId("a1".into()));
        assert_eq!(ctx.status, ActorStatus::Created);
        assert_eq!(ctx.actor_id.0, "a1");
    }

    #[test]
    fn transition_created_to_running_succeeds() {
        let mut ctx = ActorContext::new(ActorId("a1".into()));
        ctx.transition(ActorStatus::Running).unwrap();
        assert_eq!(ctx.status, ActorStatus::Running);
    }

    #[test]
    fn transition_running_to_failed_succeeds() {
        let mut ctx = ActorContext::new(ActorId("a1".into()));
        ctx.transition(ActorStatus::Running).unwrap();
        ctx.transition(ActorStatus::Failed).unwrap();
        assert_eq!(ctx.status, ActorStatus::Failed);
    }

    #[test]
    fn transition_failed_to_running_succeeds_for_restart() {
        let mut ctx = ActorContext::new(ActorId("a1".into()));
        ctx.transition(ActorStatus::Running).unwrap();
        ctx.transition(ActorStatus::Failed).unwrap();
        ctx.transition(ActorStatus::Running).unwrap();
        assert_eq!(ctx.status, ActorStatus::Running);
    }

    #[test]
    fn transition_running_to_stopped_succeeds() {
        let mut ctx = ActorContext::new(ActorId("a1".into()));
        ctx.transition(ActorStatus::Running).unwrap();
        ctx.transition(ActorStatus::Stopped).unwrap();
        assert_eq!(ctx.status, ActorStatus::Stopped);
    }

    #[test]
    fn transition_created_to_stopped_rejected() {
        let mut ctx = ActorContext::new(ActorId("a1".into()));
        let err = ctx.transition(ActorStatus::Stopped).unwrap_err();
        assert!(err.to_string().contains("invalid state transition"));
        assert_eq!(ctx.status, ActorStatus::Created);
    }

    #[test]
    fn transition_created_to_failed_rejected() {
        let mut ctx = ActorContext::new(ActorId("a1".into()));
        assert!(ctx.transition(ActorStatus::Failed).is_err());
        assert_eq!(ctx.status, ActorStatus::Created);
    }

    #[test]
    fn transition_stopped_to_running_rejected() {
        let mut ctx = ActorContext::new(ActorId("a1".into()));
        ctx.transition(ActorStatus::Running).unwrap();
        ctx.transition(ActorStatus::Stopped).unwrap();
        assert!(ctx.transition(ActorStatus::Running).is_err());
        assert_eq!(ctx.status, ActorStatus::Stopped);
    }

    #[test]
    fn transition_failed_to_stopped_rejected() {
        let mut ctx = ActorContext::new(ActorId("a1".into()));
        ctx.transition(ActorStatus::Running).unwrap();
        ctx.transition(ActorStatus::Failed).unwrap();
        assert!(ctx.transition(ActorStatus::Stopped).is_err());
        assert_eq!(ctx.status, ActorStatus::Failed);
    }

    #[test]
    fn transition_same_state_rejected() {
        let mut ctx = ActorContext::new(ActorId("a1".into()));
        ctx.transition(ActorStatus::Running).unwrap();
        assert!(ctx.transition(ActorStatus::Running).is_err());
    }
}
