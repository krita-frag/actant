use async_trait::async_trait;

use pyo3::prelude::*;

use crate::common::{ActorMessage, ActorMessageResult, Result};
use crate::runtime::actor::Actor;

pub struct PythonActor {
    actor_type: String,
    dispatcher: Py<PyAny>,
}

impl PythonActor {
    pub fn new(actor_type: String, dispatcher: Py<PyAny>) -> Self {
        Self {
            actor_type,
            dispatcher,
        }
    }
}

#[async_trait]
impl Actor for PythonActor {
    fn actor_type(&self) -> &str {
        &self.actor_type
    }

    async fn handle_message(&mut self, msg: ActorMessage) -> Result<ActorMessageResult> {
        let dispatcher = Python::attach(|py| self.dispatcher.clone_ref(py));
        let method = msg.method.clone();
        let payload = msg.payload.clone();
        let message_id = msg.id.clone();

        tokio::task::spawn_blocking(move || {
            Python::attach(|py| {
                let disp = dispatcher.bind(py);
                let py_payload = pyo3::types::PyBytes::new(py, &payload);
                let result_bytes: Vec<u8> = disp
                    .call_method1("_handle_message", (method.as_str(), py_payload))
                    .map_err(|e| {
                        crate::common::ActantError::Internal(format!("actor dispatch error: {}", e))
                    })?
                    .extract()
                    .map_err(|e| {
                        crate::common::ActantError::Internal(format!(
                            "actor result extraction error: {}",
                            e
                        ))
                    })?;

                Ok(ActorMessageResult {
                    message_id,
                    payload: result_bytes,
                    error: None,
                })
            })
        })
        .await
        .map_err(|e| crate::common::ActantError::Internal(e.to_string()))?
    }

    fn supports_state_persistence(&self) -> bool {
        true
    }

    fn save_state(&self) -> Result<Vec<u8>> {
        let dispatcher = Python::attach(|py| self.dispatcher.clone_ref(py));
        tokio::task::block_in_place(|| {
            Python::attach(|py| {
                let disp = dispatcher.bind(py);
                let result: Vec<u8> = disp
                    .call_method0("_save_state")
                    .map_err(|e| {
                        crate::common::ActantError::Internal(format!(
                            "actor save_state error: {}",
                            e
                        ))
                    })?
                    .extract()
                    .map_err(|e| {
                        crate::common::ActantError::Internal(format!(
                            "actor save_state extraction error: {}",
                            e
                        ))
                    })?;
                Ok(result)
            })
        })
    }

    fn load_state(&mut self, state: &[u8]) -> Result<()> {
        tokio::task::block_in_place(|| {
            Python::attach(|py| {
                let disp = self.dispatcher.bind(py);
                let py_state = pyo3::types::PyBytes::new(py, state);
                disp.call_method1("_load_state", (py_state,)).map_err(|e| {
                    crate::common::ActantError::Internal(format!("actor load_state error: {}", e))
                })?;
                Ok(())
            })
        })
    }
}
