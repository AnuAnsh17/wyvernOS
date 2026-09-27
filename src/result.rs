use crate::domain::{RuntimeId, Timestamp};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeOutcome {
    Completed,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RuntimeResult {
    pub runtime_id: RuntimeId,
    pub status: RuntimeOutcome,
    pub summary: String,
    pub data: Value,
    pub child_results: Vec<RuntimeResult>,
    pub artifacts: Vec<String>,
    pub timestamp: Timestamp,
}

impl RuntimeResult {
    pub fn completed(
        runtime_id: RuntimeId,
        summary: impl Into<String>,
        data: Value,
        child_results: Vec<RuntimeResult>,
    ) -> Self {
        Self {
            runtime_id,
            status: RuntimeOutcome::Completed,
            summary: summary.into(),
            data,
            child_results,
            artifacts: Vec::new(),
            timestamp: crate::domain::now_millis(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn completed_result_is_serializable_and_structured() {
        let runtime_id = RuntimeId::new();
        let result =
            RuntimeResult::completed(runtime_id, "done", json!({ "ok": true }), Vec::new());
        assert_eq!(result.runtime_id, runtime_id);
        assert_eq!(result.status, RuntimeOutcome::Completed);
        assert_eq!(result.data["ok"], true);
        let encoded = serde_json::to_string(&result).unwrap();
        assert!(encoded.contains("done"));
    }
}
