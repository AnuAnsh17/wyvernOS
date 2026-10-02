//! Typed contracts for reusable capability packages.

use crate::capability::{
    CapabilityDescriptor, CapabilityExecutor, CapabilityRegistry, RiskLevel, ToolDescriptor,
};
use crate::domain::{CapabilityId, SkillId};
use crate::error::WyvernError;
use crate::persistence::RuntimeStore;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SkillManifest {
    pub id: SkillId,
    pub name: String,
    pub version: String,
    pub source: String,
    pub license: String,
    pub capabilities: Vec<SkillCapabilityDeclaration>,
    pub dependencies: Vec<String>,
    pub risk: RiskLevel,
    pub supported_platforms: Vec<String>,
    pub entrypoint: Option<String>,
    pub configuration: Value,
    pub healthcheck: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillCapabilityDeclaration {
    pub name: String,
    pub capability_id: Option<CapabilityId>,
}

impl SkillManifest {
    pub fn new(
        name: impl Into<String>,
        version: impl Into<String>,
        source: impl Into<String>,
        license: impl Into<String>,
    ) -> Self {
        Self {
            id: SkillId::new(),
            name: name.into(),
            version: version.into(),
            source: source.into(),
            license: license.into(),
            capabilities: Vec::new(),
            dependencies: Vec::new(),
            risk: RiskLevel::Low,
            supported_platforms: Vec::new(),
            entrypoint: None,
            configuration: Value::Object(Default::default()),
            healthcheck: None,
        }
    }

    pub fn validate(&self) -> Result<(), SkillError> {
        for (field, value) in [
            ("name", self.name.as_str()),
            ("version", self.version.as_str()),
            ("source", self.source.as_str()),
            ("license", self.license.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(SkillError::InvalidManifest(format!(
                    "{field} must not be empty"
                )));
            }
        }
        if self
            .capabilities
            .iter()
            .any(|capability| capability.name.trim().is_empty())
        {
            return Err(SkillError::InvalidManifest(
                "capability names must not be empty".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct SkillCapabilityBinding {
    pub descriptor: CapabilityDescriptor,
    pub tools: Vec<ToolDescriptor>,
    pub executor: Arc<dyn CapabilityExecutor>,
}

impl SkillCapabilityBinding {
    pub fn new(
        descriptor: CapabilityDescriptor,
        tools: Vec<ToolDescriptor>,
        executor: Arc<dyn CapabilityExecutor>,
    ) -> Self {
        Self {
            descriptor,
            tools,
            executor,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillHealthStatus {
    Healthy,
    Degraded,
    Unavailable,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillHealthReport {
    pub status: SkillHealthStatus,
    pub detail: Option<String>,
}

#[derive(Debug, Error)]
pub enum SkillError {
    #[error("invalid skill manifest: {0}")]
    InvalidManifest(String),
    #[error("skill health check failed: {0}")]
    HealthcheckFailed(String),
    #[error("skill capability binding failed: {0}")]
    BindingFailed(String),
    #[error("skill registry error: {0}")]
    Registry(String),
    #[error("skill persistence failed: {0}")]
    Persistence(String),
}

#[derive(Clone)]
pub struct SkillRegistry {
    store: Arc<dyn RuntimeStore>,
    adapters: Arc<RwLock<HashMap<SkillId, Arc<dyn SkillAdapter>>>>,
}

impl SkillRegistry {
    pub fn new(store: Arc<dyn RuntimeStore>) -> Self {
        Self {
            store,
            adapters: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn register_manifest(&self, manifest: SkillManifest) -> Result<(), SkillError> {
        manifest.validate()?;
        self.store
            .insert_skill_manifest(&manifest)
            .await
            .map_err(storage_error)
    }

    pub async fn get(&self, skill_id: SkillId) -> Result<Option<SkillManifest>, SkillError> {
        self.store
            .get_skill_manifest(skill_id)
            .await
            .map_err(storage_error)
    }

    pub async fn list(&self) -> Result<Vec<SkillManifest>, SkillError> {
        self.store
            .list_skill_manifests()
            .await
            .map_err(storage_error)
    }

    pub async fn bind_adapter(&self, adapter: Arc<dyn SkillAdapter>) -> Result<(), SkillError> {
        let manifest = adapter.manifest();
        manifest.validate()?;
        let Some(persisted) = self.get(manifest.id).await? else {
            return Err(SkillError::Registry(format!(
                "skill manifest is not registered: {}",
                manifest.id
            )));
        };
        if persisted != *manifest {
            return Err(SkillError::BindingFailed(format!(
                "adapter metadata does not match registered skill: {}",
                manifest.id
            )));
        }
        self.adapters.write().await.insert(manifest.id, adapter);
        Ok(())
    }

    pub async fn install_adapter(
        &self,
        adapter: Arc<dyn SkillAdapter>,
        capabilities: &CapabilityRegistry,
    ) -> Result<Vec<SkillCapabilityBinding>, SkillError> {
        self.bind_adapter(adapter.clone()).await?;
        let bindings = adapter.capability_bindings().await?;
        for binding in &bindings {
            capabilities
                .register(
                    binding.descriptor.clone(),
                    binding.tools.clone(),
                    binding.executor.clone(),
                )
                .await
                .map_err(|error| SkillError::BindingFailed(error.to_string()))?;
        }
        Ok(bindings)
    }

    pub async fn adapter(&self, skill_id: SkillId) -> Option<Arc<dyn SkillAdapter>> {
        self.adapters.read().await.get(&skill_id).cloned()
    }
}

fn storage_error(error: WyvernError) -> SkillError {
    SkillError::Persistence(error.to_string())
}

#[async_trait]
pub trait SkillAdapter: Send + Sync {
    fn manifest(&self) -> &SkillManifest;

    async fn capability_bindings(&self) -> Result<Vec<SkillCapabilityBinding>, SkillError>;

    async fn healthcheck(&self) -> Result<SkillHealthReport, SkillError>;
}
