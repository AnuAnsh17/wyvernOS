use async_trait::async_trait;
use serde_json::json;
use std::sync::Arc;
use wyvern::capability::mock_echo_definition;
use wyvern::skills::{
    SkillAdapter, SkillCapabilityBinding, SkillCapabilityDeclaration, SkillHealthReport,
    SkillHealthStatus, SkillManifest,
};
use wyvern::{CapabilityDescriptor, CapabilityExecutor, CapabilityKind, RiskLevel};

struct MockSkillAdapter {
    manifest: SkillManifest,
}

#[async_trait]
impl SkillAdapter for MockSkillAdapter {
    fn manifest(&self) -> &SkillManifest {
        &self.manifest
    }

    async fn capability_bindings(&self) -> Result<Vec<SkillCapabilityBinding>, wyvern::SkillError> {
        let (descriptor, tool, executor) = mock_echo_definition();
        Ok(vec![SkillCapabilityBinding::new(
            descriptor,
            vec![tool],
            executor,
        )])
    }

    async fn healthcheck(&self) -> Result<SkillHealthReport, wyvern::SkillError> {
        Ok(SkillHealthReport {
            status: SkillHealthStatus::Healthy,
            detail: None,
        })
    }
}

fn manifest() -> SkillManifest {
    let mut manifest = SkillManifest::new("mock skill", "0.1.0", "local:test", "MIT");
    manifest.capabilities.push(SkillCapabilityDeclaration {
        name: "echo".into(),
        capability_id: None,
    });
    manifest.risk = RiskLevel::Low;
    manifest.supported_platforms = vec!["windows".into(), "linux".into()];
    manifest.configuration = json!({"mode": "test"});
    manifest
}

#[test]
fn skill_manifest_validates_and_round_trips() {
    let manifest = manifest();
    manifest.validate().unwrap();
    let encoded = serde_json::to_string(&manifest).unwrap();
    let decoded: SkillManifest = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded, manifest);
}

#[test]
fn invalid_skill_manifest_reports_the_first_missing_identity_field() {
    let mut manifest = manifest();
    manifest.license.clear();
    assert!(matches!(
        manifest.validate(),
        Err(wyvern::SkillError::InvalidManifest(message)) if message.contains("license")
    ));

    manifest.license = "MIT".into();
    manifest.capabilities[0].name.clear();
    assert!(matches!(
        manifest.validate(),
        Err(wyvern::SkillError::InvalidManifest(message)) if message.contains("capability")
    ));
}

#[tokio::test]
async fn adapter_exposes_process_local_bindings_and_health() {
    let adapter = MockSkillAdapter {
        manifest: manifest(),
    };
    assert_eq!(adapter.manifest().name, "mock skill");
    let bindings = adapter.capability_bindings().await.unwrap();
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0].tools.len(), 1);
    assert_eq!(
        adapter.healthcheck().await.unwrap().status,
        SkillHealthStatus::Healthy
    );
}

#[test]
fn capability_binding_keeps_descriptor_and_executor_together() {
    let descriptor = CapabilityDescriptor::new(
        "test capability",
        "1.0.0",
        "test skill",
        CapabilityKind::Custom,
    );
    let capability_id = descriptor.id;
    let tool = wyvern::ToolDescriptor::new(
        "test tool",
        "test",
        capability_id,
        json!({}),
        json!({}),
        RiskLevel::Low,
    );
    let (_, _, executor) = mock_echo_definition();
    let binding = SkillCapabilityBinding::new(descriptor, vec![tool], executor);
    assert_eq!(binding.descriptor.id, capability_id);
    assert_eq!(binding.tools[0].capability_id, capability_id);
    let _: Arc<dyn CapabilityExecutor> = binding.executor;
}
