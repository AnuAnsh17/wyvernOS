use async_trait::async_trait;
use std::sync::Arc;
use wyvern::models::{
    InferenceRequest, InferenceResponse, Model, ModelCapability, ModelError, ModelPolicy,
    ModelPrivacy, ModelProvider, ModelRouter, ModelUsage, OpenAiCompatibleProvider,
};

struct MockProvider {
    provider_name: &'static str,
    fail: bool,
}

#[async_trait]
impl ModelProvider for MockProvider {
    fn name(&self) -> &str {
        self.provider_name
    }

    async fn infer(
        &self,
        model: &Model,
        _request: InferenceRequest,
    ) -> Result<InferenceResponse, ModelError> {
        if self.fail {
            return Err(ModelError::Provider("temporary outage".into()));
        }
        Ok(InferenceResponse {
            model_id: model.id.clone(),
            text: format!("response from {}", model.id),
            usage: ModelUsage::default(),
            finish_reason: Some("stop".into()),
            metadata: Default::default(),
        })
    }
}

fn model(id: &str, provider: &str, privacy: ModelPrivacy) -> Model {
    Model {
        id: id.into(),
        provider: provider.into(),
        capabilities: vec![ModelCapability::Text],
        privacy,
        context_window: 8_192,
        input_cost_per_million_tokens: Some(1.0),
        output_cost_per_million_tokens: Some(2.0),
    }
}

async fn router() -> ModelRouter {
    let router = ModelRouter::new();
    router
        .register_provider(Arc::new(MockProvider {
            provider_name: "cloud",
            fail: true,
        }))
        .await;
    router
        .register_provider(Arc::new(MockProvider {
            provider_name: "local",
            fail: false,
        }))
        .await;
    router
        .register_model(model("frontier", "cloud", ModelPrivacy::Public))
        .await
        .unwrap();
    router
        .register_model(model("local-small", "local", ModelPrivacy::Local))
        .await
        .unwrap();
    router
}

#[tokio::test]
async fn router_prefers_configured_model_and_falls_back_after_provider_failure() {
    let router = router().await;
    let request = InferenceRequest::new("coding", "write a test");
    let policy = ModelPolicy {
        preferred_models: vec!["frontier".into()],
        fallback_models: vec!["local-small".into()],
        ..ModelPolicy::default()
    };
    assert_eq!(
        router.route(&policy, &request).await.unwrap().id,
        "frontier"
    );
    let response = router.infer(&policy, request).await.unwrap();
    assert_eq!(response.model_id, "local-small");
}

#[tokio::test]
async fn policy_filters_by_privacy_capability_and_cost() {
    let router = router().await;
    let request = InferenceRequest {
        max_tokens: Some(100_000),
        ..InferenceRequest::new("private", "classify")
    };
    let policy = ModelPolicy {
        preferred_models: vec!["frontier".into(), "local-small".into()],
        privacy: Some(ModelPrivacy::Local),
        max_cost_usd: Some(0.01),
        ..ModelPolicy::default()
    };
    assert!(matches!(
        router.route(&policy, &request).await,
        Err(ModelError::NoEligibleModel)
    ));
}

#[tokio::test]
async fn model_registration_requires_provider_and_rejects_duplicates() {
    let router = ModelRouter::new();
    let missing = router
        .register_model(model("missing", "unknown", ModelPrivacy::Private))
        .await;
    assert!(matches!(missing, Err(ModelError::ProviderNotFound(_))));
    router
        .register_provider(Arc::new(MockProvider {
            provider_name: "local",
            fail: false,
        }))
        .await;
    router
        .register_model(model("local-model", "local", ModelPrivacy::Local))
        .await
        .unwrap();
    assert!(matches!(
        router
            .register_model(model("local-model", "local", ModelPrivacy::Local))
            .await,
        Err(ModelError::DuplicateModel(id)) if id == "local-model"
    ));
}

#[tokio::test]
async fn openai_compatible_provider_parses_a_chat_completion() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0u8; 8192];
        let _ = socket.read(&mut request).await.unwrap();
        let payload = r#"{"choices":[{"message":{"content":"hello from gateway"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":4}}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            payload.len(),
            payload
        );
        socket.write_all(response.as_bytes()).await.unwrap();
    });

    let provider = OpenAiCompatibleProvider::new("gateway", format!("http://{address}/v1/"));
    let response = provider
        .infer(
            &model("frontier", "gateway", ModelPrivacy::Public),
            InferenceRequest::new("test", "say hello"),
        )
        .await
        .unwrap();
    server.await.unwrap();
    assert_eq!(response.text, "hello from gateway");
    assert_eq!(response.usage.input_tokens, 3);
    assert_eq!(response.usage.output_tokens, 4);
    assert_eq!(response.finish_reason.as_deref(), Some("stop"));
}
