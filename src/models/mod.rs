//! Provider-neutral model contracts and deterministic routing.

use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelCapability {
    Text,
    Vision,
    Tools,
    StructuredOutput,
    Streaming,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelPrivacy {
    Public,
    Private,
    Local,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Model {
    pub id: String,
    pub provider: String,
    pub capabilities: Vec<ModelCapability>,
    pub privacy: ModelPrivacy,
    pub context_window: u32,
    pub input_cost_per_million_tokens: Option<f64>,
    pub output_cost_per_million_tokens: Option<f64>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ModelUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cost_usd: Option<f64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InferenceRequest {
    pub task: String,
    pub prompt: String,
    pub system_prompt: Option<String>,
    pub max_tokens: Option<u32>,
    pub temperature: Option<f32>,
    pub metadata: BTreeMap<String, Value>,
}

impl InferenceRequest {
    pub fn new(task: impl Into<String>, prompt: impl Into<String>) -> Self {
        Self {
            task: task.into(),
            prompt: prompt.into(),
            system_prompt: None,
            max_tokens: None,
            temperature: None,
            metadata: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InferenceResponse {
    pub model_id: String,
    pub text: String,
    pub usage: ModelUsage,
    pub finish_reason: Option<String>,
    pub metadata: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelPolicy {
    pub preferred_models: Vec<String>,
    pub fallback_models: Vec<String>,
    pub required_capabilities: Vec<ModelCapability>,
    pub privacy: Option<ModelPrivacy>,
    pub max_cost_usd: Option<f64>,
}

impl Default for ModelPolicy {
    fn default() -> Self {
        Self {
            preferred_models: Vec::new(),
            fallback_models: Vec::new(),
            required_capabilities: vec![ModelCapability::Text],
            privacy: None,
            max_cost_usd: None,
        }
    }
}

#[derive(Debug, Error)]
pub enum ModelError {
    #[error("model provider is not registered: {0}")]
    ProviderNotFound(String),
    #[error("model is not registered: {0}")]
    ModelNotFound(String),
    #[error("no model satisfies the routing policy")]
    NoEligibleModel,
    #[error("model provider failed: {0}")]
    Provider(String),
    #[error("duplicate model: {0}")]
    DuplicateModel(String),
}

#[async_trait]
pub trait ModelProvider: Send + Sync {
    fn name(&self) -> &str;

    async fn infer(
        &self,
        model: &Model,
        request: InferenceRequest,
    ) -> Result<InferenceResponse, ModelError>;
}

#[derive(Clone)]
pub struct OpenAiCompatibleProvider {
    name: String,
    endpoint: String,
    api_key: Option<String>,
    client: Client,
}

impl OpenAiCompatibleProvider {
    pub fn new(name: impl Into<String>, endpoint: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            endpoint: endpoint.into().trim_end_matches('/').to_owned(),
            api_key: None,
            client: Client::new(),
        }
    }

    pub fn with_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(api_key.into());
        self
    }
}

#[async_trait]
impl ModelProvider for OpenAiCompatibleProvider {
    fn name(&self) -> &str {
        &self.name
    }

    async fn infer(
        &self,
        model: &Model,
        request: InferenceRequest,
    ) -> Result<InferenceResponse, ModelError> {
        let mut messages = Vec::new();
        if let Some(system_prompt) = request.system_prompt {
            messages.push(serde_json::json!({
                "role": "system",
                "content": system_prompt,
            }));
        }
        messages.push(serde_json::json!({
            "role": "user",
            "content": request.prompt,
        }));
        let mut body = serde_json::json!({
            "model": model.id,
            "messages": messages,
        });
        if let Some(max_tokens) = request.max_tokens {
            body["max_tokens"] = serde_json::json!(max_tokens);
        }
        if let Some(temperature) = request.temperature {
            body["temperature"] = serde_json::json!(temperature);
        }
        let mut call = self
            .client
            .post(format!("{}/chat/completions", self.endpoint))
            .json(&body);
        if let Some(api_key) = &self.api_key {
            call = call.bearer_auth(api_key);
        }
        let response = call
            .send()
            .await
            .map_err(|error| ModelError::Provider(error.to_string()))?
            .error_for_status()
            .map_err(|error| ModelError::Provider(error.to_string()))?;
        let payload: Value = response
            .json()
            .await
            .map_err(|error| ModelError::Provider(error.to_string()))?;
        let text = payload
            .pointer("/choices/0/message/content")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ModelError::Provider("response did not contain message content".into())
            })?;
        let usage = ModelUsage {
            input_tokens: payload
                .pointer("/usage/prompt_tokens")
                .and_then(Value::as_u64)
                .unwrap_or_default(),
            output_tokens: payload
                .pointer("/usage/completion_tokens")
                .and_then(Value::as_u64)
                .unwrap_or_default(),
            cost_usd: None,
        };
        Ok(InferenceResponse {
            model_id: model.id.clone(),
            text: text.to_owned(),
            usage,
            finish_reason: payload
                .pointer("/choices/0/finish_reason")
                .and_then(Value::as_str)
                .map(str::to_owned),
            metadata: BTreeMap::new(),
        })
    }
}

#[derive(Clone)]
pub struct ModelRouter {
    models: Arc<RwLock<HashMap<String, Model>>>,
    providers: Arc<RwLock<HashMap<String, Arc<dyn ModelProvider>>>>,
}

impl Default for ModelRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl ModelRouter {
    pub fn new() -> Self {
        Self {
            models: Arc::new(RwLock::new(HashMap::new())),
            providers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn register_provider(&self, provider: Arc<dyn ModelProvider>) {
        self.providers
            .write()
            .await
            .insert(provider.name().to_owned(), provider);
    }

    pub async fn register_model(&self, model: Model) -> Result<(), ModelError> {
        let mut models = self.models.write().await;
        if models.contains_key(&model.id) {
            return Err(ModelError::DuplicateModel(model.id));
        }
        if !self.providers.read().await.contains_key(&model.provider) {
            return Err(ModelError::ProviderNotFound(model.provider));
        }
        models.insert(model.id.clone(), model);
        Ok(())
    }

    pub async fn route(
        &self,
        policy: &ModelPolicy,
        request: &InferenceRequest,
    ) -> Result<Model, ModelError> {
        self.candidate_models(policy, request)
            .await
            .into_iter()
            .next()
            .ok_or(ModelError::NoEligibleModel)
    }

    async fn candidate_models(
        &self,
        policy: &ModelPolicy,
        request: &InferenceRequest,
    ) -> Vec<Model> {
        let models = self.models.read().await;
        let mut candidates = Vec::new();
        for id in policy
            .preferred_models
            .iter()
            .chain(policy.fallback_models.iter())
        {
            if let Some(model) = models.get(id) {
                candidates.push(model.clone());
            }
        }
        if candidates.is_empty() {
            candidates.extend(models.values().cloned());
            candidates.sort_by(|left, right| left.id.cmp(&right.id));
        }
        candidates
            .into_iter()
            .filter(|model| eligible(model, policy, request))
            .collect()
    }

    pub async fn infer(
        &self,
        policy: &ModelPolicy,
        request: InferenceRequest,
    ) -> Result<InferenceResponse, ModelError> {
        let candidates = self.candidate_models(policy, &request).await;
        if candidates.is_empty() {
            return Err(ModelError::NoEligibleModel);
        }
        let providers = self.providers.read().await;
        let mut last_error = None;
        for model in candidates {
            let Some(provider) = providers.get(&model.provider).cloned() else {
                last_error = Some(ModelError::ProviderNotFound(model.provider));
                continue;
            };
            match provider.infer(&model, request.clone()).await {
                Ok(response) => return Ok(response),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or(ModelError::NoEligibleModel))
    }
}

fn eligible(model: &Model, policy: &ModelPolicy, request: &InferenceRequest) -> bool {
    let has_capabilities = policy
        .required_capabilities
        .iter()
        .all(|required| model.capabilities.contains(required));
    let privacy_matches = policy
        .privacy
        .as_ref()
        .is_none_or(|privacy| privacy == &model.privacy);
    let token_cost = request.max_tokens.map(|tokens| {
        model
            .output_cost_per_million_tokens
            .map(|cost| cost * f64::from(tokens) / 1_000_000.0)
    });
    let cost_matches = policy
        .max_cost_usd
        .is_none_or(|maximum| token_cost.flatten().is_none_or(|cost| cost <= maximum));
    has_capabilities && privacy_matches && cost_matches
}
