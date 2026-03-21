use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use bytes::Bytes;
use futures_util::StreamExt;
use rand::RngCore;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex, OnceLock, RwLock,
};
use tauri::Emitter;
use tokio::sync::oneshot;
use tokio::sync::Notify;

static TOOL_CALL_ID_MAP: OnceLock<Mutex<HashMap<String, (String, i64)>>> = OnceLock::new();

fn tool_call_id_map() -> &'static Mutex<HashMap<String, (String, i64)>> {
    TOOL_CALL_ID_MAP.get_or_init(|| Mutex::new(HashMap::new()))
}

fn store_tool_call_id_mapping(client_call_id: &str, upstream_call_id: &str) {
    let client_call_id = client_call_id.trim();
    let upstream_call_id = upstream_call_id.trim();
    if client_call_id.is_empty() || upstream_call_id.is_empty() {
        return;
    }

    let now = chrono::Utc::now().timestamp();
    let mut map = tool_call_id_map().lock().unwrap_or_else(|e| e.into_inner());
    map.insert(
        client_call_id.to_string(),
        (upstream_call_id.to_string(), now),
    );

    // Best-effort TTL + size cap to avoid unbounded growth.
    const TTL_SECS: i64 = 60 * 60;
    const MAX: usize = 20_000;
    map.retain(|_, (_, ts)| now.saturating_sub(*ts) <= TTL_SECS);
    if map.len() > MAX {
        // Drop arbitrary extra keys (hashmap iteration order is fine for a cap).
        let extra = map.len() - MAX;
        let keys: Vec<String> = map.keys().take(extra).cloned().collect();
        for k in keys {
            map.remove(&k);
        }
    }
}

fn resolve_upstream_tool_call_id(call_id: &str) -> String {
    let call_id = call_id.trim();
    if call_id.is_empty() {
        return String::new();
    }
    let unwrapped = unwrap_client_tool_call_id(call_id);
    if unwrapped != call_id {
        return unwrapped;
    }
    if !call_id.starts_with("chatcmpl-tool-") {
        return call_id.to_string();
    }
    let map = tool_call_id_map().lock().unwrap_or_else(|e| e.into_inner());
    map.get(call_id)
        .map(|(upstream, _)| upstream.clone())
        .unwrap_or_else(|| call_id.to_string())
}

// ─── paths ───────────────────────────────────────────────────────────────────

fn codex_dir() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
    PathBuf::from(home).join(".codex")
}

fn accounts_dir() -> PathBuf {
    codex_dir().join("accounts")
}

fn auth_file() -> PathBuf {
    codex_dir().join("auth.json")
}

fn meta_file() -> PathBuf {
    codex_dir().join("accounts_meta.json")
}

fn proxy_log_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
    PathBuf::from(home).join(".codex-manager").join("proxy.log")
}

fn proxy_log_db_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
    PathBuf::from(home)
        .join(".codex-manager")
        .join("proxy_logs.db")
}

fn proxy_config_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
    PathBuf::from(home)
        .join(".codex-manager")
        .join("proxy_config.json")
}

fn log_proxy(message: &str) {
    let path = proxy_log_path();
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    eprintln!("{message}");
    if let Ok(mut file) = fs::OpenOptions::new().create(true).append(true).open(&path) {
        let ts = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ");
        let _ = writeln!(file, "{ts} {message}");
    }
}

fn log_proxy_error_detail(
    request_id: usize,
    status: u16,
    method: &str,
    path: &str,
    request_url: &Option<String>,
    request_headers: &Option<String>,
    request_body: &Option<String>,
    response_body: &Option<String>,
    error: &Option<String>,
) {
    let detail = serde_json::json!({
        "status": status,
        "method": method,
        "path": path,
        "request_url": request_url,
        "request_headers": request_headers,
        "request_body": request_body,
        "response_body": response_body,
        "error": error,
    });
    log_proxy(&format!("req#{request_id} error detail {detail}"));
}

#[derive(Serialize, Deserialize, Clone)]
struct ProxyConfig {
    api_key: Option<String>,
    enable_logging: bool,
    max_logs: usize,
    #[serde(default)]
    disable_on_usage_limit: bool,
    #[serde(default)]
    model_override: Option<String>,
    #[serde(default)]
    reasoning_effort_override: Option<String>,
    #[serde(default = "default_proxy_upstream_mode")]
    upstream_mode: String,
    #[serde(default)]
    custom_openai_base_url: Option<String>,
    #[serde(default)]
    custom_openai_api_key: Option<String>,
    #[serde(default = "default_enable_exact_cache")]
    enable_exact_cache: bool,
    #[serde(default = "default_exact_cache_ttl_minutes")]
    exact_cache_ttl_minutes: i64,
    #[serde(default = "default_exact_cache_max_entries")]
    exact_cache_max_entries: usize,
    #[serde(default)]
    enable_semantic_cache: bool,
    #[serde(default = "default_semantic_cache_threshold")]
    semantic_cache_threshold: f64,
    #[serde(default = "default_vector_provider_mode")]
    vector_provider_mode: String,
    #[serde(default)]
    vector_api_base_url: Option<String>,
    #[serde(default)]
    vector_api_key: Option<String>,
    #[serde(default)]
    vector_model: Option<String>,
}

fn default_proxy_upstream_mode() -> String {
    "codex".to_string()
}

fn default_enable_exact_cache() -> bool {
    true
}

fn default_exact_cache_ttl_minutes() -> i64 {
    60
}

fn default_exact_cache_max_entries() -> usize {
    2000
}

fn default_semantic_cache_threshold() -> f64 {
    0.95
}

fn default_vector_provider_mode() -> String {
    "local".to_string()
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            api_key: None,
            enable_logging: true,
            max_logs: 1000,
            disable_on_usage_limit: false,
            model_override: None,
            reasoning_effort_override: None,
            upstream_mode: default_proxy_upstream_mode(),
            custom_openai_base_url: None,
            custom_openai_api_key: None,
            enable_exact_cache: default_enable_exact_cache(),
            exact_cache_ttl_minutes: default_exact_cache_ttl_minutes(),
            exact_cache_max_entries: default_exact_cache_max_entries(),
            enable_semantic_cache: false,
            semantic_cache_threshold: default_semantic_cache_threshold(),
            vector_provider_mode: default_vector_provider_mode(),
            vector_api_base_url: None,
            vector_api_key: None,
            vector_model: None,
        }
    }
}

static PROXY_CONFIG: OnceLock<Mutex<ProxyConfig>> = OnceLock::new();
static APP_HANDLE: OnceLock<tauri::AppHandle> = OnceLock::new();

fn openai_compat_configs_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
    PathBuf::from(home)
        .join(".codex-manager")
        .join("openai_compat_configs.json")
}

#[derive(Serialize, Deserialize, Clone, Default)]
struct OpenAICompatModelMapping {
    alias: String,
    provider_model: String,
}

#[derive(Serialize, Deserialize, Clone)]
struct OpenAICompatProviderConfig {
    id: String,
    provider_name: String,
    base_url: String,
    api_key: String,
    default_model: Option<String>,
    #[serde(default)]
    model_mappings: Vec<OpenAICompatModelMapping>,
    created_at: u64,
    updated_at: u64,
}

struct OpenAICompatProxyState {
    client: reqwest::Client,
    config: Arc<RwLock<OpenAICompatProviderConfig>>,
}

#[derive(Serialize, Deserialize, Clone, Default)]
struct OpenAICompatProbeCheck {
    name: String,
    method: String,
    path: String,
    status: u16,
    ok: bool,
    duration_ms: u64,
    summary: String,
    response_excerpt: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Default)]
struct OpenAICompatProbeResult {
    provider_name: String,
    base_url: String,
    requested_model: String,
    effective_model: String,
    checked_at: u64,
    supports_models: bool,
    supports_chat_completions: bool,
    supports_responses: bool,
    supports_messages: bool,
    chat_tool_call_ok: bool,
    responses_tool_call_ok: bool,
    streaming_tool_call_ok: bool,
    recommended_strategy: String,
    recommendations: Vec<String>,
    checks: Vec<OpenAICompatProbeCheck>,
}

static OPENAI_COMPAT_PROXY_SHUTDOWN: Mutex<Option<oneshot::Sender<()>>> = Mutex::new(None);
static OPENAI_COMPAT_PROXY_PORT: Mutex<Option<u16>> = Mutex::new(None);
static OPENAI_COMPAT_PROXY_STATE: Mutex<Option<Arc<OpenAICompatProxyState>>> = Mutex::new(None);

// Pending OAuth session (verifier + state + redirect_uri) for manual callback flow
struct OAuthPending {
    verifier: String,
    state: String,
    redirect_uri: String,
}
static OAUTH_PENDING: OnceLock<Mutex<Option<OAuthPending>>> = OnceLock::new();
fn oauth_pending() -> &'static Mutex<Option<OAuthPending>> {
    OAUTH_PENDING.get_or_init(|| Mutex::new(None))
}

fn load_proxy_config() -> ProxyConfig {
    let path = proxy_config_path();
    if let Ok(content) = fs::read_to_string(&path) {
        if let Ok(cfg) = serde_json::from_str::<ProxyConfig>(&content) {
            return cfg;
        }
    }
    ProxyConfig::default()
}

fn save_proxy_config(cfg: &ProxyConfig) -> Result<(), String> {
    let path = proxy_config_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let payload = serde_json::to_string_pretty(cfg).map_err(|e| e.to_string())?;
    fs::write(path, payload).map_err(|e| e.to_string())
}

fn proxy_config() -> &'static Mutex<ProxyConfig> {
    PROXY_CONFIG.get_or_init(|| Mutex::new(load_proxy_config()))
}

fn emit_accounts_updated(reason: &str) {
    if let Some(handle) = APP_HANDLE.get() {
        let _ = handle.emit(
            "accounts_updated",
            serde_json::json!({
                "reason": reason,
            }),
        );
    }
}

fn proxy_config_snapshot() -> ProxyConfig {
    proxy_config().lock().unwrap().clone()
}

fn load_openai_compat_configs() -> Vec<OpenAICompatProviderConfig> {
    let path = openai_compat_configs_path();
    if !path.exists() {
        return vec![];
    }
    fs::read_to_string(&path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

fn save_openai_compat_configs(configs: &[OpenAICompatProviderConfig]) -> Result<(), String> {
    let path = openai_compat_configs_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let payload = serde_json::to_string_pretty(configs).map_err(|e| e.to_string())?;
    fs::write(path, payload).map_err(|e| e.to_string())
}

fn sanitize_openai_compat_mappings(
    mappings: Vec<OpenAICompatModelMapping>,
) -> Vec<OpenAICompatModelMapping> {
    let mut out = Vec::new();
    for mapping in mappings {
        let alias = mapping.alias.trim().to_string();
        let provider_model = mapping.provider_model.trim().to_string();
        if alias.is_empty() || provider_model.is_empty() {
            continue;
        }
        out.push(OpenAICompatModelMapping {
            alias,
            provider_model,
        });
    }
    out
}

fn now_unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn build_openai_compat_provider_config(
    id: Option<String>,
    provider_name: String,
    base_url: String,
    api_key: String,
    default_model: Option<String>,
    model_mappings: Vec<OpenAICompatModelMapping>,
    created_at: Option<u64>,
) -> Result<OpenAICompatProviderConfig, String> {
    let provider_name = provider_name.trim().to_string();
    if provider_name.is_empty() {
        return Err("Provider 命名不能为空".to_string());
    }
    let base_url = base_url.trim().trim_end_matches('/').to_string();
    if base_url.is_empty() {
        return Err("兼容地址不能为空".to_string());
    }
    let api_key = api_key.trim().to_string();
    if api_key.is_empty() {
        return Err("兼容 API Key 不能为空".to_string());
    }
    let default_model = default_model
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    let now = now_unix_millis();
    Ok(OpenAICompatProviderConfig {
        id: id.unwrap_or_else(|| format!("openai-compat-{now}")),
        provider_name,
        base_url,
        api_key,
        default_model,
        model_mappings: sanitize_openai_compat_mappings(model_mappings),
        created_at: created_at.unwrap_or(now),
        updated_at: now,
    })
}

fn openai_compat_exposed_models(config: &OpenAICompatProviderConfig) -> Vec<String> {
    let mut models = Vec::new();
    for mapping in &config.model_mappings {
        if !mapping.alias.is_empty() {
            models.push(mapping.alias.clone());
        }
    }
    if let Some(default_model) = config
        .default_model
        .as_ref()
        .filter(|v| !v.trim().is_empty())
    {
        models.push(default_model.trim().to_string());
    }
    models.sort();
    models.dedup();
    models
}

fn openai_compat_is_glm_family(
    config: &OpenAICompatProviderConfig,
    requested_model: Option<&str>,
) -> bool {
    let mut haystacks = vec![config.provider_name.as_str(), config.base_url.as_str()];
    if let Some(model) = config.default_model.as_deref() {
        haystacks.push(model);
    }
    if let Some(model) = requested_model {
        haystacks.push(model);
    }
    for mapping in &config.model_mappings {
        haystacks.push(mapping.alias.as_str());
        haystacks.push(mapping.provider_model.as_str());
    }
    haystacks.into_iter().any(|value| {
        let normalized = value.to_ascii_lowercase();
        normalized.contains("glm")
            || normalized.contains("openclaw")
            || normalized.contains("zhipu")
            || normalized.contains("midea")
    })
}

fn map_openai_compat_model(
    config: &OpenAICompatProviderConfig,
    requested_model: Option<&str>,
) -> Option<String> {
    let requested = requested_model
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string())
        .or_else(|| config.default_model.clone())?;
    for mapping in &config.model_mappings {
        if mapping.alias == "*" {
            return Some(mapping.provider_model.clone());
        }
    }
    for mapping in &config.model_mappings {
        if mapping.alias.eq_ignore_ascii_case(&requested) {
            return Some(mapping.provider_model.clone());
        }
    }
    if requested.eq_ignore_ascii_case("glm5") {
        Some("glm-5".to_string())
    } else {
        Some(requested)
    }
}

const GLM_CODING_AGENT_TOOL_BIAS_PROMPT: &str = "You are operating inside a tool-using coding agent runtime. When tools are available and the task requires inspection, editing, searching, execution, or patching, emit tool calls instead of describing what you would do. Prefer apply_patch for file edits. If the user explicitly requests a tool, call it before prose. Tool arguments must be valid JSON and complete.";

fn maybe_inject_glm_coding_agent_bias(
    config: &OpenAICompatProviderConfig,
    request_model: Option<&str>,
    chat_request: &mut Value,
) -> bool {
    if !openai_compat_is_glm_family(config, request_model) {
        return false;
    }
    let has_tools = chat_request
        .get("tools")
        .and_then(|v| v.as_array())
        .map(|v| !v.is_empty())
        .unwrap_or(false);
    if !has_tools {
        return false;
    }

    let Some(messages) = chat_request
        .get_mut("messages")
        .and_then(|v| v.as_array_mut())
    else {
        return false;
    };

    let already_present = messages.iter().any(|message| {
        let role = message.get("role").and_then(|v| v.as_str()).unwrap_or("");
        if role != "system" {
            return false;
        }
        response_message_text(message.get("content").unwrap_or(&Value::Null))
            .contains(GLM_CODING_AGENT_TOOL_BIAS_PROMPT)
    });
    if !already_present {
        messages.insert(
            0,
            serde_json::json!({
                "role": "system",
                "content": GLM_CODING_AGENT_TOOL_BIAS_PROMPT,
            }),
        );
    }

    if chat_request.get("temperature").is_none() {
        chat_request["temperature"] = serde_json::json!(0.2);
    }
    if chat_request.get("tool_choice").is_none() {
        chat_request["tool_choice"] = serde_json::json!("auto");
    }
    if chat_request.get("parallel_tool_calls").is_none() {
        chat_request["parallel_tool_calls"] = serde_json::json!(true);
    }
    true
}

fn normalize_proxy_upstream_mode(value: &str) -> &'static str {
    let normalized = value.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "openai" | "openai-compatible" | "openai_compatible" | "custom_openai"
        | "custom-openai" => "openai_compatible",
        _ => "codex",
    }
}

fn normalized_custom_base_url(value: Option<&String>) -> Option<String> {
    value
        .map(|v| v.trim().trim_end_matches('/').to_string())
        .filter(|v| !v.is_empty())
}

fn normalized_custom_api_key(value: Option<&String>) -> Option<String> {
    value
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn proxy_uses_custom_openai(cfg: &ProxyConfig) -> bool {
    normalize_proxy_upstream_mode(&cfg.upstream_mode) == "openai_compatible"
}

fn proxy_custom_openai_ready(cfg: &ProxyConfig) -> bool {
    proxy_uses_custom_openai(cfg)
        && normalized_custom_base_url(cfg.custom_openai_base_url.as_ref()).is_some()
        && normalized_custom_api_key(cfg.custom_openai_api_key.as_ref()).is_some()
}

fn proxy_api_key_valid(headers: &axum::http::HeaderMap) -> bool {
    let cfg = proxy_config_snapshot();
    let expected = match cfg.api_key {
        Some(value) if !value.trim().is_empty() => value,
        _ => return true,
    };
    let bearer = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|v| v.trim().to_string());
    let api_key = headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim().to_string());
    bearer.as_deref() == Some(expected.as_str()) || api_key.as_deref() == Some(expected.as_str())
}

#[derive(Serialize, Deserialize, Clone)]
struct ProxyLogSummary {
    id: i64,
    timestamp: String,
    method: String,
    path: String,
    status: u16,
    duration_ms: u64,
    proxy_account_id: String,
    account_id: Option<String>,
    error: Option<String>,
    model: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
struct ProxyLogDetail {
    id: i64,
    timestamp: String,
    method: String,
    path: String,
    request_url: Option<String>,
    status: u16,
    duration_ms: u64,
    proxy_account_id: String,
    account_id: Option<String>,
    error: Option<String>,
    model: Option<String>,
    request_headers: Option<String>,
    response_headers: Option<String>,
    request_body: Option<String>,
    response_body: Option<String>,
    input_tokens: Option<i64>,
    output_tokens: Option<i64>,
    cache_status: Option<String>,
    cache_key: Option<String>,
    cache_eligible: Option<bool>,
    cache_bypass_reason: Option<String>,
    local_cached_input_tokens: Option<i64>,
    provider_cached_input_tokens: Option<i64>,
}

#[derive(Serialize, Deserialize, Clone)]
struct ProxyTokenStatsItem {
    name: String,
    requests: i64,
    input_tokens: i64,
    output_tokens: i64,
    total_tokens: i64,
}

#[derive(Serialize, Deserialize, Clone)]
struct ProxyTokenStats {
    window_hours: i64,
    total_requests: i64,
    success_requests: i64,
    error_requests: i64,
    input_tokens: i64,
    output_tokens: i64,
    total_tokens: i64,
    avg_duration_ms: f64,
    top_models: Vec<ProxyTokenStatsItem>,
    top_accounts: Vec<ProxyTokenStatsItem>,
}

#[derive(Serialize, Deserialize, Clone)]
struct AICacheOverview {
    window_hours: i64,
    total_requests: i64,
    cache_eligible_requests: i64,
    local_hits: i64,
    local_misses: i64,
    bypassed_requests: i64,
    provider_cached_requests: i64,
    local_hit_rate: f64,
    input_tokens: i64,
    output_tokens: i64,
    local_cached_input_tokens: i64,
    provider_cached_input_tokens: i64,
    total_cached_input_tokens: i64,
    avg_hit_duration_ms: f64,
    avg_miss_duration_ms: f64,
}

#[derive(Serialize, Deserialize, Clone)]
struct AICacheTrendPoint {
    bucket: String,
    total_requests: i64,
    cache_eligible_requests: i64,
    local_hits: i64,
    provider_cached_input_tokens: i64,
    local_cached_input_tokens: i64,
    input_tokens: i64,
    output_tokens: i64,
}

#[derive(Serialize, Deserialize, Clone)]
struct AICacheEntrySummary {
    id: i64,
    cache_key: String,
    path: String,
    model: Option<String>,
    cache_type: String,
    hit_count: i64,
    input_tokens: i64,
    output_tokens: i64,
    local_cached_input_tokens: i64,
    provider_cached_input_tokens: i64,
    created_at: String,
    last_hit_at: String,
    expires_at: String,
    response_preview: Option<String>,
}

struct LocalCacheHit {
    cache_key: String,
    body: Bytes,
    status: u16,
    content_type: String,
    model: Option<String>,
    input_tokens: i64,
    output_tokens: i64,
    provider_cached_input_tokens: i64,
}

struct ProxyLogEntry {
    timestamp: String,
    method: String,
    path: String,
    request_url: Option<String>,
    status: u16,
    duration_ms: u64,
    proxy_account_id: String,
    account_id: Option<String>,
    error: Option<String>,
    model: Option<String>,
    request_headers: Option<String>,
    response_headers: Option<String>,
    request_body: Option<String>,
    response_body: Option<String>,
    input_tokens: Option<i64>,
    output_tokens: Option<i64>,
    cache_status: Option<String>,
    cache_key: Option<String>,
    cache_eligible: Option<bool>,
    cache_bypass_reason: Option<String>,
    local_cached_input_tokens: Option<i64>,
    provider_cached_input_tokens: Option<i64>,
}

impl Default for ProxyLogEntry {
    fn default() -> Self {
        Self {
            timestamp: String::new(),
            method: String::new(),
            path: String::new(),
            request_url: None,
            status: 0,
            duration_ms: 0,
            proxy_account_id: String::new(),
            account_id: None,
            error: None,
            model: None,
            request_headers: None,
            response_headers: None,
            request_body: None,
            response_body: None,
            input_tokens: None,
            output_tokens: None,
            cache_status: None,
            cache_key: None,
            cache_eligible: None,
            cache_bypass_reason: None,
            local_cached_input_tokens: None,
            provider_cached_input_tokens: None,
        }
    }
}

fn proxy_log_db() -> Result<Connection, String> {
    let path = proxy_log_db_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let conn = Connection::open(path).map_err(|e| e.to_string())?;
    init_proxy_log_db(&conn)?;
    Ok(conn)
}

fn init_proxy_log_db(conn: &Connection) -> Result<(), String> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS request_logs (            id INTEGER PRIMARY KEY AUTOINCREMENT,            timestamp TEXT NOT NULL,            method TEXT NOT NULL,            path TEXT NOT NULL,            request_url TEXT,            status INTEGER NOT NULL,            duration_ms INTEGER NOT NULL,            proxy_account_id TEXT NOT NULL,            account_id TEXT,            error TEXT,            request_headers TEXT,            response_headers TEXT,            request_body TEXT,            response_body TEXT,            model TEXT,            input_tokens INTEGER,            output_tokens INTEGER        )",
        [],
    )
    .map_err(|e| e.to_string())?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS ai_cache_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cache_key TEXT NOT NULL UNIQUE,
            cache_type TEXT NOT NULL,
            path TEXT NOT NULL,
            method TEXT NOT NULL,
            model TEXT,
            status INTEGER NOT NULL,
            content_type TEXT NOT NULL,
            response_body BLOB NOT NULL,
            response_preview TEXT,
            input_tokens INTEGER NOT NULL DEFAULT 0,
            output_tokens INTEGER NOT NULL DEFAULT 0,
            local_cached_input_tokens INTEGER NOT NULL DEFAULT 0,
            provider_cached_input_tokens INTEGER NOT NULL DEFAULT 0,
            hit_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            last_hit_at TEXT NOT NULL,
            expires_at TEXT NOT NULL
        )",
        [],
    )
    .map_err(|e| e.to_string())?;
    ensure_log_columns(conn)?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_request_logs_timestamp ON request_logs (id DESC)",
        [],
    )
    .map_err(|e| e.to_string())?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_request_logs_status ON request_logs (status)",
        [],
    )
    .map_err(|e| e.to_string())?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ai_cache_entries_expires_at ON ai_cache_entries (expires_at)",
        [],
    )
    .map_err(|e| e.to_string())?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ai_cache_entries_last_hit_at ON ai_cache_entries (last_hit_at DESC)",
        [],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

fn ensure_log_columns(conn: &Connection) -> Result<(), String> {
    let mut stmt = conn
        .prepare("PRAGMA table_info(request_logs)")
        .map_err(|e| e.to_string())?;
    let cols_iter = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|e| e.to_string())?;
    let mut cols = std::collections::HashSet::new();
    for col in cols_iter {
        cols.insert(col.map_err(|e| e.to_string())?);
    }

    let required = vec![
        ("request_url", "TEXT"),
        ("request_headers", "TEXT"),
        ("response_headers", "TEXT"),
        ("request_body", "TEXT"),
        ("response_body", "TEXT"),
        ("model", "TEXT"),
        ("input_tokens", "INTEGER"),
        ("output_tokens", "INTEGER"),
        ("cache_status", "TEXT"),
        ("cache_key", "TEXT"),
        ("cache_eligible", "INTEGER"),
        ("cache_bypass_reason", "TEXT"),
        ("local_cached_input_tokens", "INTEGER"),
        ("provider_cached_input_tokens", "INTEGER"),
    ];
    for (name, ty) in required {
        if !cols.contains(name) {
            let sql = format!("ALTER TABLE request_logs ADD COLUMN {name} {ty}");
            conn.execute(&sql, []).map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}

fn extract_provider_cached_input_tokens_from_value(value: &Value) -> i64 {
    if let Some(usage) = value.get("usage") {
        if let Some(v) = usage
            .get("cache_read_input_tokens")
            .and_then(|v| v.as_i64())
            .or_else(|| {
                usage
                    .get("input_tokens_details")
                    .and_then(|v| v.get("cached_tokens"))
                    .and_then(|v| v.as_i64())
            })
            .or_else(|| {
                usage
                    .get("prompt_tokens_details")
                    .and_then(|v| v.get("cached_tokens"))
                    .and_then(|v| v.as_i64())
            })
        {
            return v.max(0);
        }
    }
    0
}

fn extract_provider_cached_input_tokens_from_text(text: Option<&String>) -> Option<i64> {
    let text = text?.trim();
    if text.is_empty() {
        return None;
    }
    let value: Value = serde_json::from_str(text).ok()?;
    let cached = extract_provider_cached_input_tokens_from_value(&value);
    Some(cached)
}

fn insert_proxy_log(entry: &ProxyLogEntry) -> Result<(), String> {
    let cfg = proxy_config_snapshot();
    if !cfg.enable_logging {
        return Ok(());
    }
    let (derived_cache_eligible, derived_cache_key, _, derived_bypass_reason) = if cfg
        .enable_exact_cache
    {
        match entry.request_body.as_ref() {
            Some(body) => evaluate_local_cache_request(&entry.method, &entry.path, body.as_bytes()),
            None => (false, None, None, Some("empty_body".to_string())),
        }
    } else {
        (false, None, None, Some("exact_cache_disabled".to_string()))
    };
    let cache_eligible = entry.cache_eligible.or(Some(derived_cache_eligible));
    let cache_key = entry.cache_key.clone().or(derived_cache_key);
    let cache_bypass_reason = entry.cache_bypass_reason.clone().or_else(|| {
        if cache_eligible == Some(true) {
            None
        } else {
            derived_bypass_reason
        }
    });
    let cache_status = entry.cache_status.clone().or_else(|| {
        if cache_eligible == Some(true) {
            Some("miss".to_string())
        } else {
            Some("bypass".to_string())
        }
    });
    let provider_cached_input_tokens = entry
        .provider_cached_input_tokens
        .or_else(|| extract_provider_cached_input_tokens_from_text(entry.response_body.as_ref()));
    if cache_eligible == Some(true)
        && cache_status.as_deref() != Some("local_hit")
        && entry.status >= 200
        && entry.status < 300
    {
        if let (Some(key), Some(response_body)) = (cache_key.as_ref(), entry.response_body.as_ref())
        {
            let content_type = entry
                .response_headers
                .as_ref()
                .and_then(|text| serde_json::from_str::<Vec<(String, String)>>(text).ok())
                .and_then(|headers| {
                    headers
                        .into_iter()
                        .find(|(name, _)| name.eq_ignore_ascii_case("content-type"))
                        .map(|(_, value)| value)
                })
                .unwrap_or_else(|| "application/json".to_string());
            let _ = store_local_cache_entry(
                key,
                &entry.method,
                &entry.path,
                entry.model.as_deref(),
                entry.status,
                &content_type,
                response_body.as_bytes(),
                entry.input_tokens,
                entry.output_tokens,
                provider_cached_input_tokens,
            );
        }
    }
    let conn = proxy_log_db()?;
    conn.execute(
        "INSERT INTO request_logs (
            timestamp, method, path, request_url, status, duration_ms, proxy_account_id, account_id, error,
            request_headers, response_headers, request_body, response_body, model, input_tokens, output_tokens,
            cache_status, cache_key, cache_eligible, cache_bypass_reason, local_cached_input_tokens, provider_cached_input_tokens
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22)",
        params![
            entry.timestamp,
            entry.method,
            entry.path,
            entry.request_url,
            entry.status as i64,
            entry.duration_ms as i64,
            entry.proxy_account_id,
            entry.account_id,
            entry.error,
            entry.request_headers,
            entry.response_headers,
            entry.request_body,
            entry.response_body,
            entry.model,
            entry.input_tokens,
            entry.output_tokens,
            cache_status,
            cache_key,
            cache_eligible.map(|v| if v { 1_i64 } else { 0_i64 }),
            cache_bypass_reason,
            entry.local_cached_input_tokens,
            provider_cached_input_tokens,
        ],
    )
    .map_err(|e| e.to_string())?;
    if cfg.max_logs > 0 {
        conn.execute(
            "DELETE FROM request_logs WHERE id NOT IN (SELECT id FROM request_logs ORDER BY id DESC LIMIT ?1)",
            params![cfg.max_logs as i64],
        )
        .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn sanitize_headers(headers: &axum::http::HeaderMap) -> Vec<(String, String)> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            let name_str = name.as_str().to_lowercase();
            if matches!(
                name_str.as_str(),
                "authorization" | "x-api-key" | "cookie" | "proxy-authorization"
            ) {
                return None;
            }
            let value_str = value.to_str().unwrap_or("").to_string();
            Some((name.as_str().to_string(), value_str))
        })
        .collect()
}

fn sanitize_reqwest_headers(headers: &reqwest::header::HeaderMap) -> Vec<(String, String)> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            let name_str = name.as_str().to_lowercase();
            if matches!(
                name_str.as_str(),
                "authorization" | "x-api-key" | "cookie" | "proxy-authorization"
            ) {
                return None;
            }
            let value_str = value.to_str().unwrap_or("").to_string();
            Some((name.as_str().to_string(), value_str))
        })
        .collect()
}

fn headers_to_json_string(headers: Vec<(String, String)>) -> Option<String> {
    serde_json::to_string(&headers).ok()
}

const MAX_LOG_BODY_BYTES: usize = 64 * 1024;

fn truncate_body(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return String::new();
    }
    let slice = if bytes.len() > MAX_LOG_BODY_BYTES {
        &bytes[..MAX_LOG_BODY_BYTES]
    } else {
        bytes
    };
    let mut text = String::from_utf8_lossy(slice).to_string();
    if bytes.len() > MAX_LOG_BODY_BYTES {
        text.push_str(&format!(
            "
...truncated {} bytes",
            bytes.len() - MAX_LOG_BODY_BYTES
        ));
    }
    text
}

fn extract_model(body: &[u8]) -> Option<String> {
    if body.is_empty() {
        return None;
    }
    let value: Value = serde_json::from_slice(body).ok()?;
    value.get("model")?.as_str().map(|s| s.to_string())
}

fn canonicalize_json_value(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let mut out = serde_json::Map::new();
            for key in keys {
                out.insert(key.clone(), canonicalize_json_value(&map[key]));
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(canonicalize_json_value).collect()),
        _ => value.clone(),
    }
}

fn request_has_tools(value: &Value) -> bool {
    match value {
        Value::Object(map) => {
            if map.contains_key("tools")
                || map.contains_key("tool_choice")
                || map.contains_key("tool_calls")
                || map.contains_key("tool_use")
                || map.contains_key("tool_result")
                || map.contains_key("parallel_tool_calls")
            {
                return true;
            }
            map.values().any(request_has_tools)
        }
        Value::Array(items) => items.iter().any(request_has_tools),
        _ => false,
    }
}

fn compute_cache_key(namespace: &str, method: &str, path: &str, body: &Value) -> String {
    let canonical = canonicalize_json_value(body);
    let payload = serde_json::json!({
        "namespace": namespace,
        "method": method,
        "path": path,
        "body": canonical,
    });
    let bytes = serde_json::to_vec(&payload).unwrap_or_default();
    let digest = Sha256::digest(&bytes);
    format!("{:x}", digest)
}

fn cache_namespace(path: &str) -> &'static str {
    if path.starts_with("/v1/messages") {
        "anthropic"
    } else if path.starts_with("/v1/chat/completions") {
        "openai-chat"
    } else if path.starts_with("/v1/responses") {
        "responses"
    } else {
        "generic"
    }
}

fn evaluate_local_cache_request(
    method: &str,
    path: &str,
    body_bytes: &[u8],
) -> (bool, Option<String>, Option<String>, Option<String>) {
    if method != "POST" {
        return (false, None, None, Some("method_not_post".to_string()));
    }
    if !(path.starts_with("/v1/messages")
        || path.starts_with("/v1/chat/completions")
        || path.starts_with("/v1/responses"))
        || path.starts_with("/v1/messages/count_tokens")
    {
        return (false, None, None, Some("path_not_supported".to_string()));
    }
    if body_bytes.len() > 256 * 1024 {
        return (false, None, None, Some("request_too_large".to_string()));
    }
    let mut value: Value = match serde_json::from_slice(body_bytes) {
        Ok(v) => v,
        Err(_) => return (false, None, None, Some("invalid_json".to_string())),
    };
    if value
        .get("stream")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        return (false, None, None, Some("streaming_request".to_string()));
    }
    if request_has_tools(&value) {
        return (false, None, None, Some("tool_request".to_string()));
    }
    if value
        .get("temperature")
        .and_then(|v| v.as_f64())
        .map(|v| v > 0.3)
        .unwrap_or(false)
    {
        return (false, None, None, Some("high_temperature".to_string()));
    }
    if let Some(obj) = value.as_object_mut() {
        obj.remove("stream");
        obj.remove("stream_options");
    }
    let model = value
        .get("model")
        .and_then(|v| v.as_str())
        .map(|v| v.to_string());
    let key = compute_cache_key(cache_namespace(path), method, path, &value);
    (true, Some(key), model, None)
}

fn cleanup_expired_ai_cache(
    conn: &Connection,
    now: &str,
    max_entries: usize,
) -> Result<(), String> {
    conn.execute(
        "DELETE FROM ai_cache_entries WHERE expires_at <= ?1",
        params![now],
    )
    .map_err(|e| e.to_string())?;
    if max_entries > 0 {
        conn.execute(
            "DELETE FROM ai_cache_entries
             WHERE id NOT IN (
                SELECT id FROM ai_cache_entries
                ORDER BY last_hit_at DESC, id DESC
                LIMIT ?1
             )",
            params![max_entries as i64],
        )
        .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn lookup_local_cache(cache_key: &str) -> Result<Option<LocalCacheHit>, String> {
    let conn = proxy_log_db()?;
    let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    cleanup_expired_ai_cache(&conn, &now, proxy_config_snapshot().exact_cache_max_entries)?;
    let mut stmt = conn
        .prepare(
            "SELECT id, status, content_type, response_body, model, input_tokens, output_tokens, provider_cached_input_tokens
             FROM ai_cache_entries
             WHERE cache_key = ?1 AND expires_at > ?2",
        )
        .map_err(|e| e.to_string())?;
    let hit = stmt
        .query_row(params![cache_key, now], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)? as u16,
                row.get::<_, String>(2)?,
                row.get::<_, Vec<u8>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, i64>(7)?,
            ))
        })
        .optional()
        .map_err(|e| e.to_string())?;
    let Some((
        id,
        status,
        content_type,
        response_body,
        model,
        input_tokens,
        output_tokens,
        provider_cached_input_tokens,
    )) = hit
    else {
        return Ok(None);
    };
    conn.execute(
        "UPDATE ai_cache_entries SET hit_count = hit_count + 1, last_hit_at = ?2 WHERE id = ?1",
        params![
            id,
            chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
        ],
    )
    .map_err(|e| e.to_string())?;
    Ok(Some(LocalCacheHit {
        cache_key: cache_key.to_string(),
        body: Bytes::from(response_body),
        status,
        content_type,
        model,
        input_tokens,
        output_tokens,
        provider_cached_input_tokens,
    }))
}

fn store_local_cache_entry(
    cache_key: &str,
    method: &str,
    path: &str,
    model: Option<&str>,
    status: u16,
    content_type: &str,
    response_body: &[u8],
    input_tokens: Option<i64>,
    output_tokens: Option<i64>,
    provider_cached_input_tokens: Option<i64>,
) -> Result<(), String> {
    let cfg = proxy_config_snapshot();
    let conn = proxy_log_db()?;
    let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    cleanup_expired_ai_cache(&conn, &now, cfg.exact_cache_max_entries)?;
    let expires_at = (chrono::Utc::now()
        + chrono::Duration::minutes(cfg.exact_cache_ttl_minutes.max(1)))
    .format("%Y-%m-%dT%H:%M:%SZ")
    .to_string();
    conn.execute(
        "INSERT INTO ai_cache_entries (
            cache_key, cache_type, path, method, model, status, content_type, response_body, response_preview,
            input_tokens, output_tokens, local_cached_input_tokens, provider_cached_input_tokens,
            hit_count, created_at, last_hit_at, expires_at
        ) VALUES (?1, 'exact', ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, 0, ?13, ?13, ?14)
        ON CONFLICT(cache_key) DO UPDATE SET
            model = excluded.model,
            status = excluded.status,
            content_type = excluded.content_type,
            response_body = excluded.response_body,
            response_preview = excluded.response_preview,
            input_tokens = excluded.input_tokens,
            output_tokens = excluded.output_tokens,
            local_cached_input_tokens = excluded.local_cached_input_tokens,
            provider_cached_input_tokens = excluded.provider_cached_input_tokens,
            expires_at = excluded.expires_at",
        params![
            cache_key,
            path,
            method,
            model.map(|v| v.to_string()),
            status as i64,
            content_type,
            response_body,
            truncate_body(response_body),
            input_tokens.unwrap_or(0),
            output_tokens.unwrap_or(0),
            input_tokens.unwrap_or(0),
            provider_cached_input_tokens.unwrap_or(0),
            now,
            expires_at,
        ],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

fn extract_usage(body: &[u8]) -> (Option<i64>, Option<i64>) {
    if body.is_empty() {
        return (None, None);
    }
    let value: Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => return (None, None),
    };
    let usage = match value.get("usage") {
        Some(value) => value,
        None => return (None, None),
    };
    let input = usage.get("input_tokens").and_then(|v| v.as_i64());
    let output = usage.get("output_tokens").and_then(|v| v.as_i64());
    (input, output)
}

fn parse_usage_limit_error(body: &[u8]) -> Option<(Option<i64>, Option<i64>)> {
    if body.is_empty() {
        return None;
    }
    let value: Value = serde_json::from_slice(body).ok()?;
    let err = value.get("error")?;
    let err_type = err.get("type").and_then(|v| v.as_str())?;
    if err_type != "usage_limit_reached" {
        return None;
    }
    let resets_at = err.get("resets_at").and_then(|v| v.as_i64());
    let resets_in_seconds = err.get("resets_in_seconds").and_then(|v| v.as_i64());
    Some((resets_at, resets_in_seconds))
}

fn usage_limit_cooldown_until(
    resets_at: Option<i64>,
    resets_in_seconds: Option<i64>,
) -> std::time::Instant {
    let now = chrono::Utc::now().timestamp();
    if let Some(secs) = resets_in_seconds {
        if secs > 0 {
            return std::time::Instant::now() + std::time::Duration::from_secs(secs as u64);
        }
    }
    if let Some(at) = resets_at {
        if at > now {
            let delta = (at - now) as u64;
            return std::time::Instant::now() + std::time::Duration::from_secs(delta);
        }
    }
    std::time::Instant::now() + std::time::Duration::from_secs(COOLDOWN_SECS)
}

fn rough_token_count(text: &str) -> i64 {
    let mut count: i64 = 0;
    let mut in_ascii_word = false;
    for ch in text.chars() {
        if ch.is_whitespace() {
            if in_ascii_word {
                count += 1;
                in_ascii_word = false;
            }
            continue;
        }
        if ch.is_ascii() {
            if ch.is_ascii_punctuation() {
                if in_ascii_word {
                    count += 1;
                    in_ascii_word = false;
                }
                count += 1;
            } else {
                in_ascii_word = true;
            }
        } else {
            if in_ascii_word {
                count += 1;
                in_ascii_word = false;
            }
            count += 1;
        }
    }
    if in_ascii_word {
        count += 1;
    }
    count
}

fn normalize_reasoning_effort(value: &str) -> Option<String> {
    match value.trim() {
        "none" | "low" | "medium" | "high" | "xhigh" => Some(value.trim().to_string()),
        _ => None,
    }
}

fn count_codex_input_tokens(body: &Value) -> i64 {
    let mut segments: Vec<String> = Vec::new();

    if let Some(inst) = body.get("instructions").and_then(|v| v.as_str()) {
        let trimmed = inst.trim();
        if !trimmed.is_empty() {
            segments.push(trimmed.to_string());
        }
    }

    if let Some(items) = body.get("input").and_then(|v| v.as_array()) {
        for item in items {
            let item_type = item.get("type").and_then(|v| v.as_str()).unwrap_or("");
            match item_type {
                "message" => {
                    if let Some(parts) = item.get("content").and_then(|v| v.as_array()) {
                        for part in parts {
                            if let Some(text) = part.get("text").and_then(|v| v.as_str()) {
                                let trimmed = text.trim();
                                if !trimmed.is_empty() {
                                    segments.push(trimmed.to_string());
                                }
                            }
                        }
                    }
                }
                "function_call" => {
                    if let Some(name) = item.get("name").and_then(|v| v.as_str()) {
                        let trimmed = name.trim();
                        if !trimmed.is_empty() {
                            segments.push(trimmed.to_string());
                        }
                    }
                    if let Some(args) = item.get("arguments") {
                        let text = if let Some(s) = args.as_str() {
                            s.to_string()
                        } else {
                            serde_json::to_string(args).unwrap_or_default()
                        };
                        let trimmed = text.trim();
                        if !trimmed.is_empty() {
                            segments.push(trimmed.to_string());
                        }
                    }
                }
                "function_call_output" => {
                    if let Some(out) = item.get("output") {
                        let text = if let Some(s) = out.as_str() {
                            s.to_string()
                        } else {
                            serde_json::to_string(out).unwrap_or_default()
                        };
                        let trimmed = text.trim();
                        if !trimmed.is_empty() {
                            segments.push(trimmed.to_string());
                        }
                    }
                }
                _ => {
                    if let Some(text) = item.get("text").and_then(|v| v.as_str()) {
                        let trimmed = text.trim();
                        if !trimmed.is_empty() {
                            segments.push(trimmed.to_string());
                        }
                    }
                }
            }
        }
    }

    if let Some(tools) = body.get("tools").and_then(|v| v.as_array()) {
        for tool in tools {
            if let Some(name) = tool.get("name").and_then(|v| v.as_str()) {
                let trimmed = name.trim();
                if !trimmed.is_empty() {
                    segments.push(trimmed.to_string());
                }
            }
            if let Some(desc) = tool.get("description").and_then(|v| v.as_str()) {
                let trimmed = desc.trim();
                if !trimmed.is_empty() {
                    segments.push(trimmed.to_string());
                }
            }
            if let Some(params) = tool.get("parameters") {
                let text = if let Some(s) = params.as_str() {
                    s.to_string()
                } else {
                    serde_json::to_string(params).unwrap_or_default()
                };
                let trimmed = text.trim();
                if !trimmed.is_empty() {
                    segments.push(trimmed.to_string());
                }
            }
        }
    }

    if let Some(text) = body.get("text").and_then(|v| v.get("format")) {
        if let Some(name) = text.get("name").and_then(|v| v.as_str()) {
            let trimmed = name.trim();
            if !trimmed.is_empty() {
                segments.push(trimmed.to_string());
            }
        }
        if let Some(schema) = text.get("schema") {
            let text = if let Some(s) = schema.as_str() {
                s.to_string()
            } else {
                serde_json::to_string(schema).unwrap_or_default()
            };
            let trimmed = text.trim();
            if !trimmed.is_empty() {
                segments.push(trimmed.to_string());
            }
        }
    }

    let joined = segments.join("\n");
    if joined.is_empty() {
        return 0;
    }
    rough_token_count(&joined)
}

fn apply_usage_limit_policy(
    state: &Arc<ProxyState>,
    idx: usize,
    id: &str,
    until: std::time::Instant,
) {
    let cfg = proxy_config_snapshot();
    if cfg.disable_on_usage_limit {
        {
            let mut accounts_lock = state.accounts.write().unwrap();
            if let Some(acc) = accounts_lock.get_mut(idx) {
                acc.health = AccountHealth::Blocked;
            }
        }
        if let Err(err) = update_proxy_enabled(id.to_string(), false) {
            log_proxy(&format!("usage-limit disable failed for {id}: {err}"));
        } else {
            log_proxy(&format!("usage-limit disabled account {id}"));
        }
    } else {
        let mut accounts_lock = state.accounts.write().unwrap();
        if let Some(acc) = accounts_lock.get_mut(idx) {
            acc.health = AccountHealth::Cooldown(until);
        }
    }
}

// ─── types ───────────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Clone)]
struct MetaEntry {
    label: Option<String>,
    added_at: u64,
    #[serde(default = "default_proxy_enabled")]
    proxy_enabled: bool,
}

fn default_proxy_enabled() -> bool {
    true
}

#[derive(Serialize, Deserialize, Clone)]
pub struct CodexAccount {
    id: String,
    email: String,
    plan: String,
    user_id: String,
    expires_at: i64,
    last_refresh: Option<String>,
    has_refresh_token: bool,
    openai_api_key: Option<String>,
    label: Option<String>,
    added_at: u64,
    proxy_enabled: bool,
}

// ─── Global State for Proxy Gateway ──────────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
enum AccountHealth {
    Active,
    Cooldown(std::time::Instant), // 429 → cooldown until this instant
    Blocked,                      // 401/403 after refresh attempt
}

#[derive(Clone)]
struct ProxyAccount {
    id: String,
    account_id: Option<String>,
    access_token: String,
    refresh_token: Option<String>,
    health: AccountHealth,
}

struct ProxyState {
    client: reqwest::Client,
    accounts: Arc<RwLock<Vec<ProxyAccount>>>,
    req_counter: AtomicUsize,
}

// Global proxy shutdown sender and live state
static PROXY_SHUTDOWN: Mutex<Option<oneshot::Sender<()>>> = Mutex::new(None);
static PROXY_PORT: Mutex<Option<u16>> = Mutex::new(None);
// Shared live proxy state for status queries and hot-reload
static PROXY_STATE: Mutex<Option<Arc<ProxyState>>> = Mutex::new(None);

// ─── Anthropic proxy globals ─────────────────────────────────────────────────

fn anthropic_config_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
    PathBuf::from(home)
        .join(".codex-manager")
        .join("anthropic_keys.json")
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct AnthropicKeyEntry {
    id: String,
    label: Option<String>,
    // Either a raw sk-ant- API key or a Claude Code OAuth access_token
    key: String,
    added_at: u64,
}

fn load_anthropic_keys() -> Vec<AnthropicKeyEntry> {
    let path = anthropic_config_path();
    if !path.exists() {
        return vec![];
    }
    fs::read_to_string(&path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

fn save_anthropic_keys(keys: &[AnthropicKeyEntry]) {
    let path = anthropic_config_path();
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(s) = serde_json::to_string_pretty(keys) {
        let _ = fs::write(&path, s);
    }
}

#[tauri::command]
fn list_anthropic_keys() -> Result<Vec<AnthropicKeyEntry>, String> {
    Ok(load_anthropic_keys())
}

#[tauri::command]
fn add_anthropic_key(label: Option<String>, key: String) -> Result<AnthropicKeyEntry, String> {
    if key.trim().is_empty() {
        return Err("Key cannot be empty".into());
    }
    let mut keys = load_anthropic_keys();
    let entry = AnthropicKeyEntry {
        id: format!("{}", chrono::Utc::now().timestamp_millis()),
        label,
        key: key.trim().to_string(),
        added_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
    };
    keys.push(entry.clone());
    save_anthropic_keys(&keys);
    Ok(entry)
}

#[tauri::command]
fn delete_anthropic_key(id: String) -> Result<(), String> {
    let mut keys = load_anthropic_keys();
    keys.retain(|k| k.id != id);
    save_anthropic_keys(&keys);
    Ok(())
}

#[tauri::command]
fn update_anthropic_key_label(id: String, label: Option<String>) -> Result<(), String> {
    let mut keys = load_anthropic_keys();
    if let Some(entry) = keys.iter_mut().find(|k| k.id == id) {
        entry.label = label;
        save_anthropic_keys(&keys);
        Ok(())
    } else {
        Err("Key not found".into())
    }
}

// ─── Anthropic protocol (Codex compatibility) ─────────────────────────────────

fn anthropic_system_parts(value: &Value) -> Vec<String> {
    match value {
        Value::String(s) => {
            if s.is_empty() {
                Vec::new()
            } else {
                vec![s.clone()]
            }
        }
        Value::Array(items) => items
            .iter()
            .filter_map(|item| {
                if item.get("type").and_then(|v| v.as_str()) == Some("text") {
                    item.get("text")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                } else {
                    None
                }
            })
            .filter(|s| !s.is_empty())
            .collect(),
        _ => Vec::new(),
    }
}

fn anthropic_image_url(item: &Value) -> Option<String> {
    let source = item.get("source")?;
    let src_type = source.get("type").and_then(|v| v.as_str()).unwrap_or("");
    if src_type == "base64" {
        let data = source
            .get("data")
            .and_then(|v| v.as_str())
            .filter(|v| !v.is_empty())
            .or_else(|| source.get("base64").and_then(|v| v.as_str()));
        if let Some(data) = data {
            let media_type = source
                .get("media_type")
                .and_then(|v| v.as_str())
                .filter(|v| !v.is_empty())
                .or_else(|| source.get("mime_type").and_then(|v| v.as_str()))
                .unwrap_or("application/octet-stream");
            return Some(format!("data:{media_type};base64,{data}"));
        }
    } else if src_type == "url" {
        if let Some(url) = source.get("url").and_then(|v| v.as_str()) {
            if !url.is_empty() {
                return Some(url.to_string());
            }
        }
    }
    None
}

fn anthropic_tool_result_content(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Array(items) => {
            let mut parts = Vec::new();
            for item in items {
                match item.get("type").and_then(|v| v.as_str()).unwrap_or("") {
                    "text" => {
                        if let Some(text) = item.get("text").and_then(|v| v.as_str()) {
                            let trimmed = text.trim();
                            if !trimmed.is_empty() {
                                parts.push(trimmed.to_string());
                            }
                        }
                    }
                    "tool_reference" => {
                        let tool_name = item
                            .get("tool_name")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .trim();
                        if !tool_name.is_empty() {
                            parts.push(format!("[tool reference: {tool_name}]"));
                        }
                    }
                    "image" => {
                        parts.push("[image omitted]".to_string());
                    }
                    _ => {
                        let fallback = serde_json::to_string(item).unwrap_or_default();
                        if !fallback.is_empty() {
                            parts.push(fallback);
                        }
                    }
                }
            }
            if parts.is_empty() {
                serde_json::to_string(value).unwrap_or_default()
            } else {
                parts.join("\n\n")
            }
        }
        Value::Object(obj) => {
            if let Some(text) = obj.get("text").and_then(|v| v.as_str()) {
                text.to_string()
            } else {
                serde_json::to_string(value).unwrap_or_default()
            }
        }
        Value::Null => "".to_string(),
        _ => serde_json::to_string(value).unwrap_or_default(),
    }
}

fn compact_middle_text(text: &str, max_chars: usize, head_chars: usize, tail_chars: usize) -> String {
    if text.len() <= max_chars {
        return text.to_string();
    }
    let head_end = text
        .char_indices()
        .nth(head_chars)
        .map(|(idx, _)| idx)
        .unwrap_or(text.len());
    let tail_start = if tail_chars == 0 {
        text.len()
    } else {
        let total_chars = text.chars().count();
        let skip = total_chars.saturating_sub(tail_chars);
        text.char_indices()
            .nth(skip)
            .map(|(idx, _)| idx)
            .unwrap_or(0)
    };
    let omitted = text.len().saturating_sub(head_end).saturating_sub(text.len().saturating_sub(tail_start));
    format!(
        "{}\n[... truncated {} chars ...]\n{}",
        &text[..head_end],
        omitted,
        &text[tail_start..]
    )
}

fn text_fingerprint(text: &str) -> String {
    let hex = format!("{:x}", Sha256::digest(text.as_bytes()));
    hex[..16.min(hex.len())].to_string()
}

fn extract_system_reminder_tool_name(text: &str, prefix: &str, suffix: &str) -> Option<String> {
    let start = text.find(prefix)? + prefix.len();
    let rest = &text[start..];
    let end = rest.find(suffix)?;
    let name = rest[..end].trim();
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

fn is_noisy_context_tool(tool_name: &str) -> bool {
    let normalized = tool_name.trim().to_ascii_lowercase();
    matches!(
        normalized.as_str(),
        "read" | "exec_command" | "write_stdin" | "open" | "find" | "search_query" | "image_query"
    )
}

fn compact_system_reminder_text(text: &str) -> String {
    let trimmed = text.trim();
    if !trimmed.starts_with("<system-reminder>") || !trimmed.ends_with("</system-reminder>") {
        return text.to_string();
    }

    if trimmed.contains("The following skills are available for use with the Skill tool:") {
        return format!(
            "<system-reminder>\nSkill inventory omitted for context efficiency. Original size: {} chars.\n</system-reminder>",
            trimmed.len()
        );
    }

    if let Some(tool_name) = extract_system_reminder_tool_name(
        trimmed,
        "Result of calling the ",
        " tool:",
    ) {
        let noisy = is_noisy_context_tool(&tool_name);
        let max_chars = if noisy { 2200 } else { 4200 };
        let head_chars = if noisy { 1200 } else { 2400 };
        let tail_chars = if noisy { 500 } else { 900 };
        if trimmed.len() <= max_chars {
            return text.to_string();
        }
        let compacted = compact_middle_text(trimmed, max_chars, head_chars, tail_chars);
        return format!(
            "<system-reminder>\nResult of calling the {tool_name} tool (truncated from {} chars):\n{}\n</system-reminder>",
            trimmed.len(),
            compacted
        );
    }
    if let Some(tool_name) = extract_system_reminder_tool_name(
        trimmed,
        "Called the ",
        " tool with the following input:",
    ) {
        return format!(
            "<system-reminder>\nCalled the {tool_name} tool.\nInput omitted for context efficiency. Original size: {} chars.\n</system-reminder>",
            trimmed.len()
        );
    }

    const MAX_REMINDER_CHARS: usize = 4000;
    const HEAD_CHARS: usize = 2400;
    const TAIL_CHARS: usize = 900;
    if trimmed.len() <= MAX_REMINDER_CHARS {
        return text.to_string();
    }
    compact_middle_text(trimmed, MAX_REMINDER_CHARS, HEAD_CHARS, TAIL_CHARS)
}

fn compact_embedded_system_reminders(text: &str) -> String {
    const START: &str = "<system-reminder>";
    const END: &str = "</system-reminder>";

    let mut output = String::with_capacity(text.len());
    let mut cursor = 0usize;

    while let Some(rel_start) = text[cursor..].find(START) {
        let start = cursor + rel_start;
        output.push_str(&text[cursor..start]);
        if let Some(rel_end) = text[start..].find(END) {
            let end = start + rel_end + END.len();
            output.push_str(&compact_system_reminder_text(&text[start..end]));
            cursor = end;
        } else {
            output.push_str(&text[start..]);
            cursor = text.len();
            break;
        }
    }

    if cursor < text.len() {
        output.push_str(&text[cursor..]);
    }

    output
}

fn compact_anthropic_text_content(text: &str) -> String {
    if text.contains("<system-reminder>") {
        if text.trim().starts_with("<system-reminder>") && text.trim().ends_with("</system-reminder>") {
            compact_system_reminder_text(text)
        } else {
            compact_embedded_system_reminders(text)
        }
    } else {
        text.to_string()
    }
}

fn compact_anthropic_tool_result_content(text: &str) -> String {
    let normalized = compact_embedded_system_reminders(text);
    const MAX_TOOL_RESULT_CHARS: usize = 8000;
    const HEAD_CHARS: usize = 4200;
    const TAIL_CHARS: usize = 1400;
    compact_middle_text(&normalized, MAX_TOOL_RESULT_CHARS, HEAD_CHARS, TAIL_CHARS)
}

fn convert_budget_to_effort(budget: i64) -> Option<&'static str> {
    match budget {
        b if b < -1 => None,
        -1 => Some("auto"),
        0 => Some("none"),
        1..=512 => Some("minimal"),
        513..=1024 => Some("low"),
        1025..=8192 => Some("medium"),
        8193..=24576 => Some("high"),
        _ => Some("xhigh"),
    }
}

fn shorten_name_if_needed(name: &str) -> String {
    const LIMIT: usize = 64;
    if name.len() <= LIMIT {
        return name.to_string();
    }
    if name.starts_with("mcp__") {
        if let Some(idx) = name.rfind("__") {
            let cand = format!("mcp__{}", &name[idx + 2..]);
            if cand.len() > LIMIT {
                return cand[..LIMIT].to_string();
            }
            return cand;
        }
    }
    name[..LIMIT].to_string()
}

fn build_short_name_map(names: &[String]) -> HashMap<String, String> {
    use std::collections::HashSet;
    const LIMIT: usize = 64;
    let mut used: HashSet<String> = HashSet::new();
    let mut map = HashMap::new();

    let base_candidate = |n: &str| -> String {
        if n.len() <= LIMIT {
            return n.to_string();
        }
        if n.starts_with("mcp__") {
            if let Some(idx) = n.rfind("__") {
                let mut cand = format!("mcp__{}", &n[idx + 2..]);
                if cand.len() > LIMIT {
                    cand = cand[..LIMIT].to_string();
                }
                return cand;
            }
        }
        n[..LIMIT].to_string()
    };

    let make_unique = |cand: &str, used: &HashSet<String>| -> String {
        if !used.contains(cand) {
            return cand.to_string();
        }
        let base = cand.to_string();
        for i in 1.. {
            let suffix = format!("_{i}");
            let allowed = LIMIT.saturating_sub(suffix.len());
            let mut tmp = base.clone();
            if tmp.len() > allowed {
                tmp.truncate(allowed);
            }
            tmp.push_str(&suffix);
            if !used.contains(&tmp) {
                return tmp;
            }
        }
        base
    };

    for name in names {
        let cand = base_candidate(name);
        let uniq = make_unique(&cand, &used);
        used.insert(uniq.clone());
        map.insert(name.clone(), uniq);
    }
    map
}

fn normalize_tool_schema_type(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::String(s)) if !s.trim().is_empty() => Some(s.trim().to_string()),
        Some(Value::Array(items)) => items.iter().find_map(|item| {
            let ty = item.as_str()?.trim();
            if ty.is_empty() || ty == "null" {
                None
            } else {
                Some(ty.to_string())
            }
        }),
        _ => None,
    }
}

fn normalize_tool_parameters(value: &Value) -> Value {
    fn normalize_schema(value: &Value) -> Value {
        let mut schema = match value {
            Value::Object(obj) => Value::Object(obj.clone()),
            _ => serde_json::json!({}),
        };

        let inferred_type = normalize_tool_schema_type(schema.get("type")).or_else(|| {
            if schema.get("properties").is_some() || schema.get("required").is_some() {
                Some("object".to_string())
            } else if schema.get("items").is_some() {
                Some("array".to_string())
            } else {
                None
            }
        });

        if let Some(obj) = schema.as_object_mut() {
            obj.remove("$schema");

            if let Some(schema_type) = inferred_type {
                obj.insert("type".to_string(), Value::String(schema_type.clone()));

                if schema_type == "object" {
                    let properties = match obj.remove("properties") {
                        Some(Value::Object(props)) => {
                            let mut normalized = serde_json::Map::new();
                            for (key, prop_schema) in props {
                                normalized.insert(key, normalize_schema(&prop_schema));
                            }
                            Value::Object(normalized)
                        }
                        _ => serde_json::json!({}),
                    };
                    obj.insert("properties".to_string(), properties);

                    if let Some(required) = obj.remove("required") {
                        if let Value::Array(items) = required {
                            let filtered: Vec<Value> = items
                                .into_iter()
                                .filter_map(|item| {
                                    item.as_str().map(|s| Value::String(s.to_string()))
                                })
                                .collect();
                            if !filtered.is_empty() {
                                obj.insert("required".to_string(), Value::Array(filtered));
                            }
                        }
                    }

                    if let Some(additional) = obj.remove("additionalProperties") {
                        let normalized = match additional {
                            Value::Object(_) => normalize_schema(&additional),
                            Value::Bool(flag) => Value::Bool(flag),
                            _ => Value::Bool(false),
                        };
                        obj.insert("additionalProperties".to_string(), normalized);
                    }
                } else {
                    obj.remove("properties");
                    obj.remove("required");
                    obj.remove("additionalProperties");
                }

                if schema_type == "array" {
                    let items = obj
                        .remove("items")
                        .map(|items| normalize_schema(&items))
                        .unwrap_or_else(|| serde_json::json!({}));
                    obj.insert("items".to_string(), items);
                } else {
                    obj.remove("items");
                }
            } else {
                obj.insert("type".to_string(), Value::String("object".to_string()));
                obj.insert("properties".to_string(), serde_json::json!({}));
                obj.remove("required");
                obj.remove("additionalProperties");
                obj.remove("items");
            }

            for key in ["anyOf", "oneOf", "allOf"] {
                if let Some(Value::Array(items)) = obj.remove(key) {
                    let normalized_items: Vec<Value> = items
                        .into_iter()
                        .map(|item| normalize_schema(&item))
                        .collect();
                    if !normalized_items.is_empty() {
                        obj.insert(key.to_string(), Value::Array(normalized_items));
                    }
                }
            }
        }

        schema
    }

    normalize_schema(value)
}

fn build_reverse_tool_map(original: &Value) -> HashMap<String, String> {
    let tools = original
        .get("tools")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let mut names = Vec::new();
    for tool in &tools {
        if let Some(name) = tool.get("name").and_then(|v| v.as_str()) {
            if !name.is_empty() {
                names.push(name.to_string());
            }
        }
    }
    let short_map = build_short_name_map(&names);
    let mut reverse = HashMap::new();
    for (orig, short) in short_map {
        reverse.insert(short, orig);
    }
    reverse
}

fn normalize_openai_tool_choice_for_chat(value: &Value) -> Option<Value> {
    match value {
        Value::String(s) => {
            let normalized = s.trim().to_ascii_lowercase();
            match normalized.as_str() {
                "auto" | "none" | "required" => Some(Value::String(normalized)),
                _ => None,
            }
        }
        Value::Object(obj) => {
            let choice_type = obj
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim()
                .to_ascii_lowercase();
            match choice_type.as_str() {
                "function" => {
                    let name = obj
                        .get("function")
                        .and_then(|v| v.get("name"))
                        .and_then(|v| v.as_str())
                        .or_else(|| obj.get("name").and_then(|v| v.as_str()))
                        .map(|v| v.trim())
                        .filter(|v| !v.is_empty())?;
                    Some(serde_json::json!({
                        "type": "function",
                        "function": { "name": name },
                    }))
                }
                "auto" | "none" | "required" => Some(Value::String(choice_type)),
                _ => None,
            }
        }
        _ => None,
    }
}

fn convert_claude_tool_choice(
    value: &Value,
    short_map: &HashMap<String, String>,
) -> Option<(Value, Option<bool>)> {
    let Value::Object(obj) = value else {
        return None;
    };
    let disable_parallel = obj
        .get("disable_parallel_tool_use")
        .and_then(|v| v.as_bool());
    let choice = match obj.get("type").and_then(|v| v.as_str()).unwrap_or("") {
        "auto" => Value::String("auto".to_string()),
        "any" => Value::String("required".to_string()),
        "none" => Value::String("none".to_string()),
        "tool" => {
            let raw_name = obj
                .get("name")
                .and_then(|v| v.as_str())
                .map(|v| v.trim())
                .filter(|v| !v.is_empty())?;
            let mapped_name = short_map
                .get(raw_name)
                .cloned()
                .unwrap_or_else(|| shorten_name_if_needed(raw_name));
            serde_json::json!({
                "type": "function",
                "name": mapped_name,
            })
        }
        _ => return None,
    };
    Some((choice, disable_parallel))
}

fn convert_claude_to_codex(body: &Value) -> Result<(Value, HashMap<String, String>, bool), String> {
    let mut model = body
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or("gpt-4o-mini")
        .to_string();
    let cfg = proxy_config_snapshot();
    if let Some(override_model) = cfg.model_override.as_ref() {
        let trimmed = override_model.trim();
        if !trimmed.is_empty() {
            model = trimmed.to_string();
        }
    }
    let mut template = serde_json::json!({
        "model": model,
        "instructions": "",
        "input": [],
    });

    let reverse_map = build_reverse_tool_map(body);
    let mut short_map = HashMap::new();
    if let Some(tools) = body.get("tools").and_then(|v| v.as_array()) {
        let mut names = Vec::new();
        for tool in tools {
            if let Some(name) = tool.get("name").and_then(|v| v.as_str()) {
                if !name.is_empty() {
                    names.push(name.to_string());
                }
            }
        }
        short_map = build_short_name_map(&names);
    }

    let mut system_parts = if let Some(system) = body.get("system") {
        anthropic_system_parts(system)
    } else {
        Vec::new()
    };
    if system_parts.first().map(|s| s.as_str()) != Some(CLAUDE_CODE_SYSTEM_PROMPT) {
        system_parts.insert(0, CLAUDE_CODE_SYSTEM_PROMPT.to_string());
    }
    {
        let mut seen_system_parts = HashSet::new();
        system_parts.retain(|part| seen_system_parts.insert(part.clone()));
    }
    if !system_parts.is_empty() {
        let mut msg = serde_json::json!({
            "type": "message",
            "role": "developer",
            "content": []
        });
        let mut content = Vec::new();
        for part in system_parts {
            content.push(serde_json::json!({
                "type": "input_text",
                "text": part
            }));
        }
        msg["content"] = Value::Array(content);
        template["input"].as_array_mut().unwrap().push(msg);
    }

    if let Some(messages) = body.get("messages").and_then(|v| v.as_array()) {
        let mut seen_reminder_fingerprints: HashSet<String> = HashSet::new();
        let mut seen_tool_output_fingerprints: HashSet<String> = HashSet::new();
        for msg in messages {
            let role = msg.get("role").and_then(|v| v.as_str()).unwrap_or("user");
            let mut message = serde_json::json!({
                "type": "message",
                "role": role,
                "content": []
            });
            let mut has_content = false;

            let mut flush_message =
                |template: &mut Value, message: &mut Value, has_content: &mut bool| {
                    if *has_content {
                        if let Some(arr) = template["input"].as_array_mut() {
                            arr.push(message.clone());
                        }
                        message["content"] = Value::Array(vec![]);
                        *has_content = false;
                    }
                };

            let mut append_text = |text: &str, message: &mut Value, has_content: &mut bool| {
                let normalized_text = compact_anthropic_text_content(text);
                if normalized_text.is_empty() {
                    return;
                }
                if normalized_text.contains("<system-reminder>") {
                    let fingerprint = text_fingerprint(&normalized_text);
                    if !seen_reminder_fingerprints.insert(fingerprint) {
                        return;
                    }
                }
                let part_type = if role == "assistant" {
                    "output_text"
                } else {
                    "input_text"
                };
                if let Some(arr) = message["content"].as_array_mut() {
                    arr.push(serde_json::json!({ "type": part_type, "text": normalized_text }));
                }
                *has_content = true;
            };

            let mut append_image = |url: &str, message: &mut Value, has_content: &mut bool| {
                if let Some(arr) = message["content"].as_array_mut() {
                    arr.push(serde_json::json!({ "type": "input_image", "image_url": url }));
                }
                *has_content = true;
            };

            let content_value = msg.get("content").unwrap_or(&Value::Null);
            match content_value {
                Value::Array(items) => {
                    for item in items {
                        let kind = item.get("type").and_then(|v| v.as_str()).unwrap_or("");
                        match kind {
                            "text" => {
                                if let Some(text) = item.get("text").and_then(|v| v.as_str()) {
                                    if !text.is_empty() {
                                        append_text(text, &mut message, &mut has_content);
                                    }
                                }
                            }
                            "image" => {
                                if let Some(url) = anthropic_image_url(item) {
                                    append_image(&url, &mut message, &mut has_content);
                                }
                            }
                            "tool_use" => {
                                let id = item
                                    .get("id")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string();
                                let name_raw = item
                                    .get("name")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string();
                                if id.is_empty() || name_raw.is_empty() {
                                    continue;
                                }
                                flush_message(&mut template, &mut message, &mut has_content);
                                let mut name = name_raw;
                                if let Some(short) = short_map.get(&name) {
                                    name = short.clone();
                                } else {
                                    name = shorten_name_if_needed(&name);
                                }
                                let input = item.get("input").cloned().unwrap_or(Value::Null);
                                let args_string = if input.is_null() {
                                    "{}".to_string()
                                } else {
                                    serde_json::to_string(&input)
                                        .unwrap_or_else(|_| "{}".to_string())
                                };
                                let mut fn_call = serde_json::json!({
                                    "type": "function_call",
                                    "call_id": id,
                                    "name": name,
                                    "arguments": args_string,
                                });
                                if let Some(arr) = template["input"].as_array_mut() {
                                    arr.push(fn_call.take());
                                }
                            }
                            "tool_result" => {
                                let tool_use_id = item
                                    .get("tool_use_id")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string();
                                if tool_use_id.is_empty() {
                                    continue;
                                }
                                flush_message(&mut template, &mut message, &mut has_content);
                                let content = item.get("content").unwrap_or(&Value::Null);
                                let mut output_string =
                                    compact_anthropic_tool_result_content(&anthropic_tool_result_content(content));
                                if output_string.is_empty() {
                                    continue;
                                }
                                if output_string.len() > 512 {
                                    let fingerprint = text_fingerprint(&output_string);
                                    if !seen_tool_output_fingerprints.insert(fingerprint) {
                                        output_string = format!(
                                            "[duplicate tool output omitted; identical content already provided, original size {} chars]",
                                            output_string.len()
                                        );
                                    }
                                }
                                let mut fn_out = serde_json::json!({
                                    "type": "function_call_output",
                                    "call_id": tool_use_id,
                                    "output": output_string,
                                });
                                if let Some(arr) = template["input"].as_array_mut() {
                                    arr.push(fn_out.take());
                                }
                            }
                            _ => {}
                        }
                    }
                    flush_message(&mut template, &mut message, &mut has_content);
                }
                Value::String(text) => {
                    if !text.is_empty() {
                        append_text(text, &mut message, &mut has_content);
                    }
                    flush_message(&mut template, &mut message, &mut has_content);
                }
                _ => {
                    flush_message(&mut template, &mut message, &mut has_content);
                }
            }
        }
    }

    let mut parallel_tool_calls = true;
    if let Some(tools) = body.get("tools").and_then(|v| v.as_array()) {
        let mut out_tools: Vec<Value> = Vec::new();
        for tool in tools {
            if tool.get("type").and_then(|v| v.as_str()) == Some("web_search_20250305") {
                out_tools.push(serde_json::json!({ "type": "web_search" }));
                continue;
            }
            let mut name = tool
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if let Some(short) = short_map.get(&name) {
                name = short.clone();
            } else if !name.is_empty() {
                name = shorten_name_if_needed(&name);
            }
            let desc = tool
                .get("description")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let params =
                normalize_tool_parameters(tool.get("input_schema").unwrap_or(&Value::Null));
            let mut entry = serde_json::json!({
                "type": "function",
                "name": name,
                "description": desc,
                "parameters": params,
                "strict": false
            });
            if let Some(obj) = entry.as_object_mut() {
                if let Some(params_obj) = obj.get_mut("parameters").and_then(|v| v.as_object_mut())
                {
                    params_obj.remove("$schema");
                }
            }
            out_tools.push(entry);
        }
        if !out_tools.is_empty() {
            template["tools"] = Value::Array(out_tools);
            template["tool_choice"] = serde_json::json!("auto");
        }
    }

    if let Some(tool_choice) = body.get("tool_choice") {
        if let Some((choice, disable_parallel)) =
            convert_claude_tool_choice(tool_choice, &short_map)
        {
            template["tool_choice"] = choice;
            if disable_parallel == Some(true) {
                parallel_tool_calls = false;
            }
        }
    }

    template["parallel_tool_calls"] = serde_json::json!(parallel_tool_calls);

    let mut reasoning_effort = "medium".to_string();
    if let Some(thinking) = body.get("thinking") {
        if let Some(t_type) = thinking.get("type").and_then(|v| v.as_str()) {
            match t_type {
                "enabled" => {
                    if let Some(budget) = thinking.get("budget_tokens").and_then(|v| v.as_i64()) {
                        if let Some(level) = convert_budget_to_effort(budget) {
                            reasoning_effort = level.to_string();
                        }
                    }
                }
                "adaptive" => reasoning_effort = "xhigh".to_string(),
                "disabled" => reasoning_effort = "none".to_string(),
                _ => {}
            }
        }
    }
    if let Some(override_effort) = cfg.reasoning_effort_override.as_ref() {
        if let Some(normalized) = normalize_reasoning_effort(override_effort) {
            reasoning_effort = normalized;
        }
    }
    let model_lc = model.to_lowercase();
    if reasoning_effort == "minimal" {
        reasoning_effort = "low".to_string();
    }
    if reasoning_effort == "xhigh" && model_lc.starts_with("gpt-5.1") {
        reasoning_effort = "high".to_string();
    }
    template["reasoning"] = serde_json::json!({
        "effort": reasoning_effort,
        "summary": "auto"
    });

    let stream = body
        .get("stream")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    template["stream"] = serde_json::json!(stream);
    template["store"] = serde_json::json!(false);
    template["include"] = serde_json::json!(["reasoning.encrypted_content"]);

    Ok((template, reverse_map, stream))
}

fn extract_responses_usage(value: &Value) -> (i64, i64, i64) {
    let input_tokens = value
        .get("input_tokens")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let output_tokens = value
        .get("output_tokens")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let cached_tokens = value
        .get("input_tokens_details")
        .and_then(|v| v.get("cached_tokens"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let adjusted_input = if cached_tokens > 0 && input_tokens >= cached_tokens {
        input_tokens - cached_tokens
    } else {
        input_tokens
    };
    (adjusted_input, output_tokens, cached_tokens)
}

fn synth_response_id(prefix: &str) -> String {
    format!(
        "{prefix}_{:x}_{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default(),
        PROXY_REQ_ID.fetch_add(1, Ordering::SeqCst)
    )
}

fn response_message_text(content: &Value) -> String {
    match content {
        Value::String(text) => text.clone(),
        Value::Array(items) => {
            let mut out = String::new();
            for item in items {
                if let Some(text) = item.get("text").and_then(|v| v.as_str()) {
                    out.push_str(text);
                } else if let Some(text) = item.as_str() {
                    out.push_str(text);
                }
            }
            out
        }
        _ => String::new(),
    }
}

fn response_reasoning_text(value: &Value) -> String {
    if let Some(text) = value.get("reasoning_content").and_then(|v| v.as_str()) {
        return text.to_string();
    }
    if let Some(text) = value.get("reasoning").and_then(|v| v.as_str()) {
        return text.to_string();
    }
    String::new()
}

fn openai_chat_usage(
    value: Option<&Value>,
) -> (Option<i64>, Option<i64>, Option<i64>, Option<i64>) {
    let Some(usage) = value else {
        return (None, None, None, None);
    };
    let input = usage
        .get("prompt_tokens")
        .or_else(|| usage.get("input_tokens"))
        .and_then(|v| v.as_i64());
    let cached = usage
        .get("prompt_tokens_details")
        .and_then(|v| v.get("cached_tokens"))
        .or_else(|| {
            usage
                .get("input_tokens_details")
                .and_then(|v| v.get("cached_tokens"))
        })
        .and_then(|v| v.as_i64());
    let output = usage
        .get("completion_tokens")
        .or_else(|| usage.get("output_tokens"))
        .and_then(|v| v.as_i64());
    let reasoning = usage
        .get("completion_tokens_details")
        .and_then(|v| v.get("reasoning_tokens"))
        .or_else(|| {
            usage
                .get("output_tokens_details")
                .and_then(|v| v.get("reasoning_tokens"))
        })
        .and_then(|v| v.as_i64());
    (input, output, cached, reasoning)
}

fn normalize_tool_arguments_string(value: &Value) -> String {
    match value {
        Value::String(s) => {
            if s.trim().is_empty() {
                "{}".to_string()
            } else {
                repair_tool_arguments_json(s).unwrap_or_else(|| s.clone())
            }
        }
        Value::Object(_) | Value::Array(_) => {
            serde_json::to_string(value).unwrap_or_else(|_| "{}".to_string())
        }
        Value::Null => "{}".to_string(),
        _ => value.to_string(),
    }
}

fn repair_json_like_quotes(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut in_double = false;
    let mut in_single = false;
    let mut escaped = false;

    for ch in input.chars() {
        if in_double {
            out.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_double = false;
            }
            continue;
        }

        if in_single {
            if escaped {
                escaped = false;
                match ch {
                    'n' | 'r' | 't' | 'b' | 'f' | '/' | '"' => {
                        out.push('\\');
                        out.push(ch);
                    }
                    '\\' => {
                        out.push('\\');
                        out.push('\\');
                    }
                    '\'' => out.push('\''),
                    'u' => {
                        out.push('\\');
                        out.push('u');
                    }
                    _ => {
                        out.push('\\');
                        out.push(ch);
                    }
                }
                continue;
            }

            match ch {
                '\\' => {
                    escaped = true;
                }
                '\'' => {
                    out.push('"');
                    in_single = false;
                }
                '"' => {
                    out.push('\\');
                    out.push('"');
                }
                _ => out.push(ch),
            }
            continue;
        }

        match ch {
            '"' => {
                in_double = true;
                out.push(ch);
            }
            '\'' => {
                in_single = true;
                out.push('"');
            }
            _ => out.push(ch),
        }
    }

    if in_single {
        out.push('"');
    }

    out
}

fn extract_balanced_json_object(input: &str) -> Option<String> {
    let bytes = input.as_bytes();
    let mut start = None;
    let mut depth = 0_i32;
    let mut in_string = false;
    let mut escaped = false;
    let mut candidates: Vec<String> = Vec::new();

    for (idx, byte) in bytes.iter().enumerate() {
        let ch = *byte as char;

        if in_string {
            if escaped {
                escaped = false;
                continue;
            }
            if ch == '\\' {
                escaped = true;
                continue;
            }
            if ch == '"' {
                in_string = false;
            }
            continue;
        }

        match ch {
            '"' => in_string = true,
            '{' => {
                if depth == 0 {
                    start = Some(idx);
                }
                depth += 1;
            }
            '}' => {
                if depth > 0 {
                    depth -= 1;
                    if depth == 0 {
                        if let Some(begin) = start.take() {
                            if let Some(slice) = input.get(begin..=idx) {
                                if let Ok(Value::Object(_)) = serde_json::from_str::<Value>(slice) {
                                    candidates.push(slice.to_string());
                                }
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }

    candidates
        .into_iter()
        .max_by_key(|candidate| candidate.len())
}

fn extract_any_valid_json_object(input: &str) -> Option<String> {
    let bytes = input.as_bytes();
    let mut best: Option<(usize, String)> = None;
    let mut starts = Vec::new();

    for (idx, byte) in bytes.iter().enumerate() {
        if *byte == b'{' {
            starts.push(idx);
        }
    }

    for start in starts {
        for end in (start + 1)..bytes.len() {
            if bytes[end] != b'}' {
                continue;
            }
            let Some(slice) = input.get(start..=end) else {
                continue;
            };
            let Ok(Value::Object(_)) = serde_json::from_str::<Value>(slice) else {
                continue;
            };
            let replace = match best.as_ref() {
                Some((best_start, best_slice)) => {
                    start > *best_start || (start == *best_start && slice.len() > best_slice.len())
                }
                None => true,
            };
            if replace {
                best = Some((start, slice.to_string()));
            }
        }
    }

    best.map(|(_, slice)| slice)
}

fn parse_tagged_tool_arguments(args: &str) -> Option<String> {
    let trimmed = args.trim();
    if !trimmed.contains("<tool_call>") || !trimmed.contains("</tool_call>") {
        return None;
    }
    let start = trimmed.find("<tool_call>")? + "<tool_call>".len();
    let end = trimmed.rfind("</tool_call>")?;
    if end < start {
        return None;
    }
    let inner = &trimmed[start..end];
    if !inner.contains("<arg_key>") {
        return Some("{}".to_string());
    }

    let mut output = serde_json::Map::new();
    let mut cursor = inner;
    loop {
        let Some(key_start) = cursor.find("<arg_key>") else {
            break;
        };
        let key_body = &cursor[key_start + "<arg_key>".len()..];
        let key_end = key_body.find("</arg_key>")?;
        let key = key_body[..key_end].trim();
        if key.is_empty() {
            return None;
        }
        let value_body = &key_body[key_end + "</arg_key>".len()..];
        let value_start = value_body.find("<arg_value>")?;
        let value_body = &value_body[value_start + "<arg_value>".len()..];
        let value_end = value_body.find("</arg_value>")?;
        let value = &value_body[..value_end];
        output.insert(key.to_string(), Value::String(value.to_string()));
        cursor = &value_body[value_end + "</arg_value>".len()..];
    }

    if output.is_empty() {
        return Some("{}".to_string());
    }
    serde_json::to_string(&Value::Object(output)).ok()
}

fn repair_tool_arguments_json(args: &str) -> Option<String> {
    let trimmed = args.trim();
    if trimmed.is_empty() {
        return Some("{}".to_string());
    }
    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        return match value {
            Value::Object(_) => serde_json::to_string(&value).ok(),
            _ => None,
        };
    }

    let repaired = repair_json_like_quotes(trimmed);
    match serde_json::from_str::<Value>(&repaired) {
        Ok(Value::Object(_)) => Some(repaired),
        _ => extract_balanced_json_object(trimmed)
            .or_else(|| extract_balanced_json_object(&repaired))
            .or_else(|| extract_any_valid_json_object(trimmed))
            .or_else(|| extract_any_valid_json_object(&repaired)),
    }
    .or_else(|| parse_tagged_tool_arguments(trimmed))
}

fn parse_tool_arguments_json(args: &str) -> Result<Value, String> {
    let repaired = repair_tool_arguments_json(args).ok_or_else(|| {
        format!(
            "invalid tool arguments JSON: raw={}",
            truncate_body(args.trim().as_bytes())
        )
    })?;
    serde_json::from_str::<Value>(&repaired).map_err(|e| {
        format!(
            "invalid tool arguments JSON: {e}; raw={}",
            truncate_body(repaired.as_bytes())
        )
    })
}

fn normalize_chat_completion_tool_arguments_value(root: &mut Value) -> bool {
    let mut changed = false;
    if let Some(choices) = root.get_mut("choices").and_then(|v| v.as_array_mut()) {
        for choice in choices {
            if let Some(tool_calls) = choice
                .get_mut("message")
                .and_then(|v| v.get_mut("tool_calls"))
                .and_then(|v| v.as_array_mut())
            {
                for tool_call in tool_calls {
                    if let Some(arguments_value) = tool_call
                        .get_mut("function")
                        .and_then(|v| v.get_mut("arguments"))
                    {
                        if let Some(arguments) = arguments_value.as_str() {
                            if let Some(repaired) = repair_tool_arguments_json(arguments) {
                                if repaired != arguments {
                                    *arguments_value = Value::String(repaired);
                                    changed = true;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    changed
}

fn normalize_chat_completion_tool_arguments_bytes(body_bytes: &[u8]) -> Option<Bytes> {
    let mut root: Value = serde_json::from_slice(body_bytes).ok()?;
    if !normalize_chat_completion_tool_arguments_value(&mut root) {
        return None;
    }
    serde_json::to_vec(&root).ok().map(Bytes::from)
}

fn maybe_normalize_chat_completion_response_bytes(
    upstream_path: &str,
    status: reqwest::StatusCode,
    body_bytes: &[u8],
) -> Bytes {
    let is_chat_path =
        upstream_path == "/v1/chat/completions" || upstream_path.starts_with("/v1/chat/completions?");
    if status.is_success() && is_chat_path {
        normalize_chat_completion_tool_arguments_bytes(body_bytes)
            .unwrap_or_else(|| Bytes::copy_from_slice(body_bytes))
    } else {
        Bytes::copy_from_slice(body_bytes)
    }
}

fn build_local_models_response(models: &[String]) -> Bytes {
    let data: Vec<Value> = models
        .iter()
        .map(|model| {
            serde_json::json!({
                "id": model,
                "object": "model",
                "created": 0,
                "owned_by": "custom-openai"
            })
        })
        .collect();
    serde_json::to_vec(&serde_json::json!({
        "object": "list",
        "data": data,
    }))
    .unwrap_or_else(|_| b"{\"object\":\"list\",\"data\":[]}".to_vec())
    .into()
}

fn configured_proxy_models(cfg: &ProxyConfig) -> Vec<String> {
    cfg.model_override
        .as_ref()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .map(|v| vec![v])
        .unwrap_or_default()
}

fn build_probe_check(
    name: &str,
    method: &str,
    path: &str,
    status: u16,
    ok: bool,
    duration_ms: u64,
    summary: String,
    response_excerpt: Option<String>,
) -> OpenAICompatProbeCheck {
    OpenAICompatProbeCheck {
        name: name.to_string(),
        method: method.to_string(),
        path: path.to_string(),
        status,
        ok,
        duration_ms,
        summary,
        response_excerpt,
    }
}

fn parse_probe_chat_tool_call(root: &Value) -> (bool, String) {
    let Some(tool_call) = root
        .get("choices")
        .and_then(|v| v.as_array())
        .and_then(|choices| choices.first())
        .and_then(|choice| choice.get("message"))
        .and_then(|message| message.get("tool_calls"))
        .and_then(|v| v.as_array())
        .and_then(|tool_calls| tool_calls.first())
    else {
        return (false, "未返回 tool_calls".to_string());
    };
    let name = tool_call
        .get("function")
        .and_then(|v| v.get("name"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let arguments = tool_call
        .get("function")
        .and_then(|v| v.get("arguments"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let args_ok = repair_tool_arguments_json(arguments).is_some();
    (
        !name.is_empty() && args_ok,
        format!(
            "name={name} args_json={} finish_reason={}",
            args_ok,
            root.get("choices")
                .and_then(|v| v.as_array())
                .and_then(|choices| choices.first())
                .and_then(|choice| choice.get("finish_reason"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
        ),
    )
}

fn parse_probe_responses_tool_call(root: &Value) -> (bool, String) {
    let Some(tool_call) = root
        .get("output")
        .and_then(|v| v.as_array())
        .and_then(|items| {
            items
                .iter()
                .find(|item| item.get("type").and_then(|v| v.as_str()) == Some("function_call"))
        })
    else {
        return (false, "未返回 function_call".to_string());
    };
    let name = tool_call.get("name").and_then(|v| v.as_str()).unwrap_or("");
    let arguments = tool_call
        .get("arguments")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let args_ok = repair_tool_arguments_json(arguments).is_some();
    (
        !name.is_empty() && args_ok,
        format!(
            "name={name} args_json={} raw={}",
            args_ok,
            truncate_body(arguments.as_bytes())
        ),
    )
}

fn parse_probe_chat_stream_tool_call(body: &str) -> (bool, String) {
    let mut call_name = String::new();
    let mut call_args = String::new();
    for block in body.split("\n\n") {
        let mut data_lines = Vec::new();
        for line in block.lines() {
            if let Some(rest) = line.strip_prefix("data:") {
                data_lines.push(rest.trim().to_string());
            }
        }
        if data_lines.is_empty() {
            continue;
        }
        let payload = data_lines.join("\n");
        if payload == "[DONE]" {
            continue;
        }
        let Ok(root) = serde_json::from_str::<Value>(&payload) else {
            continue;
        };
        if let Some(tool_calls) = root
            .get("choices")
            .and_then(|v| v.as_array())
            .and_then(|choices| choices.first())
            .and_then(|choice| choice.get("delta"))
            .and_then(|delta| delta.get("tool_calls"))
            .and_then(|v| v.as_array())
        {
            for tool_call in tool_calls {
                if let Some(name) = tool_call
                    .get("function")
                    .and_then(|v| v.get("name"))
                    .and_then(|v| v.as_str())
                {
                    if !name.is_empty() {
                        call_name = name.to_string();
                    }
                }
                if let Some(arguments) = tool_call
                    .get("function")
                    .and_then(|v| v.get("arguments"))
                    .and_then(|v| v.as_str())
                {
                    let (merged, _) = merge_custom_tool_arguments(&call_args, arguments);
                    call_args = merged;
                }
            }
        }
    }
    let args_ok = repair_tool_arguments_json(&call_args).is_some();
    (
        !call_name.is_empty() && args_ok,
        format!(
            "name={call_name} args_json={} raw={}",
            args_ok,
            truncate_body(call_args.as_bytes())
        ),
    )
}

async fn run_smoke_chat_tool_case(
    client: &reqwest::Client,
    chat_url: &str,
    config: &OpenAICompatProviderConfig,
    model: &str,
    tool_name: &str,
    prompt: &str,
    description: &str,
    parameters: Value,
    tool_choice: Value,
) -> Result<Value, String> {
    let mut request = serde_json::json!({
        "model": model,
        "messages": [
            { "role": "system", "content": "You are Codex, a coding agent. Prefer using tools over describing what you would do." },
            { "role": "user", "content": prompt }
        ],
        "tools": [{
            "type": "function",
            "function": {
                "name": tool_name,
                "description": description,
                "parameters": parameters,
            }
        }],
        "tool_choice": tool_choice,
    });
    let _ = maybe_inject_glm_coding_agent_bias(config, Some(model), &mut request);
    let resp = client
        .post(chat_url)
        .bearer_auth(&config.api_key)
        .json(&request)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let status = resp.status().as_u16();
    let bytes = resp.bytes().await.map_err(|e| e.to_string())?;
    let root: Value = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
    let (ok, summary) = if let Some(tool_call) = root
        .get("choices")
        .and_then(|v| v.as_array())
        .and_then(|choices| choices.first())
        .and_then(|choice| choice.get("message"))
        .and_then(|message| message.get("tool_calls"))
        .and_then(|v| v.as_array())
        .and_then(|tool_calls| tool_calls.first())
    {
        let name = tool_call
            .get("function")
            .and_then(|v| v.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let arguments = tool_call
            .get("function")
            .and_then(|v| v.get("arguments"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let args_ok = repair_tool_arguments_json(arguments).is_some();
        (
            name == tool_name && args_ok,
            format!(
                "name={name} args_json={} raw={}",
                args_ok,
                truncate_body(arguments.as_bytes())
            ),
        )
    } else {
        (false, "未返回 tool_calls".to_string())
    };
    Ok(serde_json::json!({
        "status": status,
        "ok": ok,
        "summary": summary,
        "excerpt": truncate_body(&bytes),
    }))
}

async fn probe_openai_compat_upstream(
    config: &OpenAICompatProviderConfig,
) -> Result<OpenAICompatProbeResult, String> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(45))
        .build()
        .map_err(|e| e.to_string())?;
    let requested_model = config
        .default_model
        .clone()
        .or_else(|| config.model_mappings.first().map(|item| item.alias.clone()))
        .unwrap_or_else(|| "glm-5".to_string());
    let effective_model = map_openai_compat_model(config, Some(&requested_model))
        .unwrap_or_else(|| requested_model.clone());
    let mut checks = Vec::new();

    let probe_base_url = config.base_url.clone();
    let probe_api_key = config.api_key.clone();
    let probe_client = client.clone();
    let run_json_request =
        move |method: reqwest::Method, path: &'static str, body: Option<Value>, anthropic: bool| {
            let client = probe_client.clone();
            let base_url = probe_base_url.clone();
            let api_key = probe_api_key.clone();
            async move {
                let url = build_upstream_url_with_base(&base_url, path);
                let started_at = std::time::Instant::now();
                let mut req = client.request(method.clone(), &url);
                if anthropic {
                    req = req
                        .header("x-api-key", api_key)
                        .header("anthropic-version", "2023-06-01");
                } else {
                    req = req.bearer_auth(api_key);
                }
                if let Some(body) = body {
                    req = req.json(&body);
                }
                let resp = req.send().await;
                let duration_ms = started_at.elapsed().as_millis() as u64;
                match resp {
                    Ok(resp) => {
                        let status = resp.status().as_u16();
                        let body_bytes = resp.bytes().await.unwrap_or_default();
                        let body_text = String::from_utf8_lossy(&body_bytes).to_string();
                        let json = serde_json::from_slice::<Value>(&body_bytes).ok();
                        (status, duration_ms, body_text, json)
                    }
                    Err(err) => (599, duration_ms, err.to_string(), None),
                }
            }
        };

    let (status, duration_ms, body_text, json) =
        run_json_request(reqwest::Method::GET, "/v1/models", None, false).await;
    let supports_models = status >= 200
        && status < 300
        && json
            .as_ref()
            .and_then(|root| root.get("data").and_then(|v| v.as_array()))
            .is_some();
    checks.push(build_probe_check(
        "models",
        "GET",
        "/v1/models",
        status,
        supports_models,
        duration_ms,
        if supports_models {
            "支持模型列表查询".to_string()
        } else {
            format!(
                "上游未提供标准模型列表，body={}",
                truncate_body(body_text.as_bytes())
            )
        },
        Some(truncate_body(body_text.as_bytes())),
    ));

    let chat_body = serde_json::json!({
        "model": effective_model,
        "messages": [{ "role": "user", "content": "Reply with exactly: pong" }],
    });
    let (status, duration_ms, body_text, json) = run_json_request(
        reqwest::Method::POST,
        "/v1/chat/completions",
        Some(chat_body),
        false,
    )
    .await;
    let chat_text_ok = json
        .as_ref()
        .and_then(|root| root.get("choices").and_then(|v| v.as_array()))
        .and_then(|choices| choices.first())
        .and_then(|choice| choice.get("message"))
        .map(|message| response_message_text(message.get("content").unwrap_or(&Value::Null)))
        .map(|text| text.trim() == "pong")
        .unwrap_or(false);
    checks.push(build_probe_check(
        "chat_completions",
        "POST",
        "/v1/chat/completions",
        status,
        chat_text_ok,
        duration_ms,
        if chat_text_ok {
            "文本对话正常".to_string()
        } else {
            format!("文本对话异常，body={}", truncate_body(body_text.as_bytes()))
        },
        Some(truncate_body(body_text.as_bytes())),
    ));

    let responses_body = serde_json::json!({
        "model": effective_model,
        "input": "Reply with exactly: pong",
    });
    let (status, duration_ms, body_text, json) = run_json_request(
        reqwest::Method::POST,
        "/v1/responses",
        Some(responses_body),
        false,
    )
    .await;
    let responses_text_ok = json
        .as_ref()
        .and_then(|root| root.get("output").and_then(|v| v.as_array()))
        .and_then(|items| {
            items
                .iter()
                .find(|item| item.get("type").and_then(|v| v.as_str()) == Some("message"))
        })
        .map(|message| response_message_text(message.get("content").unwrap_or(&Value::Null)))
        .map(|text| text.trim() == "pong")
        .unwrap_or(false);
    checks.push(build_probe_check(
        "responses",
        "POST",
        "/v1/responses",
        status,
        responses_text_ok,
        duration_ms,
        if responses_text_ok {
            "Responses 文本返回正常".to_string()
        } else {
            format!(
                "Responses 文本返回异常，body={}",
                truncate_body(body_text.as_bytes())
            )
        },
        Some(truncate_body(body_text.as_bytes())),
    ));

    let messages_body = serde_json::json!({
        "model": effective_model,
        "max_tokens": 64,
        "messages": [{ "role": "user", "content": "Reply with exactly: pong" }],
    });
    let (status, duration_ms, body_text, _json) = run_json_request(
        reqwest::Method::POST,
        "/v1/messages",
        Some(messages_body),
        true,
    )
    .await;
    let supports_messages = status >= 200 && status < 300;
    checks.push(build_probe_check(
        "messages",
        "POST",
        "/v1/messages",
        status,
        supports_messages,
        duration_ms,
        if supports_messages {
            "支持原生 Anthropic messages".to_string()
        } else {
            format!(
                "Anthropic 协议不可直通，body={}",
                truncate_body(body_text.as_bytes())
            )
        },
        Some(truncate_body(body_text.as_bytes())),
    ));

    let chat_tool_body = serde_json::json!({
        "model": effective_model,
        "messages": [
            { "role": "system", "content": "You are a coding agent. Use tools when explicitly required." },
            { "role": "user", "content": "Call the get_workspace_root tool now. Do not answer directly." }
        ],
        "tools": [{
            "type": "function",
            "function": {
                "name": "get_workspace_root",
                "description": "Return current workspace root path.",
                "parameters": { "type": "object", "properties": {}, "additionalProperties": false }
            }
        }],
        "tool_choice": {
            "type": "function",
            "function": { "name": "get_workspace_root" }
        }
    });
    let (status, duration_ms, body_text, json) = run_json_request(
        reqwest::Method::POST,
        "/v1/chat/completions",
        Some(chat_tool_body),
        false,
    )
    .await;
    let (chat_tool_call_ok, chat_tool_summary) = json
        .as_ref()
        .map(parse_probe_chat_tool_call)
        .unwrap_or_else(|| (false, truncate_body(body_text.as_bytes())));
    checks.push(build_probe_check(
        "chat_tool_call",
        "POST",
        "/v1/chat/completions",
        status,
        status >= 200 && status < 300 && chat_tool_call_ok,
        duration_ms,
        chat_tool_summary,
        Some(truncate_body(body_text.as_bytes())),
    ));

    let responses_tool_body = serde_json::json!({
        "model": effective_model,
        "input": "Call the get_workspace_root tool now. Do not answer directly.",
        "tools": [{
            "type": "function",
            "name": "get_workspace_root",
            "description": "Return current workspace root path.",
            "parameters": { "type": "object", "properties": {}, "additionalProperties": false }
        }],
        "tool_choice": {
            "type": "function",
            "name": "get_workspace_root"
        }
    });
    let (status, duration_ms, body_text, json) = run_json_request(
        reqwest::Method::POST,
        "/v1/responses",
        Some(responses_tool_body),
        false,
    )
    .await;
    let (responses_tool_call_ok, responses_tool_summary) = json
        .as_ref()
        .map(parse_probe_responses_tool_call)
        .unwrap_or_else(|| (false, truncate_body(body_text.as_bytes())));
    checks.push(build_probe_check(
        "responses_tool_call",
        "POST",
        "/v1/responses",
        status,
        status >= 200 && status < 300 && responses_tool_call_ok,
        duration_ms,
        responses_tool_summary,
        Some(truncate_body(body_text.as_bytes())),
    ));

    let url = build_upstream_url_with_base(&config.base_url, "/v1/chat/completions");
    let stream_body = serde_json::json!({
        "model": effective_model,
        "stream": true,
        "messages": [
            { "role": "system", "content": "You are Codex, a coding agent. Prefer using tools over describing what you would do." },
            { "role": "user", "content": "Use the apply_patch tool to create a new file hello.txt with the single line hello. Do not answer with prose." }
        ],
        "tools": [{
            "type": "function",
            "function": {
                "name": "apply_patch",
                "description": "Apply a unified patch to modify files.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "patch": { "type": "string", "description": "Patch text in apply_patch format." }
                    },
                    "required": ["patch"],
                    "additionalProperties": false
                }
            }
        }],
        "tool_choice": "auto"
    });
    let started_at = std::time::Instant::now();
    let stream_resp = client
        .post(&url)
        .bearer_auth(&config.api_key)
        .json(&stream_body)
        .send()
        .await;
    let (status, duration_ms, body_text) = match stream_resp {
        Ok(resp) => {
            let status = resp.status().as_u16();
            let bytes = resp.bytes().await.unwrap_or_default();
            (
                status,
                started_at.elapsed().as_millis() as u64,
                String::from_utf8_lossy(&bytes).to_string(),
            )
        }
        Err(err) => (
            599,
            started_at.elapsed().as_millis() as u64,
            err.to_string(),
        ),
    };
    let (streaming_tool_call_ok, stream_summary) = parse_probe_chat_stream_tool_call(&body_text);
    checks.push(build_probe_check(
        "chat_stream_tool_call",
        "POST",
        "/v1/chat/completions",
        status,
        status >= 200 && status < 300 && streaming_tool_call_ok,
        duration_ms,
        stream_summary,
        Some(truncate_body(body_text.as_bytes())),
    ));

    let supports_chat_completions = chat_text_ok;
    let supports_responses = responses_text_ok;
    let recommended_strategy = if supports_chat_completions && !responses_tool_call_ok {
        "bridge_all_agent_traffic_to_chat_completions".to_string()
    } else if supports_chat_completions && supports_messages {
        "native_mixed_protocols".to_string()
    } else {
        "chat_completions_only".to_string()
    };
    let mut recommendations = Vec::new();
    if !supports_models {
        recommendations.push("上游不支持 `/v1/models`，请依赖本地默认模型和模型映射，不要把模型发现建立在上游列表上。".to_string());
    }
    if supports_chat_completions {
        recommendations.push(
            "将 Codex/Claude agent 流量统一桥接到上游 `/v1/chat/completions` 是当前最稳的路径。"
                .to_string(),
        );
    }
    if !responses_tool_call_ok {
        recommendations.push("不要直通上游原生 `/v1/responses` 工具调用；继续由本地代理做 Responses -> Chat 的协议收敛。".to_string());
    }
    if !supports_messages {
        recommendations.push(
            "不要直通上游原生 `/v1/messages`；继续由本地代理做 Claude -> Chat 的协议转换。"
                .to_string(),
        );
    }
    if chat_tool_call_ok && streaming_tool_call_ok {
        recommendations.push(
            "保留流式工具参数拼接与 JSON 修复逻辑，`glm-5` 的 SSE 工具参数是分片返回的。"
                .to_string(),
        );
    }
    if openai_compat_is_glm_family(config, Some(&effective_model)) {
        recommendations.push("对 `glm-5` 保持 coding-agent 工具偏置：工具优先、文件编辑优先 `apply_patch`、工具参数必须是完整 JSON。".to_string());
    }

    Ok(OpenAICompatProbeResult {
        provider_name: config.provider_name.clone(),
        base_url: config.base_url.clone(),
        requested_model,
        effective_model,
        checked_at: chrono::Utc::now().timestamp_millis() as u64,
        supports_models,
        supports_chat_completions,
        supports_responses,
        supports_messages,
        chat_tool_call_ok,
        responses_tool_call_ok,
        streaming_tool_call_ok,
        recommended_strategy,
        recommendations,
        checks,
    })
}

pub async fn run_openai_compat_bridge_smoke(
    base_url: String,
    api_key: String,
    model: String,
) -> Result<Value, String> {
    let config = OpenAICompatProviderConfig {
        id: "smoke".to_string(),
        provider_name: "smoke".to_string(),
        base_url,
        api_key,
        default_model: Some(model.clone()),
        model_mappings: vec![OpenAICompatModelMapping {
            alias: "*".to_string(),
            provider_model: model.clone(),
        }],
        created_at: 0,
        updated_at: 0,
    };
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(45))
        .build()
        .map_err(|e| e.to_string())?;
    let mut proxy_cfg = proxy_config_snapshot();
    proxy_cfg.model_override = Some(model.clone());
    let chat_url = build_upstream_url_with_base(&config.base_url, "/v1/chat/completions");

    let responses_text_request = serde_json::json!({
        "model": model,
        "input": "Reply with exactly: pong",
    });
    let (mut chat_request, _) =
        convert_responses_request_to_chat_completions(&responses_text_request, &proxy_cfg)?;
    let _ = maybe_inject_glm_coding_agent_bias(&config, Some(&model), &mut chat_request);
    let chat_resp = client
        .post(&chat_url)
        .bearer_auth(&config.api_key)
        .json(&chat_request)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let text_status = chat_resp.status().as_u16();
    let text_bytes = chat_resp.bytes().await.map_err(|e| e.to_string())?;
    let (responses_bytes, _, _, _) =
        convert_chat_completions_non_stream_to_responses(&responses_text_request, &text_bytes)?;
    let responses_root: Value =
        serde_json::from_slice(&responses_bytes).map_err(|e| e.to_string())?;
    let responses_text_ok = responses_root
        .get("output")
        .and_then(|v| v.as_array())
        .and_then(|items| {
            items
                .iter()
                .find(|item| item.get("type").and_then(|v| v.as_str()) == Some("message"))
        })
        .map(|message| response_message_text(message.get("content").unwrap_or(&Value::Null)))
        .map(|text| text.trim() == "pong")
        .unwrap_or(false);

    let responses_tool_request = serde_json::json!({
        "model": model,
        "input": "Call the get_workspace_root tool now. Do not answer directly.",
        "tools": [{
            "type": "function",
            "name": "get_workspace_root",
            "description": "Return current workspace root path.",
            "parameters": { "type": "object", "properties": {}, "additionalProperties": false }
        }],
        "tool_choice": {
            "type": "function",
            "name": "get_workspace_root"
        }
    });
    let (mut chat_tool_request, _) =
        convert_responses_request_to_chat_completions(&responses_tool_request, &proxy_cfg)?;
    let _ = maybe_inject_glm_coding_agent_bias(&config, Some(&model), &mut chat_tool_request);
    let chat_tool_resp = client
        .post(&chat_url)
        .bearer_auth(&config.api_key)
        .json(&chat_tool_request)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let tool_status = chat_tool_resp.status().as_u16();
    let tool_bytes = chat_tool_resp.bytes().await.map_err(|e| e.to_string())?;
    let (responses_tool_bytes, _, _, _) =
        convert_chat_completions_non_stream_to_responses(&responses_tool_request, &tool_bytes)?;
    let responses_tool_root: Value =
        serde_json::from_slice(&responses_tool_bytes).map_err(|e| e.to_string())?;
    let (responses_tool_ok, responses_tool_summary) =
        parse_probe_responses_tool_call(&responses_tool_root);

    let claude_request = serde_json::json!({
        "model": model,
        "max_tokens": 256,
        "messages": [{
            "role": "user",
            "content": [{ "type": "text", "text": "Call the get_workspace_root tool now. Do not answer directly." }]
        }],
        "tools": [{
            "name": "get_workspace_root",
            "description": "Return current workspace root path.",
            "input_schema": { "type": "object", "properties": {}, "additionalProperties": false }
        }],
        "tool_choice": { "type": "tool", "name": "get_workspace_root" }
    });
    let (responses_request, reverse_map, _) = convert_claude_to_codex(&claude_request)?;
    let (mut claude_chat_request, _) =
        convert_responses_request_to_chat_completions(&responses_request, &proxy_cfg)?;
    let _ = maybe_inject_glm_coding_agent_bias(&config, Some(&model), &mut claude_chat_request);
    let claude_chat_resp = client
        .post(&chat_url)
        .bearer_auth(&config.api_key)
        .json(&claude_chat_request)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let claude_status = claude_chat_resp.status().as_u16();
    let claude_chat_bytes = claude_chat_resp.bytes().await.map_err(|e| e.to_string())?;
    let (claude_tool_use_ok, claude_bridge_excerpt) =
        match convert_chat_completions_non_stream_to_responses(&responses_request, &claude_chat_bytes) {
            Ok((claude_responses_bytes, _, _, _)) => {
                match convert_codex_response_bytes_to_claude(&claude_responses_bytes, &reverse_map, &model) {
                    Ok((claude_bytes, _, _)) => {
                        let claude_root: Value =
                            serde_json::from_slice(&claude_bytes).map_err(|e| e.to_string())?;
                        let ok = claude_root
                            .get("content")
                            .and_then(|v| v.as_array())
                            .map(|items| {
                                items.iter().any(|item| {
                                    item.get("type").and_then(|v| v.as_str()) == Some("tool_use")
                                        && item.get("name").and_then(|v| v.as_str()) == Some("get_workspace_root")
                                })
                            })
                            .unwrap_or(false);
                        (ok, truncate_body(&claude_bytes))
                    }
                    Err(err) => (false, err),
                }
            }
            Err(err) => (false, err),
        };

    let apply_patch_auto = run_smoke_chat_tool_case(
        &client,
        &chat_url,
        &config,
        &model,
        "apply_patch",
        "Use the apply_patch tool to create a new file hello.txt with the single line hello. Do not answer with prose.",
        "Apply a unified patch to modify files.",
        serde_json::json!({
            "type": "object",
            "properties": {
                "patch": { "type": "string", "description": "Patch text in apply_patch format." }
            },
            "required": ["patch"],
            "additionalProperties": false
        }),
        serde_json::json!("auto"),
    )
    .await?;
    let apply_patch_required = run_smoke_chat_tool_case(
        &client,
        &chat_url,
        &config,
        &model,
        "apply_patch",
        "Use the apply_patch tool to create a new file hello.txt with the single line hello. Do not answer with prose.",
        "Apply a unified patch to modify files.",
        serde_json::json!({
            "type": "object",
            "properties": {
                "patch": { "type": "string", "description": "Patch text in apply_patch format." }
            },
            "required": ["patch"],
            "additionalProperties": false
        }),
        serde_json::json!({
            "type": "function",
            "function": { "name": "apply_patch" }
        }),
    )
    .await?;
    let exec_command_auto = run_smoke_chat_tool_case(
        &client,
        &chat_url,
        &config,
        &model,
        "exec_command",
        "Run the exec_command tool with the command pwd. Do not answer with prose.",
        "Run a shell command.",
        serde_json::json!({
            "type": "object",
            "properties": {
                "cmd": { "type": "string", "description": "Shell command to execute." }
            },
            "required": ["cmd"],
            "additionalProperties": false
        }),
        serde_json::json!("auto"),
    )
    .await?;

    Ok(serde_json::json!({
        "base_url": config.base_url,
        "model": model,
        "responses_text_bridge_ok": responses_text_ok,
        "responses_text_bridge_status": text_status,
        "responses_tool_bridge_ok": responses_tool_ok,
        "responses_tool_bridge_status": tool_status,
        "responses_tool_bridge_summary": responses_tool_summary,
        "claude_tool_bridge_ok": claude_tool_use_ok,
        "claude_tool_bridge_status": claude_status,
        "apply_patch_auto_ok": apply_patch_auto.get("ok").and_then(|v| v.as_bool()).unwrap_or(false),
        "apply_patch_auto_status": apply_patch_auto.get("status").and_then(|v| v.as_u64()).unwrap_or(0),
        "apply_patch_auto_summary": apply_patch_auto.get("summary").cloned().unwrap_or(Value::Null),
        "apply_patch_required_ok": apply_patch_required.get("ok").and_then(|v| v.as_bool()).unwrap_or(false),
        "apply_patch_required_status": apply_patch_required.get("status").and_then(|v| v.as_u64()).unwrap_or(0),
        "apply_patch_required_summary": apply_patch_required.get("summary").cloned().unwrap_or(Value::Null),
        "exec_command_auto_ok": exec_command_auto.get("ok").and_then(|v| v.as_bool()).unwrap_or(false),
        "exec_command_auto_status": exec_command_auto.get("status").and_then(|v| v.as_u64()).unwrap_or(0),
        "exec_command_auto_summary": exec_command_auto.get("summary").cloned().unwrap_or(Value::Null),
        "responses_text_bridge_excerpt": truncate_body(&responses_bytes),
        "responses_tool_bridge_excerpt": truncate_body(&responses_tool_bytes),
        "claude_tool_bridge_excerpt": claude_bridge_excerpt,
        "apply_patch_auto_excerpt": apply_patch_auto.get("excerpt").cloned().unwrap_or(Value::Null),
        "apply_patch_required_excerpt": apply_patch_required.get("excerpt").cloned().unwrap_or(Value::Null),
        "exec_command_auto_excerpt": exec_command_auto.get("excerpt").cloned().unwrap_or(Value::Null),
    }))
}

fn convert_responses_request_to_chat_completions(
    body: &Value,
    cfg: &ProxyConfig,
) -> Result<(Value, bool), String> {
    let mut model = body
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if let Some(override_model) = cfg
        .model_override
        .as_ref()
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
    {
        model = override_model.to_string();
    }
    if model.eq_ignore_ascii_case("glm5") {
        model = "glm-5".to_string();
    }
    if model.is_empty() {
        return Err("未配置模型。请在代理设置中填写模型名称，例如 glm-5。".to_string());
    }

    let stream = body
        .get("stream")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let mut out = serde_json::json!({
        "model": model,
        "messages": [],
        "stream": stream,
    });

    if let Some(instructions) = body
        .get("instructions")
        .and_then(|v| v.as_str())
        .filter(|v| !v.is_empty())
    {
        if let Some(messages) = out.get_mut("messages").and_then(|v| v.as_array_mut()) {
            messages.push(serde_json::json!({
                "role": "system",
                "content": instructions,
            }));
        }
    }

    if let Some(input) = body.get("input") {
        match input {
            Value::String(text) => {
                if let Some(messages) = out.get_mut("messages").and_then(|v| v.as_array_mut()) {
                    messages.push(serde_json::json!({
                        "role": "user",
                        "content": text,
                    }));
                }
            }
            Value::Array(items) => {
                for item in items {
                    let item_type = item
                        .get("type")
                        .and_then(|v| v.as_str())
                        .or_else(|| item.get("role").and_then(|_| Some("message")))
                        .unwrap_or("");
                    match item_type {
                        "message" | "" => {
                            let role = item.get("role").and_then(|v| v.as_str()).unwrap_or("user");
                            let role = if role == "developer" { "system" } else { role };
                            let mut message = serde_json::json!({
                                "role": role,
                                "content": [],
                            });
                            match item.get("content") {
                                Some(Value::String(text)) => {
                                    message["content"] = Value::String(text.clone());
                                }
                                Some(Value::Array(content_items)) => {
                                    let mut content = Vec::new();
                                    for content_item in content_items {
                                        let content_type = content_item
                                            .get("type")
                                            .and_then(|v| v.as_str())
                                            .unwrap_or("input_text");
                                        match content_type {
                                            "input_text" | "output_text" => {
                                                if let Some(text) = content_item
                                                    .get("text")
                                                    .and_then(|v| v.as_str())
                                                {
                                                    content.push(serde_json::json!({
                                                        "type": "text",
                                                        "text": text,
                                                    }));
                                                }
                                            }
                                            "input_image" => {
                                                if let Some(url) = content_item
                                                    .get("image_url")
                                                    .and_then(|v| v.as_str())
                                                {
                                                    content.push(serde_json::json!({
                                                        "type": "image_url",
                                                        "image_url": { "url": url },
                                                    }));
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                    if content.is_empty() {
                                        message["content"] = Value::String(String::new());
                                    } else {
                                        message["content"] = Value::Array(content);
                                    }
                                }
                                _ => {}
                            }
                            if let Some(messages) =
                                out.get_mut("messages").and_then(|v| v.as_array_mut())
                            {
                                messages.push(message);
                            }
                        }
                        "function_call" => {
                            let call_id = item
                                .get("call_id")
                                .and_then(|v| v.as_str())
                                .map(resolve_upstream_tool_call_id)
                                .unwrap_or_default();
                            let name = item.get("name").and_then(|v| v.as_str()).unwrap_or("");
                            let arguments = item
                                .get("arguments")
                                .map(normalize_tool_arguments_string)
                                .unwrap_or_else(|| "{}".to_string());
                            if !call_id.is_empty() || !name.is_empty() {
                                log_proxy(&format!(
                                    "responses->chat tool_call call_id={} name={} args_len={}",
                                    call_id,
                                    name,
                                    arguments.len()
                                ));
                            }
                            if let Some(messages) =
                                out.get_mut("messages").and_then(|v| v.as_array_mut())
                            {
                                messages.push(serde_json::json!({
                                    "role": "assistant",
                                    "content": Value::Null,
                                    "tool_calls": [{
                                        "id": call_id,
                                        "type": "function",
                                        "function": {
                                            "name": name,
                                            "arguments": arguments,
                                        }
                                    }],
                                }));
                            }
                        }
                        "function_call_output" => {
                            let call_id = item
                                .get("call_id")
                                .and_then(|v| v.as_str())
                                .map(resolve_upstream_tool_call_id)
                                .unwrap_or_default();
                            let output = item
                                .get("output")
                                .map(|v| {
                                    if let Some(text) = v.as_str() {
                                        text.to_string()
                                    } else {
                                        serde_json::to_string(v).unwrap_or_default()
                                    }
                                })
                                .unwrap_or_default();
                            if !call_id.is_empty() {
                                log_proxy(&format!(
                                    "responses->chat tool_output call_id={} output_len={}",
                                    call_id,
                                    output.len()
                                ));
                            }
                            if let Some(messages) =
                                out.get_mut("messages").and_then(|v| v.as_array_mut())
                            {
                                messages.push(serde_json::json!({
                                    "role": "tool",
                                    "tool_call_id": call_id,
                                    "content": output,
                                }));
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }

    let mut has_chat_tools = false;
    if let Some(temperature) = body.get("temperature").and_then(|v| v.as_f64()) {
        out["temperature"] = serde_json::json!(temperature);
    }
    if let Some(top_p) = body.get("top_p").and_then(|v| v.as_f64()) {
        out["top_p"] = serde_json::json!(top_p);
    }

    if let Some(tools) = body.get("tools").and_then(|v| v.as_array()) {
        let mut chat_tools = Vec::new();
        for tool in tools {
            let tool_type = tool
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("function");
            if tool_type != "function" && !tool.get("name").is_some() {
                continue;
            }
            let name = tool.get("name").and_then(|v| v.as_str()).unwrap_or("");
            if name.is_empty() {
                continue;
            }
            let description = tool
                .get("description")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let parameters =
                normalize_tool_parameters(tool.get("parameters").unwrap_or(&Value::Null));
            chat_tools.push(serde_json::json!({
                "type": "function",
                "function": {
                    "name": name,
                    "description": description,
                    "parameters": parameters,
                }
            }));
        }
        if !chat_tools.is_empty() {
            out["tools"] = Value::Array(chat_tools);
            has_chat_tools = true;
        }
    }
    if let Some(max_tokens) = body.get("max_output_tokens").and_then(|v| v.as_i64()) {
        out["max_tokens"] = serde_json::json!(max_tokens);
    }
    if has_chat_tools {
        if let Some(parallel_tool_calls) = body.get("parallel_tool_calls").and_then(|v| v.as_bool())
        {
            out["parallel_tool_calls"] = serde_json::json!(parallel_tool_calls);
        }
        if let Some(tool_choice) = body.get("tool_choice") {
            if let Some(normalized_choice) = normalize_openai_tool_choice_for_chat(tool_choice) {
                out["tool_choice"] = normalized_choice;
            }
        }
    }

    if let Some(effort) = body
        .get("reasoning")
        .and_then(|v| v.get("effort"))
        .and_then(|v| v.as_str())
        .and_then(normalize_reasoning_effort)
    {
        let effort = if effort == "xhigh" {
            "high".to_string()
        } else {
            effort
        };
        out["reasoning_effort"] = serde_json::json!(effort);
    }

    if stream {
        out["stream_options"] = serde_json::json!({ "include_usage": true });
    }

    Ok((out, stream))
}

fn convert_chat_completions_non_stream_to_responses(
    request_json: &Value,
    body_bytes: &[u8],
) -> Result<(Bytes, Option<i64>, Option<i64>, Option<String>), String> {
    let root: Value = serde_json::from_slice(body_bytes).map_err(|e| e.to_string())?;
    let response_id = root
        .get("id")
        .and_then(|v| v.as_str())
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string())
        .unwrap_or_else(|| synth_response_id("resp"));
    let created_at = root
        .get("created")
        .and_then(|v| v.as_i64())
        .unwrap_or_else(|| chrono::Utc::now().timestamp());
    let model = request_json
        .get("model")
        .and_then(|v| v.as_str())
        .or_else(|| root.get("model").and_then(|v| v.as_str()))
        .unwrap_or("unknown");

    let mut output = Vec::new();
    if let Some(choices) = root.get("choices").and_then(|v| v.as_array()) {
        for (idx, choice) in choices.iter().enumerate() {
            let message = choice.get("message").unwrap_or(&Value::Null);
            let reasoning_text = response_reasoning_text(message);
            if !reasoning_text.is_empty() {
                output.push(serde_json::json!({
                    "id": format!("rs_{response_id}_{idx}"),
                    "type": "reasoning",
                    "summary": [{
                        "type": "summary_text",
                        "text": reasoning_text,
                    }],
                }));
            }

            let text = response_message_text(message.get("content").unwrap_or(&Value::Null));
            if !text.is_empty() {
                output.push(serde_json::json!({
                    "id": format!("msg_{response_id}_{idx}"),
                    "type": "message",
                    "status": "completed",
                    "role": "assistant",
                    "content": [{
                        "type": "output_text",
                        "annotations": [],
                        "logprobs": [],
                        "text": text,
                    }],
                }));
            }

            if let Some(tool_calls) = message.get("tool_calls").and_then(|v| v.as_array()) {
                for tool_call in tool_calls {
                    let call_id = tool_call.get("id").and_then(|v| v.as_str()).unwrap_or("");
                    let name = tool_call
                        .get("function")
                        .and_then(|v| v.get("name"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let arguments = tool_call
                        .get("function")
                        .and_then(|v| v.get("arguments"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("{}");
                    let normalized_arguments = repair_tool_arguments_json(arguments)
                        .unwrap_or_else(|| arguments.to_string());
                    output.push(serde_json::json!({
                        "id": format!("fc_{call_id}"),
                        "type": "function_call",
                        "status": "completed",
                        "arguments": normalized_arguments,
                        "call_id": call_id,
                        "name": name,
                    }));
                }
            }
        }
    }

    let (input_tokens, output_tokens, cached_tokens, reasoning_tokens) =
        openai_chat_usage(root.get("usage"));
    let total_tokens = root
        .get("usage")
        .and_then(|v| v.get("total_tokens"))
        .and_then(|v| v.as_i64())
        .or_else(|| match (input_tokens, output_tokens) {
            (Some(input), Some(output)) => Some(input + output),
            _ => None,
        });
    let mut response = serde_json::json!({
        "id": response_id,
        "object": "response",
        "created_at": created_at,
        "status": "completed",
        "background": false,
        "error": Value::Null,
        "incomplete_details": Value::Null,
        "model": model,
        "output": output,
    });
    if input_tokens.is_some() || output_tokens.is_some() {
        response["usage"] = serde_json::json!({
            "input_tokens": input_tokens.unwrap_or(0),
            "input_tokens_details": {
                "cached_tokens": cached_tokens.unwrap_or(0),
            },
            "output_tokens": output_tokens.unwrap_or(0),
            "output_tokens_details": {
                "reasoning_tokens": reasoning_tokens.unwrap_or(0),
            },
            "total_tokens": total_tokens.unwrap_or(0),
        });
    }

    let bytes = serde_json::to_vec(&response).map_err(|e| e.to_string())?;
    let body_text = Some(truncate_body(&bytes));
    Ok((Bytes::from(bytes), input_tokens, output_tokens, body_text))
}

fn extract_upstream_error_message_from_value(value: &Value) -> Option<String> {
    if let Some(message) = value
        .get("error")
        .and_then(|v| v.get("message"))
        .and_then(|v| v.as_str())
    {
        let trimmed = message.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    if let Some(message) = value.get("message").and_then(|v| v.as_str()) {
        let trimmed = message.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    if let Some(detail) = value.get("detail").and_then(|v| v.as_str()) {
        let trimmed = detail.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    None
}

fn extract_upstream_error_message(body_bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(body_bytes).trim().to_string();
    if text.is_empty() {
        return "upstream request failed".to_string();
    }
    if let Ok(value) = serde_json::from_slice::<Value>(body_bytes) {
        if let Some(message) = extract_upstream_error_message_from_value(&value) {
            return message;
        }
    }
    truncate_body(text.as_bytes())
}

fn anthropic_error_type_for_status(status: reqwest::StatusCode) -> &'static str {
    match status {
        reqwest::StatusCode::BAD_REQUEST => "invalid_request_error",
        reqwest::StatusCode::UNAUTHORIZED => "authentication_error",
        reqwest::StatusCode::FORBIDDEN => "permission_error",
        reqwest::StatusCode::TOO_MANY_REQUESTS => "rate_limit_error",
        _ => "api_error",
    }
}

fn build_anthropic_error_response_bytes(
    status: reqwest::StatusCode,
    body_bytes: &[u8],
) -> (Bytes, String, Option<String>) {
    let message = extract_upstream_error_message(body_bytes);
    let payload = serde_json::json!({
        "type": "error",
        "error": {
            "type": anthropic_error_type_for_status(status),
            "message": message,
        }
    });
    let bytes = serde_json::to_vec(&payload).unwrap_or_else(|_| {
        format!(
            "{{\"type\":\"error\",\"error\":{{\"type\":\"{}\",\"message\":{}}}}}",
            anthropic_error_type_for_status(status),
            serde_json::to_string(&message).unwrap_or_else(|_| "\"upstream request failed\"".to_string())
        )
        .into_bytes()
    });
    let response_body_text = if bytes.is_empty() {
        None
    } else {
        Some(truncate_body(&bytes))
    };
    (Bytes::from(bytes), message, response_body_text)
}

fn build_claude_response_or_error_body(
    status: reqwest::StatusCode,
    bytes: &Bytes,
    reverse_tool_map: &HashMap<String, String>,
    request_model: &str,
) -> Result<
    (
        Bytes,
        Option<i64>,
        Option<i64>,
        Option<String>,
        Option<String>,
    ),
    String,
> {
    if status.is_success() {
        let (converted, input_tokens, output_tokens, response_body_text) =
            build_claude_response_body(bytes, reverse_tool_map, request_model)?;
        Ok((
            converted,
            input_tokens,
            output_tokens,
            response_body_text,
            None,
        ))
    } else {
        let (converted, error_message, response_body_text) =
            build_anthropic_error_response_bytes(status, bytes);
        Ok((converted, None, None, response_body_text, Some(error_message)))
    }
}

fn convert_codex_non_stream_to_claude(
    resp: &Value,
    reverse_tool_map: &HashMap<String, String>,
    request_model: &str,
) -> Result<Value, String> {
    let response = if resp.get("type").and_then(|v| v.as_str()) == Some("response.completed") {
        resp.get("response")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}))
    } else if resp.get("response").is_some() {
        resp.get("response")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}))
    } else {
        resp.clone()
    };

    let mut out = serde_json::json!({
        "id": response.get("id").cloned().unwrap_or_else(|| serde_json::json!("msg_codex")),
        "type": "message",
        "role": "assistant",
        "model": response.get("model").and_then(|v| v.as_str()).unwrap_or(request_model),
        "content": [],
        "stop_reason": null,
        "stop_sequence": null,
        "usage": { "input_tokens": 0, "output_tokens": 0 }
    });

    if let Some(usage) = response.get("usage") {
        let (input_tokens, output_tokens, cached_tokens) = extract_responses_usage(usage);
        out["usage"]["input_tokens"] = serde_json::json!(input_tokens);
        out["usage"]["output_tokens"] = serde_json::json!(output_tokens);
        if cached_tokens > 0 {
            out["usage"]["cache_read_input_tokens"] = serde_json::json!(cached_tokens);
        }
    }

    let mut has_tool_call = false;
    if let Some(output) = response.get("output").and_then(|v| v.as_array()) {
        for item in output {
            match item.get("type").and_then(|v| v.as_str()).unwrap_or("") {
                "reasoning" => {
                    let mut text = String::new();
                    if let Some(summary) = item.get("summary") {
                        if let Some(arr) = summary.as_array() {
                            for part in arr {
                                if let Some(txt) = part.get("text").and_then(|v| v.as_str()) {
                                    text.push_str(txt);
                                } else if let Some(txt) = part.as_str() {
                                    text.push_str(txt);
                                }
                            }
                        } else if let Some(txt) = summary.as_str() {
                            text.push_str(txt);
                        }
                    }
                    if text.is_empty() {
                        if let Some(content) = item.get("content") {
                            if let Some(arr) = content.as_array() {
                                for part in arr {
                                    if let Some(txt) = part.get("text").and_then(|v| v.as_str()) {
                                        text.push_str(txt);
                                    } else if let Some(txt) = part.as_str() {
                                        text.push_str(txt);
                                    }
                                }
                            } else if let Some(txt) = content.as_str() {
                                text.push_str(txt);
                            }
                        }
                    }
                    if !text.is_empty() {
                        if let Some(arr) = out["content"].as_array_mut() {
                            arr.push(serde_json::json!({ "type": "thinking", "thinking": text }));
                        }
                    }
                }
                "message" => {
                    if let Some(content) = item.get("content") {
                        if let Some(arr) = content.as_array() {
                            for part in arr {
                                if part.get("type").and_then(|v| v.as_str()) == Some("output_text")
                                {
                                    if let Some(text) = part.get("text").and_then(|v| v.as_str()) {
                                        if !text.is_empty() {
                                            if let Some(arr) = out["content"].as_array_mut() {
                                                arr.push(serde_json::json!({ "type": "text", "text": text }));
                                            }
                                        }
                                    }
                                }
                            }
                        } else if let Some(text) = content.as_str() {
                            if !text.is_empty() {
                                if let Some(arr) = out["content"].as_array_mut() {
                                    arr.push(serde_json::json!({ "type": "text", "text": text }));
                                }
                            }
                        }
                    }
                }
                "function_call" => {
                    has_tool_call = true;
                    let mut name = item
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    if let Some(orig) = reverse_tool_map.get(&name) {
                        name = orig.clone();
                    }
                    let mut input_raw = serde_json::json!({});
                    if let Some(args) = item.get("arguments").and_then(|v| v.as_str()) {
                        input_raw = parse_tool_arguments_json(args)?;
                    }
                    if let Some(arr) = out["content"].as_array_mut() {
                        arr.push(serde_json::json!({
                            "type": "tool_use",
                            "id": item.get("call_id").and_then(|v| v.as_str()).unwrap_or(""),
                            "name": name,
                            "input": input_raw
                        }));
                    }
                }
                _ => {}
            }
        }
    }

    if let Some(stop_reason) = response.get("stop_reason").and_then(|v| v.as_str()) {
        if !stop_reason.is_empty() {
            out["stop_reason"] = serde_json::json!(stop_reason);
        }
    } else if has_tool_call {
        out["stop_reason"] = serde_json::json!("tool_use");
    } else {
        out["stop_reason"] = serde_json::json!("end_turn");
    }
    if let Some(stop_sequence) = response.get("stop_sequence") {
        out["stop_sequence"] = stop_sequence.clone();
    }

    Ok(out)
}

fn convert_codex_response_bytes_to_claude(
    bytes: &[u8],
    reverse_tool_map: &HashMap<String, String>,
    request_model: &str,
) -> Result<(Bytes, Option<i64>, Option<i64>), String> {
    let resp_json: Value = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    let claude = convert_codex_non_stream_to_claude(&resp_json, reverse_tool_map, request_model)?;
    let out_bytes = serde_json::to_vec(&claude).map_err(|e| e.to_string())?;
    let (input_tokens, output_tokens) = extract_usage(&out_bytes);
    Ok((Bytes::from(out_bytes), input_tokens, output_tokens))
}

fn build_claude_response_body(
    bytes: &Bytes,
    reverse_tool_map: &HashMap<String, String>,
    request_model: &str,
) -> Result<(Bytes, Option<i64>, Option<i64>, Option<String>), String> {
    let (converted, input_tokens, output_tokens) =
        convert_codex_response_bytes_to_claude(bytes, reverse_tool_map, request_model)?;
    let response_body_text = if converted.is_empty() {
        None
    } else {
        Some(truncate_body(&converted))
    };
    Ok((converted, input_tokens, output_tokens, response_body_text))
}

struct CodexToClaudeStreamState {
    buffer: String,
    pending: std::collections::VecDeque<Bytes>,
    has_tool_call: bool,
    block_index: i64,
    active_tool_block_index: Option<i64>,
    active_tool_arguments: String,
    reverse_tool_map: HashMap<String, String>,
    usage_input: Option<i64>,
    usage_output: Option<i64>,
    cached_tokens: Option<i64>,
    done: bool,
    captured: String,
    log_entry: Option<ProxyLogEntry>,
}

fn append_capture_text(target: &mut String, chunk: &str) {
    if target.len() >= MAX_LOG_BODY_BYTES {
        return;
    }
    let remaining = MAX_LOG_BODY_BYTES - target.len();
    if chunk.len() <= remaining {
        target.push_str(chunk);
    } else {
        target.push_str(&chunk[..remaining]);
    }
}

fn push_claude_sse(state: &mut CodexToClaudeStreamState, event: &str, payload: &Value) {
    let line = format!(
        "event: {event}\ndata: {}\n\n",
        serde_json::to_string(payload).unwrap_or_else(|_| "{}".to_string())
    );
    append_capture_text(&mut state.captured, &line);
    state.pending.push_back(Bytes::from(line));
}

fn codex_event_to_claude(state: &mut CodexToClaudeStreamState, event: &Value) {
    let event_type = event.get("type").and_then(|v| v.as_str()).unwrap_or("");
    match event_type {
        "response.created" => {
            let message = serde_json::json!({
                "type": "message_start",
                "message": {
                    "id": event.get("response").and_then(|v| v.get("id")).and_then(|v| v.as_str()).unwrap_or(""),
                    "type": "message",
                    "role": "assistant",
                    "model": event.get("response").and_then(|v| v.get("model")).and_then(|v| v.as_str()).unwrap_or(""),
                    "stop_sequence": null,
                    "usage": { "input_tokens": 0, "output_tokens": 0 },
                    "content": [],
                    "stop_reason": null
                }
            });
            push_claude_sse(state, "message_start", &message);
        }
        "response.reasoning_summary_part.added" => {
            let payload = serde_json::json!({
                "type": "content_block_start",
                "index": state.block_index,
                "content_block": { "type": "thinking", "thinking": "" }
            });
            push_claude_sse(state, "content_block_start", &payload);
        }
        "response.reasoning_summary_text.delta" => {
            let delta = event.get("delta").and_then(|v| v.as_str()).unwrap_or("");
            let payload = serde_json::json!({
                "type": "content_block_delta",
                "index": state.block_index,
                "delta": { "type": "thinking_delta", "thinking": delta }
            });
            push_claude_sse(state, "content_block_delta", &payload);
        }
        "response.reasoning_summary_part.done" => {
            let payload = serde_json::json!({
                "type": "content_block_stop",
                "index": state.block_index
            });
            push_claude_sse(state, "content_block_stop", &payload);
            state.block_index += 1;
        }
        "response.content_part.added" => {
            let payload = serde_json::json!({
                "type": "content_block_start",
                "index": state.block_index,
                "content_block": { "type": "text", "text": "" }
            });
            push_claude_sse(state, "content_block_start", &payload);
        }
        "response.output_text.delta" => {
            let delta = event.get("delta").and_then(|v| v.as_str()).unwrap_or("");
            let payload = serde_json::json!({
                "type": "content_block_delta",
                "index": state.block_index,
                "delta": { "type": "text_delta", "text": delta }
            });
            push_claude_sse(state, "content_block_delta", &payload);
        }
        "response.content_part.done" => {
            let payload = serde_json::json!({
                "type": "content_block_stop",
                "index": state.block_index
            });
            push_claude_sse(state, "content_block_stop", &payload);
            state.block_index += 1;
        }
        "response.output_item.added" => {
            if let Some(item) = event.get("item") {
                if item.get("type").and_then(|v| v.as_str()) == Some("function_call") {
                    state.has_tool_call = true;
                    let mut name = item
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    if let Some(orig) = state.reverse_tool_map.get(&name) {
                        name = orig.clone();
                    }
                    state.active_tool_block_index = Some(state.block_index);
                    state.active_tool_arguments.clear();
                    let payload = serde_json::json!({
                        "type": "content_block_start",
                        "index": state.block_index,
                        "content_block": {
                            "type": "tool_use",
                            "id": item.get("call_id").and_then(|v| v.as_str()).unwrap_or(""),
                            "name": name,
                            "input": {}
                        }
                    });
                    push_claude_sse(state, "content_block_start", &payload);
                }
            }
        }
        "response.output_item.done" => {
            if let Some(item) = event.get("item") {
                if item.get("type").and_then(|v| v.as_str()) == Some("function_call") {
                    if let Some(active_index) = state.active_tool_block_index {
                        let partial_json = repair_tool_arguments_json(&state.active_tool_arguments)
                            .unwrap_or_else(|| state.active_tool_arguments.clone());
                        let payload = serde_json::json!({
                            "type": "content_block_delta",
                            "index": active_index,
                            "delta": { "type": "input_json_delta", "partial_json": partial_json }
                        });
                        push_claude_sse(state, "content_block_delta", &payload);
                    }
                    let payload = serde_json::json!({
                        "type": "content_block_stop",
                        "index": state.block_index
                    });
                    push_claude_sse(state, "content_block_stop", &payload);
                    state.active_tool_block_index = None;
                    state.active_tool_arguments.clear();
                    state.block_index += 1;
                }
            }
        }
        "response.function_call_arguments.delta" => {
            let delta = event.get("delta").and_then(|v| v.as_str()).unwrap_or("");
            state.active_tool_arguments.push_str(delta);
        }
        "response.completed" => {
            let stop_reason = event
                .get("response")
                .and_then(|v| v.get("stop_reason"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let mapped_reason = if state.has_tool_call {
                "tool_use".to_string()
            } else if stop_reason == "max_tokens" || stop_reason == "stop" {
                stop_reason.to_string()
            } else {
                "end_turn".to_string()
            };
            if let Some(usage) = event.get("response").and_then(|v| v.get("usage")) {
                let (input, output, cached) = extract_responses_usage(usage);
                state.usage_input = Some(input);
                state.usage_output = Some(output);
                if cached > 0 {
                    state.cached_tokens = Some(cached);
                }
            }
            let mut usage_payload = serde_json::json!({
                "input_tokens": state.usage_input.unwrap_or(0),
                "output_tokens": state.usage_output.unwrap_or(0)
            });
            if let Some(cached) = state.cached_tokens {
                usage_payload["cache_read_input_tokens"] = serde_json::json!(cached);
            }
            let payload = serde_json::json!({
                "type": "message_delta",
                "delta": { "stop_reason": mapped_reason, "stop_sequence": null },
                "usage": usage_payload
            });
            push_claude_sse(state, "message_delta", &payload);
            let stop_payload = serde_json::json!({ "type": "message_stop" });
            push_claude_sse(state, "message_stop", &stop_payload);
            state.done = true;
        }
        _ => {}
    }
}

fn process_codex_sse_chunk(state: &mut CodexToClaudeStreamState, chunk: &str) {
    state.buffer.push_str(chunk);
    loop {
        let Some(pos) = state.buffer.find("\n\n") else {
            break;
        };
        let raw = state.buffer[..pos].to_string();
        state.buffer = state.buffer[pos + 2..].to_string();
        let mut data_line = None;
        for line in raw.lines() {
            if let Some(rest) = line.strip_prefix("data:") {
                data_line = Some(rest.trim().to_string());
                break;
            }
        }
        let Some(payload) = data_line else {
            continue;
        };
        if payload.is_empty() {
            continue;
        }
        if payload == "[DONE]" {
            state.done = true;
            continue;
        }
        if let Ok(json) = serde_json::from_str::<Value>(&payload) {
            codex_event_to_claude(state, &json);
        }
    }
}

async fn build_anthropic_stream_response(
    upstream_resp: reqwest::Response,
    reverse_tool_map: HashMap<String, String>,
    log_entry: ProxyLogEntry,
) -> axum::response::Response<axum::body::Body> {
    let status = axum::http::StatusCode::from_u16(upstream_resp.status().as_u16())
        .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);

    let mut state = CodexToClaudeStreamState {
        buffer: String::new(),
        pending: std::collections::VecDeque::new(),
        has_tool_call: false,
        block_index: 0,
        active_tool_block_index: None,
        active_tool_arguments: String::new(),
        reverse_tool_map,
        usage_input: None,
        usage_output: None,
        cached_tokens: None,
        done: false,
        captured: String::new(),
        log_entry: Some(log_entry),
    };

    let mut upstream_stream = upstream_resp.bytes_stream();
    let stream = futures_util::stream::unfold(
        (upstream_stream, state),
        |(mut upstream_stream, mut state)| async move {
            loop {
                if let Some(bytes) = state.pending.pop_front() {
                    return Some((Ok::<Bytes, std::io::Error>(bytes), (upstream_stream, state)));
                }
                if state.done {
                    if let Some(mut entry) = state.log_entry.take() {
                        if !state.captured.is_empty() {
                            entry.response_body = Some(state.captured.clone());
                        }
                        entry.input_tokens = state.usage_input;
                        entry.output_tokens = state.usage_output;
                        let _ = insert_proxy_log(&entry);
                    }
                    return None;
                }
                match upstream_stream.next().await {
                    Some(Ok(chunk)) => {
                        let chunk_text = String::from_utf8_lossy(&chunk);
                        process_codex_sse_chunk(&mut state, &chunk_text);
                        continue;
                    }
                    Some(Err(err)) => {
                        let msg = format!(
                            "event: error\ndata: {{\"type\":\"error\",\"message\":\"{}\"}}\n\n",
                            err
                        );
                        append_capture_text(&mut state.captured, &msg);
                        return Some((
                            Ok::<Bytes, std::io::Error>(Bytes::from(msg)),
                            (upstream_stream, state),
                        ));
                    }
                    None => {
                        state.done = true;
                        continue;
                    }
                }
            }
        },
    );

    axum::response::Response::builder()
        .status(status)
        .header("Content-Type", "text/event-stream")
        .header("Cache-Control", "no-cache")
        .header("Access-Control-Allow-Origin", "*")
        .header("Access-Control-Allow-Headers", "*")
        .body(axum::body::Body::from_stream(stream))
        .unwrap_or_else(|_| {
            axum::response::Response::builder()
                .status(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
                .body(axum::body::Body::empty())
                .unwrap()
        })
}

struct CustomResponsesStreamState {
    buffer: String,
    pending: std::collections::VecDeque<Bytes>,
    captured: String,
    log_entry: Option<ProxyLogEntry>,
    request_json: Value,
    started: bool,
    done: bool,
    seq: i64,
    next_output_index: i64,
    response_id: String,
    created_at: i64,
    // OpenAI Chat `choice.index` -> Responses `output_index`
    message_output_index: HashMap<i64, i64>,
    // OpenAI Chat `choice.index` -> Responses `output_index`
    reasoning_output_index: HashMap<i64, i64>,
    // key = "{choice_index}:{tool_index}" -> Responses `output_index`
    tool_output_index: HashMap<String, i64>,
    // OpenAI Chat `tool_call.id` -> Responses `output_index` (preferred when available)
    tool_output_index_by_call_id: HashMap<String, i64>,
    message_text: HashMap<i64, String>,
    message_added: HashSet<i64>,
    message_content_added: HashSet<i64>,
    message_done: HashSet<i64>,
    active_reasoning_ids: HashMap<i64, String>,
    active_reasoning_text: HashMap<i64, String>,
    completed_reasoning: Vec<(i64, String, String)>,
    tool_call_ids: HashMap<i64, String>,
    tool_names: HashMap<i64, String>,
    tool_args: HashMap<i64, String>,
    tool_added: HashSet<i64>,
    tool_done: HashSet<i64>,
    input_tokens: Option<i64>,
    output_tokens: Option<i64>,
    cached_tokens: Option<i64>,
    reasoning_tokens: Option<i64>,
    total_tokens: Option<i64>,
}

fn alloc_custom_output_index(state: &mut CustomResponsesStreamState) -> i64 {
    let idx = state.next_output_index;
    state.next_output_index += 1;
    idx
}

fn wrap_client_tool_call_id(upstream_call_id: &str) -> String {
    // Codex core (and some OpenAI client stacks) expect tool call ids to look like
    // `chatcmpl-tool-*`. Make it reversible without shared state by embedding the
    // upstream id when it starts with `call_`.
    let upstream_call_id = upstream_call_id.trim();
    if upstream_call_id.is_empty() {
        return String::new();
    }
    if upstream_call_id.starts_with("chatcmpl-tool-") {
        return upstream_call_id.to_string();
    }
    if upstream_call_id.starts_with("call_") {
        return format!("chatcmpl-tool-{upstream_call_id}");
    }
    upstream_call_id.to_string()
}

fn unwrap_client_tool_call_id(call_id: &str) -> String {
    let call_id = call_id.trim();
    if let Some(rest) = call_id.strip_prefix("chatcmpl-tool-") {
        // Only unwrap ids we wrapped (the embedded upstream id starts with `call_`).
        if rest.starts_with("call_") {
            return rest.to_string();
        }
    }
    call_id.to_string()
}

fn synth_client_tool_call_id(response_id: &str, choice_index: i64, tool_index: i64) -> String {
    // Stable across streaming deltas even if upstream omits/changes ids.
    let mut hasher = Sha256::new();
    hasher.update(response_id.as_bytes());
    hasher.update(b":");
    hasher.update(choice_index.to_string().as_bytes());
    hasher.update(b":");
    hasher.update(tool_index.to_string().as_bytes());
    let digest = hasher.finalize();
    let token = URL_SAFE_NO_PAD.encode(&digest[..9]); // short + urlsafe
    format!("chatcmpl-tool-synth_{token}")
}

fn push_custom_responses_sse(state: &mut CustomResponsesStreamState, event: &str, payload: &Value) {
    let line = format!(
        "event: {event}\ndata: {}\n\n",
        serde_json::to_string(payload).unwrap_or_else(|_| "{}".to_string())
    );
    append_capture_text(&mut state.captured, &line);
    state.pending.push_back(Bytes::from(line));
}

fn next_custom_responses_seq(state: &mut CustomResponsesStreamState) -> i64 {
    state.seq += 1;
    state.seq
}

fn ensure_custom_responses_started(state: &mut CustomResponsesStreamState, chunk: &Value) {
    if state.started {
        return;
    }
    state.response_id = chunk
        .get("id")
        .and_then(|v| v.as_str())
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string())
        .unwrap_or_else(|| synth_response_id("resp"));
    state.created_at = chunk
        .get("created")
        .and_then(|v| v.as_i64())
        .unwrap_or_else(|| chrono::Utc::now().timestamp());
    let created = serde_json::json!({
        "type": "response.created",
        "sequence_number": next_custom_responses_seq(state),
        "response": {
            "id": state.response_id.clone(),
            "object": "response",
            "created_at": state.created_at,
            "status": "in_progress",
            "background": false,
            "error": Value::Null,
            "output": [],
        }
    });
    push_custom_responses_sse(state, "response.created", &created);
    let in_progress = serde_json::json!({
        "type": "response.in_progress",
        "sequence_number": next_custom_responses_seq(state),
        "response": {
            "id": state.response_id.clone(),
            "object": "response",
            "created_at": state.created_at,
            "status": "in_progress",
        }
    });
    push_custom_responses_sse(state, "response.in_progress", &in_progress);
    state.started = true;
}

fn finalize_custom_reasoning(state: &mut CustomResponsesStreamState, output_index: i64) {
    let Some(reasoning_id) = state.active_reasoning_ids.remove(&output_index) else {
        return;
    };
    let text = state
        .active_reasoning_text
        .remove(&output_index)
        .unwrap_or_default();
    let text_done = serde_json::json!({
        "type": "response.reasoning_summary_text.done",
        "sequence_number": next_custom_responses_seq(state),
        "item_id": reasoning_id,
        "output_index": output_index,
        "summary_index": 0,
        "text": text,
    });
    push_custom_responses_sse(state, "response.reasoning_summary_text.done", &text_done);
    let part_done = serde_json::json!({
        "type": "response.reasoning_summary_part.done",
        "sequence_number": next_custom_responses_seq(state),
        "item_id": reasoning_id,
        "output_index": output_index,
        "summary_index": 0,
        "part": {
            "type": "summary_text",
            "text": text,
        }
    });
    push_custom_responses_sse(state, "response.reasoning_summary_part.done", &part_done);
    let output_item_done = serde_json::json!({
        "type": "response.output_item.done",
        "sequence_number": next_custom_responses_seq(state),
        "output_index": output_index,
        "item": {
            "id": reasoning_id,
            "type": "reasoning",
            "encrypted_content": "",
            "summary": [{
                "type": "summary_text",
                "text": text,
            }],
        }
    });
    push_custom_responses_sse(state, "response.output_item.done", &output_item_done);
    state
        .completed_reasoning
        .push((output_index, reasoning_id, text));
}

fn ensure_custom_message_started(state: &mut CustomResponsesStreamState, output_index: i64) {
    if !state.message_added.contains(&output_index) {
        let payload = serde_json::json!({
            "type": "response.output_item.added",
            "sequence_number": next_custom_responses_seq(state),
            "output_index": output_index,
            "item": {
                "id": format!("msg_{}_{}", state.response_id, output_index),
                "type": "message",
                "status": "in_progress",
                "content": [],
                "role": "assistant",
            }
        });
        push_custom_responses_sse(state, "response.output_item.added", &payload);
        state.message_added.insert(output_index);
    }
    if !state.message_content_added.contains(&output_index) {
        let payload = serde_json::json!({
            "type": "response.content_part.added",
            "sequence_number": next_custom_responses_seq(state),
            "item_id": format!("msg_{}_{}", state.response_id, output_index),
            "output_index": output_index,
            "content_index": 0,
            "part": {
                "type": "output_text",
                "annotations": [],
                "logprobs": [],
                "text": "",
            }
        });
        push_custom_responses_sse(state, "response.content_part.added", &payload);
        state.message_content_added.insert(output_index);
    }
}

fn finalize_custom_message(state: &mut CustomResponsesStreamState, output_index: i64) {
    if !state.message_added.contains(&output_index) || state.message_done.contains(&output_index) {
        return;
    }
    let text = state
        .message_text
        .get(&output_index)
        .cloned()
        .unwrap_or_default();
    let text_done = serde_json::json!({
        "type": "response.output_text.done",
        "sequence_number": next_custom_responses_seq(state),
        "item_id": format!("msg_{}_{}", state.response_id, output_index),
        "output_index": output_index,
        "content_index": 0,
        "text": text,
        "logprobs": [],
    });
    push_custom_responses_sse(state, "response.output_text.done", &text_done);
    let part_done = serde_json::json!({
        "type": "response.content_part.done",
        "sequence_number": next_custom_responses_seq(state),
        "item_id": format!("msg_{}_{}", state.response_id, output_index),
        "output_index": output_index,
        "content_index": 0,
        "part": {
            "type": "output_text",
            "annotations": [],
            "logprobs": [],
            "text": text,
        }
    });
    push_custom_responses_sse(state, "response.content_part.done", &part_done);
    let item_done = serde_json::json!({
        "type": "response.output_item.done",
        "sequence_number": next_custom_responses_seq(state),
        "output_index": output_index,
        "item": {
            "id": format!("msg_{}_{}", state.response_id, output_index),
            "type": "message",
            "status": "completed",
            "role": "assistant",
            "content": [{
                "type": "output_text",
                "annotations": [],
                "logprobs": [],
                "text": text,
            }],
        }
    });
    push_custom_responses_sse(state, "response.output_item.done", &item_done);
    state.message_done.insert(output_index);
}

fn finalize_custom_tool_call(state: &mut CustomResponsesStreamState, output_index: i64) {
    let Some(call_id) = state.tool_call_ids.get(&output_index).cloned() else {
        log_proxy(&format!(
            "custom_responses: missing tool call_id for output_index={output_index}"
        ));
        return;
    };
    if state.tool_done.contains(&output_index) {
        return;
    }
    let arguments = state
        .tool_args
        .get(&output_index)
        .cloned()
        .filter(|v| !v.is_empty())
        .and_then(|v| repair_tool_arguments_json(&v).or(Some(v)))
        .unwrap_or_else(|| "{}".to_string());
    let name = state
        .tool_names
        .get(&output_index)
        .cloned()
        .unwrap_or_default();
    let args_done = serde_json::json!({
        "type": "response.function_call_arguments.done",
        "sequence_number": next_custom_responses_seq(state),
        "item_id": format!("fc_{call_id}"),
        "output_index": output_index,
        "arguments": arguments,
    });
    push_custom_responses_sse(state, "response.function_call_arguments.done", &args_done);
    let item_done = serde_json::json!({
        "type": "response.output_item.done",
        "sequence_number": next_custom_responses_seq(state),
        "output_index": output_index,
        "item": {
            "id": format!("fc_{call_id}"),
            "type": "function_call",
            "status": "completed",
            "arguments": arguments,
            "call_id": call_id,
            "name": name,
        }
    });
    push_custom_responses_sse(state, "response.output_item.done", &item_done);
    state.tool_done.insert(output_index);
}

fn merge_custom_tool_arguments(existing: &str, incoming: &str) -> (String, Option<String>) {
    let incoming = incoming.trim();
    if incoming.is_empty() {
        return (existing.to_string(), None);
    }

    if existing.is_empty() {
        return (incoming.to_string(), Some(incoming.to_string()));
    }

    if incoming == existing {
        return (existing.to_string(), None);
    }

    let repaired_existing = repair_tool_arguments_json(existing);
    let repaired_incoming = repair_tool_arguments_json(incoming);

    if let Some(incoming_json) = repaired_incoming.as_ref() {
        if repaired_existing.as_ref() == Some(incoming_json) {
            return (incoming_json.clone(), None);
        }

        // Some providers first stream partial fragments and then emit a full JSON object.
        // Replacing the buffer avoids corrupt concatenation like `{..."{...}`.
        return (incoming_json.clone(), None);
    }

    let mut merged = String::with_capacity(existing.len() + incoming.len());
    merged.push_str(existing);
    merged.push_str(incoming);
    (merged, Some(incoming.to_string()))
}

fn finalize_custom_responses_stream(state: &mut CustomResponsesStreamState) {
    if state.done {
        return;
    }

    let reasoning_indexes: Vec<i64> = state.active_reasoning_ids.keys().copied().collect();
    for idx in reasoning_indexes {
        finalize_custom_reasoning(state, idx);
    }

    let message_indexes: Vec<i64> = state.message_added.iter().copied().collect();
    for idx in message_indexes {
        finalize_custom_message(state, idx);
    }

    let tool_indexes: Vec<i64> = state.tool_call_ids.keys().copied().collect();
    for idx in tool_indexes {
        finalize_custom_tool_call(state, idx);
    }

    // Important: `output_index` must uniquely identify a slot in `response.output`.
    // Build a unified list and sort by `output_index` so the final `output` order matches.
    let mut output_items: Vec<(i64, Value)> = Vec::new();

    let mut reasoning_items = state.completed_reasoning.clone();
    reasoning_items.sort_by_key(|(idx, _, _)| *idx);
    for (idx, reasoning_id, text) in reasoning_items {
        output_items.push((
            idx,
            serde_json::json!({
                "id": reasoning_id,
                "type": "reasoning",
                "summary": [{
                    "type": "summary_text",
                    "text": text,
                }],
            }),
        ));
    }

    let mut msg_indexes: Vec<i64> = state.message_added.iter().copied().collect();
    msg_indexes.sort_unstable();
    for idx in msg_indexes {
        output_items.push((
            idx,
            serde_json::json!({
                "id": format!("msg_{}_{}", state.response_id, idx),
                "type": "message",
                "status": "completed",
                "role": "assistant",
                "content": [{
                    "type": "output_text",
                    "annotations": [],
                    "logprobs": [],
                    "text": state.message_text.get(&idx).cloned().unwrap_or_default(),
                }],
            }),
        ));
    }

    let mut tool_indexes: Vec<i64> = state.tool_call_ids.keys().copied().collect();
    tool_indexes.sort_unstable();
    for idx in tool_indexes {
        let call_id = state.tool_call_ids.get(&idx).cloned().unwrap_or_default();
        output_items.push((
            idx,
            serde_json::json!({
                "id": format!("fc_{call_id}"),
                "type": "function_call",
                "status": "completed",
                "arguments": state
                    .tool_args
                    .get(&idx)
                    .cloned()
                    .filter(|v| !v.is_empty())
                    .and_then(|v| repair_tool_arguments_json(&v).or(Some(v)))
                    .unwrap_or_else(|| "{}".to_string()),
                "call_id": call_id,
                "name": state.tool_names.get(&idx).cloned().unwrap_or_default(),
            }),
        ));
    }

    output_items.sort_by_key(|(idx, _)| *idx);
    let output: Vec<Value> = output_items.into_iter().map(|(_, item)| item).collect();

    let mut response = serde_json::json!({
        "id": state.response_id.clone(),
        "object": "response",
        "created_at": state.created_at,
        "status": "completed",
        "background": false,
        "error": Value::Null,
        "output": output,
    });
    if let Some(model) = state.request_json.get("model").and_then(|v| v.as_str()) {
        response["model"] = serde_json::json!(model);
    }
    if let Some(input_tokens) = state.input_tokens {
        response["usage"] = serde_json::json!({
            "input_tokens": input_tokens,
            "input_tokens_details": {
                "cached_tokens": state.cached_tokens.unwrap_or(0),
            },
            "output_tokens": state.output_tokens.unwrap_or(0),
            "output_tokens_details": {
                "reasoning_tokens": state.reasoning_tokens.unwrap_or(0),
            },
            "total_tokens": state.total_tokens.unwrap_or(input_tokens + state.output_tokens.unwrap_or(0)),
        });
    }

    let completed = serde_json::json!({
        "type": "response.completed",
        "sequence_number": next_custom_responses_seq(state),
        "response": response,
    });
    push_custom_responses_sse(state, "response.completed", &completed);
    state.done = true;
}

fn process_custom_openai_sse_chunk(state: &mut CustomResponsesStreamState, chunk: &str) {
    state.buffer.push_str(chunk);
    loop {
        let Some(pos) = state.buffer.find("\n\n") else {
            break;
        };
        let raw = state.buffer[..pos].to_string();
        state.buffer = state.buffer[pos + 2..].to_string();

        let mut data_lines = Vec::new();
        for line in raw.lines() {
            if let Some(rest) = line.strip_prefix("data:") {
                data_lines.push(rest.trim().to_string());
            }
        }
        if data_lines.is_empty() {
            continue;
        }
        let data = data_lines.join("\n");
        if data == "[DONE]" {
            finalize_custom_responses_stream(state);
            continue;
        }
        let Ok(event) = serde_json::from_str::<Value>(&data) else {
            continue;
        };

        ensure_custom_responses_started(state, &event);
        let (input_tokens, output_tokens, cached_tokens, reasoning_tokens) =
            openai_chat_usage(event.get("usage"));
        if input_tokens.is_some() {
            state.input_tokens = input_tokens;
        }
        if output_tokens.is_some() {
            state.output_tokens = output_tokens;
        }
        if cached_tokens.is_some() {
            state.cached_tokens = cached_tokens;
        }
        if reasoning_tokens.is_some() {
            state.reasoning_tokens = reasoning_tokens;
        }
        if let Some(total_tokens) = event
            .get("usage")
            .and_then(|v| v.get("total_tokens"))
            .and_then(|v| v.as_i64())
        {
            state.total_tokens = Some(total_tokens);
        }

        if let Some(choices) = event.get("choices").and_then(|v| v.as_array()) {
            for choice in choices {
                let choice_index = choice.get("index").and_then(|v| v.as_i64()).unwrap_or(0);
                let delta = choice.get("delta").unwrap_or(&Value::Null);
                let reasoning_delta = delta
                    .get("reasoning")
                    .and_then(|v| v.as_str())
                    .or_else(|| delta.get("reasoning_content").and_then(|v| v.as_str()))
                    .unwrap_or("");
                if !reasoning_delta.is_empty() {
                    let reasoning_output_index = if let Some(idx) =
                        state.reasoning_output_index.get(&choice_index).copied()
                    {
                        idx
                    } else {
                        let idx = alloc_custom_output_index(state);
                        state.reasoning_output_index.insert(choice_index, idx);
                        idx
                    };
                    if !state
                        .active_reasoning_ids
                        .contains_key(&reasoning_output_index)
                    {
                        let reasoning_id =
                            format!("rs_{}_{}", state.response_id, reasoning_output_index);
                        state
                            .active_reasoning_ids
                            .insert(reasoning_output_index, reasoning_id.clone());
                        let added = serde_json::json!({
                            "type": "response.output_item.added",
                            "sequence_number": next_custom_responses_seq(state),
                            "output_index": reasoning_output_index,
                            "item": {
                                "id": reasoning_id,
                                "type": "reasoning",
                                "status": "in_progress",
                                "summary": [],
                            }
                        });
                        push_custom_responses_sse(state, "response.output_item.added", &added);
                        let part_added = serde_json::json!({
                            "type": "response.reasoning_summary_part.added",
                            "sequence_number": next_custom_responses_seq(state),
                            "item_id": state.active_reasoning_ids.get(&reasoning_output_index).cloned().unwrap_or_default(),
                            "output_index": reasoning_output_index,
                            "summary_index": 0,
                            "part": {
                                "type": "summary_text",
                                "text": "",
                            }
                        });
                        push_custom_responses_sse(
                            state,
                            "response.reasoning_summary_part.added",
                            &part_added,
                        );
                    }
                    state
                        .active_reasoning_text
                        .entry(reasoning_output_index)
                        .or_default()
                        .push_str(reasoning_delta);
                    let payload = serde_json::json!({
                        "type": "response.reasoning_summary_text.delta",
                        "sequence_number": next_custom_responses_seq(state),
                        "item_id": state.active_reasoning_ids.get(&reasoning_output_index).cloned().unwrap_or_default(),
                        "output_index": reasoning_output_index,
                        "summary_index": 0,
                        "delta": reasoning_delta,
                    });
                    push_custom_responses_sse(
                        state,
                        "response.reasoning_summary_text.delta",
                        &payload,
                    );
                }

                if let Some(text_delta) = delta
                    .get("content")
                    .and_then(|v| v.as_str())
                    .filter(|v| !v.is_empty())
                {
                    if let Some(reasoning_output_index) =
                        state.reasoning_output_index.get(&choice_index).copied()
                    {
                        finalize_custom_reasoning(state, reasoning_output_index);
                    }
                    let message_output_index =
                        if let Some(idx) = state.message_output_index.get(&choice_index).copied() {
                            idx
                        } else {
                            let idx = alloc_custom_output_index(state);
                            state.message_output_index.insert(choice_index, idx);
                            idx
                        };
                    ensure_custom_message_started(state, message_output_index);
                    let payload = serde_json::json!({
                        "type": "response.output_text.delta",
                        "sequence_number": next_custom_responses_seq(state),
                        "item_id": format!("msg_{}_{}", state.response_id, message_output_index),
                        "output_index": message_output_index,
                        "content_index": 0,
                        "delta": text_delta,
                        "logprobs": [],
                    });
                    push_custom_responses_sse(state, "response.output_text.delta", &payload);
                    state
                        .message_text
                        .entry(message_output_index)
                        .or_default()
                        .push_str(text_delta);
                }

                if let Some(tool_calls) = delta.get("tool_calls").and_then(|v| v.as_array()) {
                    if let Some(reasoning_output_index) =
                        state.reasoning_output_index.get(&choice_index).copied()
                    {
                        finalize_custom_reasoning(state, reasoning_output_index);
                    }
                    if let Some(message_output_index) =
                        state.message_output_index.get(&choice_index).copied()
                    {
                        finalize_custom_message(state, message_output_index);
                    }
                    for (pos, tool_call) in tool_calls.iter().enumerate() {
                        // Some upstreams omit `tool_call.index`. Use array position as a stable fallback.
                        let tool_index = tool_call
                            .get("index")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(pos as i64);
                        let tool_key = format!("{choice_index}:{tool_index}");
                        let fallback_output_index =
                            if let Some(idx) = state.tool_output_index.get(&tool_key).copied() {
                                idx
                            } else {
                                let idx = alloc_custom_output_index(state);
                                state.tool_output_index.insert(tool_key, idx);
                                idx
                            };

                        let upstream_call_id = tool_call
                            .get("id")
                            .and_then(|v| v.as_str())
                            .filter(|v| !v.is_empty());
                        let client_call_id = upstream_call_id
                            .map(wrap_client_tool_call_id)
                            .unwrap_or_else(|| {
                                synth_client_tool_call_id(
                                    &state.response_id,
                                    choice_index,
                                    tool_index,
                                )
                            });
                        if let Some(upstream_call_id) = upstream_call_id {
                            store_tool_call_id_mapping(&client_call_id, upstream_call_id);
                        }

                        let tool_output_index = if !client_call_id.is_empty() {
                            let call_id = &client_call_id;
                            if let Some(existing) =
                                state.tool_output_index_by_call_id.get(call_id).copied()
                            {
                                if existing != fallback_output_index {
                                    log_proxy(&format!(
                                        "custom_responses: tool output_index mismatch call_id={} existing={} fallback={} choice_index={} tool_index={}",
                                        call_id, existing, fallback_output_index, choice_index, tool_index
                                    ));
                                }
                                existing
                            } else {
                                state
                                    .tool_output_index_by_call_id
                                    .insert(call_id.to_string(), fallback_output_index);
                                fallback_output_index
                            }
                        } else {
                            fallback_output_index
                        };
                        if let Some(name) = tool_call
                            .get("function")
                            .and_then(|v| v.get("name"))
                            .and_then(|v| v.as_str())
                            .filter(|v| !v.is_empty())
                        {
                            state.tool_names.insert(tool_output_index, name.to_string());
                        }
                        if !client_call_id.is_empty() {
                            match state.tool_call_ids.get(&tool_output_index).cloned() {
                                Some(existing) if existing != client_call_id => {
                                    // Never overwrite: Codex needs a stable id to match tool output.
                                    log_proxy(&format!(
                                        "custom_responses: tool_call_id changed output_index={} existing={} new={} choice_index={} tool_index={}",
                                        tool_output_index, existing, client_call_id, choice_index, tool_index
                                    ));
                                }
                                Some(_) => {}
                                None => {
                                    state
                                        .tool_call_ids
                                        .insert(tool_output_index, client_call_id.clone());
                                }
                            }
                            if !state.tool_added.contains(&tool_output_index) {
                                log_proxy(&format!(
                                    "custom_responses: tool_call_added output_index={} call_id={} upstream_call_id={} choice_index={} tool_index={}",
                                    tool_output_index,
                                    client_call_id,
                                    upstream_call_id.unwrap_or(""),
                                    choice_index,
                                    tool_index
                                ));
                                state.tool_added.insert(tool_output_index);
                                let payload = serde_json::json!({
                                    "type": "response.output_item.added",
                                    "sequence_number": next_custom_responses_seq(state),
                                    "output_index": tool_output_index,
                                    "item": {
                                        "id": format!("fc_{}", client_call_id),
                                        "type": "function_call",
                                        "status": "in_progress",
                                        "arguments": "",
                                        "call_id": client_call_id,
                                        "name": state.tool_names.get(&tool_output_index).cloned().unwrap_or_default(),
                                    }
                                });
                                push_custom_responses_sse(
                                    state,
                                    "response.output_item.added",
                                    &payload,
                                );
                            }
                        }
                        if let Some(arguments_delta) = tool_call
                            .get("function")
                            .and_then(|v| v.get("arguments"))
                            .and_then(|v| v.as_str())
                            .filter(|v| !v.is_empty())
                        {
                            let existing_arguments = state
                                .tool_args
                                .get(&tool_output_index)
                                .cloned()
                                .unwrap_or_default();
                            let (merged_arguments, emitted_delta) =
                                merge_custom_tool_arguments(&existing_arguments, arguments_delta);
                            state.tool_args.insert(tool_output_index, merged_arguments);
                            if let Some(call_id) =
                                state.tool_call_ids.get(&tool_output_index).cloned()
                            {
                                if let Some(delta_to_emit) = emitted_delta {
                                    let payload = serde_json::json!({
                                        "type": "response.function_call_arguments.delta",
                                        "sequence_number": next_custom_responses_seq(state),
                                        "item_id": format!("fc_{call_id}"),
                                        "output_index": tool_output_index,
                                        "delta": delta_to_emit,
                                    });
                                    push_custom_responses_sse(
                                        state,
                                        "response.function_call_arguments.delta",
                                        &payload,
                                    );
                                } else if repair_tool_arguments_json(arguments_delta).is_some() {
                                    log_proxy(&format!(
                                        "custom_responses: replaced tool arguments with full JSON output_index={} call_id={}",
                                        tool_output_index, call_id
                                    ));
                                }
                            }
                        }
                    }
                }

                if choice
                    .get("finish_reason")
                    .and_then(|v| v.as_str())
                    .is_some()
                {
                    if let Some(reasoning_output_index) =
                        state.reasoning_output_index.get(&choice_index).copied()
                    {
                        finalize_custom_reasoning(state, reasoning_output_index);
                    }
                    if let Some(message_output_index) =
                        state.message_output_index.get(&choice_index).copied()
                    {
                        finalize_custom_message(state, message_output_index);
                    }
                }
            }
        }
    }
}

async fn build_custom_openai_stream_response(
    upstream_resp: reqwest::Response,
    request_json: Value,
    log_entry: ProxyLogEntry,
) -> axum::response::Response<axum::body::Body> {
    let status = axum::http::StatusCode::from_u16(upstream_resp.status().as_u16())
        .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);

    let state = CustomResponsesStreamState {
        buffer: String::new(),
        pending: std::collections::VecDeque::new(),
        captured: String::new(),
        log_entry: Some(log_entry),
        request_json,
        started: false,
        done: false,
        seq: 0,
        next_output_index: 0,
        response_id: String::new(),
        created_at: 0,
        message_output_index: HashMap::new(),
        reasoning_output_index: HashMap::new(),
        tool_output_index: HashMap::new(),
        tool_output_index_by_call_id: HashMap::new(),
        message_text: HashMap::new(),
        message_added: HashSet::new(),
        message_content_added: HashSet::new(),
        message_done: HashSet::new(),
        active_reasoning_ids: HashMap::new(),
        active_reasoning_text: HashMap::new(),
        completed_reasoning: Vec::new(),
        tool_call_ids: HashMap::new(),
        tool_names: HashMap::new(),
        tool_args: HashMap::new(),
        tool_added: HashSet::new(),
        tool_done: HashSet::new(),
        input_tokens: None,
        output_tokens: None,
        cached_tokens: None,
        reasoning_tokens: None,
        total_tokens: None,
    };

    let mut upstream_stream = upstream_resp.bytes_stream();
    let stream = futures_util::stream::unfold(
        (upstream_stream, state),
        |(mut upstream_stream, mut state)| async move {
            loop {
                if let Some(bytes) = state.pending.pop_front() {
                    return Some((Ok::<Bytes, std::io::Error>(bytes), (upstream_stream, state)));
                }
                if state.done {
                    if let Some(mut entry) = state.log_entry.take() {
                        if !state.captured.is_empty() {
                            entry.response_body = Some(state.captured.clone());
                        }
                        entry.input_tokens = state.input_tokens;
                        entry.output_tokens = state.output_tokens;
                        let _ = insert_proxy_log(&entry);
                    }
                    return None;
                }
                match upstream_stream.next().await {
                    Some(Ok(chunk)) => {
                        let chunk_text = String::from_utf8_lossy(&chunk);
                        process_custom_openai_sse_chunk(&mut state, &chunk_text);
                    }
                    Some(Err(err)) => {
                        let payload = serde_json::json!({
                            "type": "error",
                            "message": err.to_string(),
                        });
                        let line = format!(
                            "event: error\ndata: {}\n\n",
                            serde_json::to_string(&payload).unwrap_or_else(|_| "{}".to_string())
                        );
                        append_capture_text(&mut state.captured, &line);
                        state.pending.push_back(Bytes::from(line));
                    }
                    None => {
                        finalize_custom_responses_stream(&mut state);
                    }
                }
            }
        },
    );

    axum::response::Response::builder()
        .status(status)
        .header("Content-Type", "text/event-stream")
        .header("Cache-Control", "no-cache")
        .header("Access-Control-Allow-Origin", "*")
        .header("Access-Control-Allow-Headers", "*")
        .body(axum::body::Body::from_stream(stream))
        .unwrap_or_else(|_| {
            axum::response::Response::builder()
                .status(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
                .body(axum::body::Body::empty())
                .unwrap()
        })
}

struct OpenAIToClaudeStreamState {
    buffer: String,
    pending: std::collections::VecDeque<Bytes>,
    captured: String,
    log_entry: Option<ProxyLogEntry>,
    reverse_tool_map: HashMap<String, String>,
    started: bool,
    done: bool,
    has_tool_call: bool,
    message_id: String,
    model: String,
    next_block_index: i64,
    text_block_index: Option<i64>,
    reasoning_block_index: Option<i64>,
    tool_block_indexes: HashMap<i64, i64>,
    tool_names: HashMap<i64, String>,
    tool_ids: HashMap<i64, String>,
    tool_arguments: HashMap<i64, String>,
    input_tokens: Option<i64>,
    output_tokens: Option<i64>,
    cached_tokens: Option<i64>,
}

fn push_openai_to_claude_sse(state: &mut OpenAIToClaudeStreamState, event: &str, payload: &Value) {
    let line = format!(
        "event: {event}\ndata: {}\n\n",
        serde_json::to_string(payload).unwrap_or_else(|_| "{}".to_string())
    );
    append_capture_text(&mut state.captured, &line);
    state.pending.push_back(Bytes::from(line));
}

fn ensure_openai_claude_started(state: &mut OpenAIToClaudeStreamState, chunk: &Value) {
    if state.started {
        return;
    }
    state.message_id = chunk
        .get("id")
        .and_then(|v| v.as_str())
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string())
        .unwrap_or_else(|| synth_response_id("msg"));
    state.model = chunk
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    let payload = serde_json::json!({
        "type": "message_start",
        "message": {
            "id": state.message_id,
            "type": "message",
            "role": "assistant",
            "model": state.model,
            "stop_sequence": null,
            "usage": { "input_tokens": 0, "output_tokens": 0 },
            "content": [],
            "stop_reason": null
        }
    });
    push_openai_to_claude_sse(state, "message_start", &payload);
    state.started = true;
}

fn stop_openai_claude_block(state: &mut OpenAIToClaudeStreamState, index: Option<i64>) {
    if let Some(index) = index {
        let payload = serde_json::json!({
            "type": "content_block_stop",
            "index": index,
        });
        push_openai_to_claude_sse(state, "content_block_stop", &payload);
    }
}

fn finalize_openai_claude_stream(
    state: &mut OpenAIToClaudeStreamState,
    finish_reason: Option<&str>,
) {
    if state.done {
        return;
    }
    let reasoning_block_index = state.reasoning_block_index.take();
    let text_block_index = state.text_block_index.take();
    stop_openai_claude_block(state, reasoning_block_index);
    stop_openai_claude_block(state, text_block_index);
    let tool_indexes: Vec<i64> = state.tool_block_indexes.values().copied().collect();
    let mut sorted_tool_indexes: Vec<i64> = state.tool_block_indexes.keys().copied().collect();
    sorted_tool_indexes.sort_unstable();
    for tool_index in sorted_tool_indexes {
        if let Some(args) = state
            .tool_arguments
            .get(&tool_index)
            .filter(|v| !v.is_empty())
        {
            let partial_json = repair_tool_arguments_json(args).unwrap_or_else(|| args.clone());
            let payload = serde_json::json!({
                "type": "content_block_delta",
                "index": state.tool_block_indexes.get(&tool_index).copied().unwrap_or(0),
                "delta": { "type": "input_json_delta", "partial_json": partial_json }
            });
            push_openai_to_claude_sse(state, "content_block_delta", &payload);
        }
    }
    for index in tool_indexes {
        stop_openai_claude_block(state, Some(index));
    }
    state.tool_block_indexes.clear();
    state.tool_arguments.clear();

    let mapped_reason = if state.has_tool_call {
        "tool_use".to_string()
    } else {
        match finish_reason.unwrap_or("") {
            "length" | "max_tokens" => "max_tokens".to_string(),
            "stop" => "end_turn".to_string(),
            _ => "end_turn".to_string(),
        }
    };
    let mut usage_payload = serde_json::json!({
        "input_tokens": state.input_tokens.unwrap_or(0),
        "output_tokens": state.output_tokens.unwrap_or(0),
    });
    if let Some(cached) = state.cached_tokens {
        usage_payload["cache_read_input_tokens"] = serde_json::json!(cached);
    }
    let delta_payload = serde_json::json!({
        "type": "message_delta",
        "delta": { "stop_reason": mapped_reason, "stop_sequence": null },
        "usage": usage_payload,
    });
    push_openai_to_claude_sse(state, "message_delta", &delta_payload);
    let stop_payload = serde_json::json!({ "type": "message_stop" });
    push_openai_to_claude_sse(state, "message_stop", &stop_payload);
    state.done = true;
}

fn process_openai_chat_to_claude_sse_chunk(state: &mut OpenAIToClaudeStreamState, chunk: &str) {
    state.buffer.push_str(chunk);
    loop {
        let Some(pos) = state.buffer.find("\n\n") else {
            break;
        };
        let raw = state.buffer[..pos].to_string();
        state.buffer = state.buffer[pos + 2..].to_string();

        let mut data_lines = Vec::new();
        for line in raw.lines() {
            if let Some(rest) = line.strip_prefix("data:") {
                data_lines.push(rest.trim().to_string());
            }
        }
        if data_lines.is_empty() {
            continue;
        }
        let data = data_lines.join("\n");
        if data == "[DONE]" {
            finalize_openai_claude_stream(state, None);
            continue;
        }
        let Ok(event) = serde_json::from_str::<Value>(&data) else {
            continue;
        };

        ensure_openai_claude_started(state, &event);
        let (input_tokens, output_tokens, cached_tokens, _) = openai_chat_usage(event.get("usage"));
        if input_tokens.is_some() {
            state.input_tokens = input_tokens;
        }
        if output_tokens.is_some() {
            state.output_tokens = output_tokens;
        }
        if cached_tokens.is_some() {
            state.cached_tokens = cached_tokens;
        }

        if let Some(choices) = event.get("choices").and_then(|v| v.as_array()) {
            for choice in choices {
                let delta = choice.get("delta").unwrap_or(&Value::Null);
                let reasoning_delta = delta
                    .get("reasoning")
                    .and_then(|v| v.as_str())
                    .or_else(|| delta.get("reasoning_content").and_then(|v| v.as_str()))
                    .unwrap_or("");
                if !reasoning_delta.is_empty() {
                    if state.reasoning_block_index.is_none() {
                        let block_index = state.next_block_index;
                        state.next_block_index += 1;
                        state.reasoning_block_index = Some(block_index);
                        let payload = serde_json::json!({
                            "type": "content_block_start",
                            "index": block_index,
                            "content_block": { "type": "thinking", "thinking": "" }
                        });
                        push_openai_to_claude_sse(state, "content_block_start", &payload);
                    }
                    let payload = serde_json::json!({
                        "type": "content_block_delta",
                        "index": state.reasoning_block_index.unwrap_or(0),
                        "delta": { "type": "thinking_delta", "thinking": reasoning_delta }
                    });
                    push_openai_to_claude_sse(state, "content_block_delta", &payload);
                }

                if let Some(text_delta) = delta
                    .get("content")
                    .and_then(|v| v.as_str())
                    .filter(|v| !v.is_empty())
                {
                    let reasoning_block_index = state.reasoning_block_index.take();
                    stop_openai_claude_block(state, reasoning_block_index);
                    if state.text_block_index.is_none() {
                        let block_index = state.next_block_index;
                        state.next_block_index += 1;
                        state.text_block_index = Some(block_index);
                        let payload = serde_json::json!({
                            "type": "content_block_start",
                            "index": block_index,
                            "content_block": { "type": "text", "text": "" }
                        });
                        push_openai_to_claude_sse(state, "content_block_start", &payload);
                    }
                    let payload = serde_json::json!({
                        "type": "content_block_delta",
                        "index": state.text_block_index.unwrap_or(0),
                        "delta": { "type": "text_delta", "text": text_delta }
                    });
                    push_openai_to_claude_sse(state, "content_block_delta", &payload);
                }

                if let Some(tool_calls) = delta.get("tool_calls").and_then(|v| v.as_array()) {
                    let reasoning_block_index = state.reasoning_block_index.take();
                    let text_block_index = state.text_block_index.take();
                    stop_openai_claude_block(state, reasoning_block_index);
                    stop_openai_claude_block(state, text_block_index);
                    for tool_call in tool_calls {
                        let tool_index =
                            tool_call.get("index").and_then(|v| v.as_i64()).unwrap_or(0);
                        if let Some(name) = tool_call
                            .get("function")
                            .and_then(|v| v.get("name"))
                            .and_then(|v| v.as_str())
                            .filter(|v| !v.is_empty())
                        {
                            let mapped_name = state
                                .reverse_tool_map
                                .get(name)
                                .cloned()
                                .unwrap_or_else(|| name.to_string());
                            state.tool_names.insert(tool_index, mapped_name);
                        }
                        if let Some(id) = tool_call
                            .get("id")
                            .and_then(|v| v.as_str())
                            .filter(|v| !v.is_empty())
                        {
                            state.tool_ids.insert(tool_index, id.to_string());
                        }
                        if !state.tool_block_indexes.contains_key(&tool_index) {
                            let block_index = state.next_block_index;
                            state.next_block_index += 1;
                            state.tool_block_indexes.insert(tool_index, block_index);
                            state.has_tool_call = true;
                            let payload = serde_json::json!({
                                "type": "content_block_start",
                                "index": block_index,
                                "content_block": {
                                    "type": "tool_use",
                                    "id": state.tool_ids.get(&tool_index).cloned().unwrap_or_default(),
                                    "name": state.tool_names.get(&tool_index).cloned().unwrap_or_default(),
                                    "input": {}
                                }
                            });
                            push_openai_to_claude_sse(state, "content_block_start", &payload);
                        }
                        if let Some(arguments_delta) = tool_call
                            .get("function")
                            .and_then(|v| v.get("arguments"))
                            .and_then(|v| v.as_str())
                            .filter(|v| !v.is_empty())
                        {
                            state
                                .tool_arguments
                                .entry(tool_index)
                                .or_default()
                                .push_str(arguments_delta);
                        }
                    }
                }

                if let Some(finish_reason) = choice.get("finish_reason").and_then(|v| v.as_str()) {
                    finalize_openai_claude_stream(state, Some(finish_reason));
                }
            }
        }
    }
}

async fn build_openai_chat_to_claude_stream_response(
    upstream_resp: reqwest::Response,
    reverse_tool_map: HashMap<String, String>,
    log_entry: ProxyLogEntry,
) -> axum::response::Response<axum::body::Body> {
    let status = axum::http::StatusCode::from_u16(upstream_resp.status().as_u16())
        .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);

    let state = OpenAIToClaudeStreamState {
        buffer: String::new(),
        pending: std::collections::VecDeque::new(),
        captured: String::new(),
        log_entry: Some(log_entry),
        reverse_tool_map,
        started: false,
        done: false,
        has_tool_call: false,
        message_id: String::new(),
        model: String::new(),
        next_block_index: 0,
        text_block_index: None,
        reasoning_block_index: None,
        tool_block_indexes: HashMap::new(),
        tool_names: HashMap::new(),
        tool_ids: HashMap::new(),
        tool_arguments: HashMap::new(),
        input_tokens: None,
        output_tokens: None,
        cached_tokens: None,
    };

    let mut upstream_stream = upstream_resp.bytes_stream();
    let stream = futures_util::stream::unfold(
        (upstream_stream, state),
        |(mut upstream_stream, mut state)| async move {
            loop {
                if let Some(bytes) = state.pending.pop_front() {
                    return Some((Ok::<Bytes, std::io::Error>(bytes), (upstream_stream, state)));
                }
                if state.done {
                    if let Some(mut entry) = state.log_entry.take() {
                        if !state.captured.is_empty() {
                            entry.response_body = Some(state.captured.clone());
                        }
                        entry.input_tokens = state.input_tokens;
                        entry.output_tokens = state.output_tokens;
                        let _ = insert_proxy_log(&entry);
                    }
                    return None;
                }
                match upstream_stream.next().await {
                    Some(Ok(chunk)) => {
                        let chunk_text = String::from_utf8_lossy(&chunk);
                        process_openai_chat_to_claude_sse_chunk(&mut state, &chunk_text);
                    }
                    Some(Err(err)) => {
                        let payload = serde_json::json!({
                            "type": "error",
                            "error": { "type": "api_error", "message": err.to_string() }
                        });
                        push_openai_to_claude_sse(&mut state, "error", &payload);
                        state.done = true;
                    }
                    None => {
                        finalize_openai_claude_stream(&mut state, None);
                    }
                }
            }
        },
    );

    axum::response::Response::builder()
        .status(status)
        .header("Content-Type", "text/event-stream")
        .header("Cache-Control", "no-cache")
        .header("Access-Control-Allow-Origin", "*")
        .header("Access-Control-Allow-Headers", "*")
        .body(axum::body::Body::from_stream(stream))
        .unwrap_or_else(|_| {
            axum::response::Response::builder()
                .status(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
                .body(axum::body::Body::empty())
                .unwrap()
        })
}

// ─── JWT / auth helpers ───────────────────────────────────────────────────────

fn decode_jwt(token: &str) -> Value {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() < 2 {
        return Value::Object(Default::default());
    }
    URL_SAFE_NO_PAD
        .decode(parts[1])
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_else(|| Value::Object(Default::default()))
}

fn read_meta() -> HashMap<String, MetaEntry> {
    let path = meta_file();
    if !path.exists() {
        return HashMap::new();
    }
    fs::read_to_string(&path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

fn write_meta(meta: &HashMap<String, MetaEntry>) {
    if let Ok(json) = serde_json::to_string_pretty(meta) {
        let _ = fs::write(meta_file(), json);
    }
}

fn parse_auth_data(auth_data: &Value, account_id: &str) -> CodexAccount {
    let tokens = auth_data.get("tokens");
    let empty = Value::Object(Default::default());

    let id_token = tokens
        .and_then(|t| t.get("id_token"))
        .or_else(|| auth_data.get("id_token"))
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let access_token = tokens
        .and_then(|t| t.get("access_token"))
        .or_else(|| auth_data.get("access_token"))
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let refresh_token = tokens
        .and_then(|t| t.get("refresh_token"))
        .or_else(|| auth_data.get("refresh_token"))
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let stored_account_id = tokens
        .and_then(|t| t.get("account_id"))
        .or_else(|| auth_data.get("account_id"))
        .and_then(|v| v.as_str())
        .unwrap_or(account_id)
        .to_string();

    let id_payload = decode_jwt(id_token);
    let at_payload = decode_jwt(access_token);

    let openai_claims = id_payload
        .get("https://api.openai.com/auth")
        .or_else(|| at_payload.get("https://api.openai.com/auth"))
        .unwrap_or(&empty);

    let profile_claims = at_payload
        .get("https://api.openai.com/profile")
        .unwrap_or(&empty);

    let email = id_payload
        .get("email")
        .or_else(|| profile_claims.get("email"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let plan = openai_claims
        .get("chatgpt_plan_type")
        .and_then(|v| v.as_str())
        .unwrap_or("free")
        .to_string();

    let user_id = openai_claims
        .get("chatgpt_user_id")
        .or_else(|| id_payload.get("sub"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let exp = at_payload
        .get("exp")
        .or_else(|| id_payload.get("exp"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    let last_refresh = auth_data
        .get("last_refresh")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let openai_api_key = auth_data
        .get("OPENAI_API_KEY")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    CodexAccount {
        id: stored_account_id,
        email,
        plan,
        user_id,
        expires_at: exp * 1000,
        last_refresh,
        has_refresh_token: !refresh_token.is_empty(),
        openai_api_key,
        label: None,
        added_at: 0,
        proxy_enabled: true,
    }
}

fn extract_auth_tokens(auth_data: &Value) -> (String, Option<String>, Option<String>) {
    let tokens = auth_data.get("tokens").unwrap_or(&Value::Null);
    let access_token = tokens
        .get("access_token")
        .or_else(|| auth_data.get("access_token"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let refresh_token = tokens
        .get("refresh_token")
        .or_else(|| auth_data.get("refresh_token"))
        .and_then(|v| v.as_str())
        .map(|v| v.to_string());
    let account_id = tokens
        .get("account_id")
        .or_else(|| auth_data.get("account_id"))
        .and_then(|v| v.as_str())
        .map(|v| v.to_string());
    (access_token, refresh_token, account_id)
}

fn load_any_auth_data() -> Result<(Value, bool), String> {
    if let Ok(content) = fs::read_to_string(auth_file()) {
        if let Ok(auth_data) = serde_json::from_str::<Value>(&content) {
            return Ok((auth_data, false));
        }
    }
    let accounts_path = accounts_dir();
    if accounts_path.exists() {
        let entries = fs::read_dir(&accounts_path).map_err(|e| e.to_string())?;
        for entry in entries.flatten() {
            let auth_path = entry.path().join("auth.json");
            if !auth_path.exists() {
                continue;
            }
            if let Ok(content) = fs::read_to_string(&auth_path) {
                if let Ok(auth_data) = serde_json::from_str::<Value>(&content) {
                    return Ok((auth_data, true));
                }
            }
        }
    }
    Err("未找到可用的账号，请先登录或导入账号。".to_string())
}

// ─── OAuth PKCE helpers ───────────────────────────────────────────────────────

// OAuth parameters for OpenAI
const AUTH0_DOMAIN: &str = "auth.openai.com";
const CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const SCOPE: &str = "openid profile email offline_access";
const OAUTH_CALLBACK_PORT: u16 = 1455;

fn pkce_verifier() -> String {
    let mut bytes = [0u8; 64];
    rand::thread_rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

fn pkce_challenge(verifier: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(verifier.as_bytes());
    URL_SAFE_NO_PAD.encode(hasher.finalize())
}

fn build_auth_url(redirect_uri: &str, code_challenge: &str, state: &str) -> String {
    let domain = AUTH0_DOMAIN;
    format!(
        "https://{domain}/oauth/authorize\
         ?response_type=code\
         &client_id={CLIENT_ID}\
         &redirect_uri={redirect_uri}\
         &scope={scope}\
         &code_challenge={code_challenge}\
         &code_challenge_method=S256\
         &id_token_add_organizations=true\
         &codex_cli_simplified_flow=true\
         &state={state}\
         &originator=codex_cli_rs",
        redirect_uri =
            percent_encoding::utf8_percent_encode(redirect_uri, percent_encoding::NON_ALPHANUMERIC),
        scope = percent_encoding::utf8_percent_encode(SCOPE, percent_encoding::NON_ALPHANUMERIC),
    )
}

async fn exchange_code(code: &str, redirect_uri: &str, verifier: &str) -> Result<Value, String> {
    let client = reqwest::Client::new();
    let params = [
        ("grant_type", "authorization_code"),
        ("client_id", CLIENT_ID),
        ("code", code),
        ("redirect_uri", redirect_uri),
        ("code_verifier", verifier),
    ];
    let resp = client
        .post(format!("https://{AUTH0_DOMAIN}/oauth/token"))
        .form(&params)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("Token exchange failed ({status}): {body}"));
    }
    resp.json::<Value>().await.map_err(|e| e.to_string())
}

async fn do_token_refresh(refresh_token: &str) -> Result<Value, String> {
    let client = reqwest::Client::new();
    let params = [
        ("grant_type", "refresh_token"),
        ("client_id", CLIENT_ID),
        ("refresh_token", refresh_token),
    ];
    let resp = client
        .post(format!("https://{AUTH0_DOMAIN}/oauth/token"))
        .form(&params)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("Refresh failed ({status}): {body}"));
    }
    resp.json::<Value>().await.map_err(|e| e.to_string())
}

fn save_oauth_tokens(token_response: &Value) -> Result<CodexAccount, String> {
    let access_token = token_response
        .get("access_token")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let id_token = token_response
        .get("id_token")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let refresh_token = token_response
        .get("refresh_token")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let now_iso = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();

    // Parse account_id from access token
    let at_payload = decode_jwt(&access_token);
    let account_id = at_payload
        .get("https://api.openai.com/auth")
        .and_then(|c| c.get("chatgpt_user_id"))
        .or_else(|| at_payload.get("sub"))
        .and_then(|v| v.as_str())
        .unwrap_or("acc_tmp")
        .to_string();

    let auth_data = serde_json::json!({
        "tokens": {
            "access_token": access_token,
            "id_token": id_token,
            "refresh_token": refresh_token,
            "account_id": account_id,
        },
        "last_refresh": now_iso,
    });

    let codex_dir = codex_dir();
    fs::create_dir_all(&codex_dir).map_err(|e| e.to_string())?;
    fs::write(
        auth_file(),
        serde_json::to_string_pretty(&auth_data).unwrap(),
    )
    .map_err(|e| e.to_string())?;

    let mut account = parse_auth_data(&auth_data, &account_id);
    account.id = account_id;
    Ok(account)
}

// ─── Tauri commands: account management ──────────────────────────────────────

#[tauri::command]
fn list_accounts() -> Result<Vec<CodexAccount>, String> {
    let accounts_path = accounts_dir();
    if !accounts_path.exists() {
        fs::create_dir_all(&accounts_path).map_err(|e| e.to_string())?;
    }

    let meta = read_meta();
    let mut accounts = Vec::new();

    let entries = fs::read_dir(&accounts_path).map_err(|e| e.to_string())?;
    for entry in entries.flatten() {
        let dir_name = entry.file_name().to_string_lossy().to_string();
        let auth_path = entry.path().join("auth.json");
        if !auth_path.exists() {
            continue;
        }
        let content = match fs::read_to_string(&auth_path) {
            Ok(c) => c,
            Err(_) => continue,
        };
        let auth_data: Value = match serde_json::from_str(&content) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let mut account = parse_auth_data(&auth_data, &dir_name);
        account.id = dir_name.clone();
        if let Some(m) = meta.get(&dir_name) {
            account.label = m.label.clone();
            account.added_at = m.added_at;
            account.proxy_enabled = m.proxy_enabled;
        }
        accounts.push(account);
    }

    Ok(accounts)
}

#[tauri::command]
fn get_current_account() -> Result<Option<CodexAccount>, String> {
    let auth_path = auth_file();
    if !auth_path.exists() {
        return Ok(None);
    }
    let content = fs::read_to_string(&auth_path).map_err(|e| e.to_string())?;
    let auth_data: Value = serde_json::from_str(&content).map_err(|e| e.to_string())?;
    let mut parsed = parse_auth_data(&auth_data, "current");

    let meta = read_meta();
    let accounts_path = accounts_dir();
    if accounts_path.exists() {
        if let Ok(entries) = fs::read_dir(&accounts_path) {
            for entry in entries.flatten() {
                let dir_name = entry.file_name().to_string_lossy().to_string();
                let candidate_path = entry.path().join("auth.json");
                if !candidate_path.exists() {
                    continue;
                }
                if let Ok(c) = fs::read_to_string(&candidate_path) {
                    if let Ok(candidate) = serde_json::from_str::<Value>(&c) {
                        let empty = Value::Object(Default::default());
                        let cand_tokens = candidate.get("tokens").unwrap_or(&empty);
                        let curr_tokens = auth_data.get("tokens").unwrap_or(&empty);

                        let cand_id = cand_tokens.get("account_id").and_then(|v| v.as_str());
                        let curr_id = curr_tokens.get("account_id").and_then(|v| v.as_str());
                        let cand_rt = cand_tokens.get("refresh_token").and_then(|v| v.as_str());
                        let curr_rt = curr_tokens.get("refresh_token").and_then(|v| v.as_str());

                        let matches = (cand_id.is_some() && cand_id == curr_id)
                            || (cand_rt.is_some() && cand_rt == curr_rt);

                        if matches {
                            parsed.id = dir_name.clone();
                            if let Some(m) = meta.get(&dir_name) {
                                parsed.label = m.label.clone();
                                parsed.added_at = m.added_at;
                                parsed.proxy_enabled = m.proxy_enabled;
                            }
                            return Ok(Some(parsed));
                        }
                    }
                }
            }
        }
    }

    Ok(Some(parsed))
}

#[tauri::command]
fn switch_account(id: String) -> Result<bool, String> {
    let src_path = accounts_dir().join(&id).join("auth.json");
    if !src_path.exists() {
        return Err("Account not found".into());
    }

    // Read the target account's auth data
    let src_content = fs::read_to_string(&src_path).map_err(|e| e.to_string())?;
    let src_data: Value = serde_json::from_str(&src_content).map_err(|e| e.to_string())?;

    let dst_path = auth_file();

    // Preserve existing non-token fields (e.g. OPENAI_API_KEY, user config) from current auth.json
    let mut dst_data: Value = if dst_path.exists() {
        fs::read_to_string(&dst_path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_else(|| serde_json::json!({}))
    } else {
        serde_json::json!({})
    };

    // Only overwrite authentication fields; leave OPENAI_API_KEY and other config intact
    if let Some(tokens) = src_data.get("tokens") {
        dst_data["tokens"] = tokens.clone();
    }
    if let Some(auth_mode) = src_data.get("auth_mode") {
        dst_data["auth_mode"] = auth_mode.clone();
    }
    if let Some(last_refresh) = src_data.get("last_refresh") {
        dst_data["last_refresh"] = last_refresh.clone();
    }

    let out = serde_json::to_string_pretty(&dst_data).map_err(|e| e.to_string())?;
    fs::write(&dst_path, out).map_err(|e| e.to_string())?;
    Ok(true)
}

#[tauri::command]
fn delete_account(id: String) -> Result<bool, String> {
    let account_dir = accounts_dir().join(&id);
    if !account_dir.exists() {
        return Err("Account not found".into());
    }
    fs::remove_dir_all(&account_dir).map_err(|e| e.to_string())?;
    let mut meta = read_meta();
    meta.remove(&id);
    write_meta(&meta);
    Ok(true)
}

#[tauri::command]
fn update_label(id: String, label: String) -> Result<bool, String> {
    let mut meta = read_meta();
    let entry = meta.entry(id).or_insert_with(|| MetaEntry {
        label: None,
        added_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
        proxy_enabled: true,
    });
    entry.label = if label.is_empty() { None } else { Some(label) };
    write_meta(&meta);
    Ok(true)
}

#[tauri::command]
fn update_proxy_enabled(id: String, enabled: bool) -> Result<bool, String> {
    let mut meta = read_meta();
    let entry = meta.entry(id).or_insert_with(|| MetaEntry {
        label: None,
        added_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
        proxy_enabled: true,
    });
    entry.proxy_enabled = enabled;
    write_meta(&meta);
    emit_accounts_updated("proxy_enabled_changed");
    Ok(true)
}

#[tauri::command]
fn import_current(label: Option<String>) -> Result<Value, String> {
    let auth_path = auth_file();
    if !auth_path.exists() {
        return Err("No auth.json found. Please login first.".into());
    }
    let content = fs::read_to_string(&auth_path).map_err(|e| e.to_string())?;
    let auth_data: Value = serde_json::from_str(&content).map_err(|e| e.to_string())?;
    let parsed = parse_auth_data(&auth_data, "tmp");

    let empty = Value::Object(Default::default());
    let tokens = auth_data.get("tokens").unwrap_or(&empty);
    let account_id = tokens
        .get("account_id")
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| {
            if parsed.user_id.is_empty() {
                "acc_tmp"
            } else {
                &parsed.user_id
            }
        })
        .to_string();

    let safe_id: String = account_id
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();

    let dest_dir = accounts_dir().join(&safe_id);
    fs::create_dir_all(&dest_dir).map_err(|e| e.to_string())?;
    fs::copy(&auth_path, dest_dir.join("auth.json")).map_err(|e| e.to_string())?;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let mut meta = read_meta();
    meta.insert(
        safe_id.clone(),
        MetaEntry {
            label,
            added_at: now,
            proxy_enabled: true,
        },
    );
    write_meta(&meta);

    Ok(serde_json::json!({
        "success": true,
        "id": safe_id,
        "email": parsed.email
    }))
}

#[tauri::command]
fn get_config() -> Result<Value, String> {
    let config_path = codex_dir().join("config.toml");
    if !config_path.exists() {
        return Ok(serde_json::json!({ "raw": "" }));
    }
    let raw = fs::read_to_string(&config_path).map_err(|e| e.to_string())?;
    Ok(serde_json::json!({ "raw": raw }))
}

// ─── Tauri commands: OAuth PKCE login ────────────────────────────────────────

#[tauri::command]
fn launch_codex_login() -> Result<Value, String> {
    std::process::Command::new("codex")
        .arg("login")
        .spawn()
        .map_err(|e| e.to_string())?;
    Ok(serde_json::json!({
        "success": true,
        "message": "codex login started. Complete login in your terminal, then click \"Import Current Account\"."
    }))
}

/// Generate an OAuth URL and return it to the frontend without opening a browser.
/// The frontend can copy the URL and let the user login manually, then paste back the callback URL.
#[tauri::command]
fn get_oauth_url() -> Result<Value, String> {
    let port = OAUTH_CALLBACK_PORT;
    let redirect_uri = format!("http://localhost:{port}/auth/callback");
    let verifier = pkce_verifier();
    let challenge = pkce_challenge(&verifier);
    let state: String = {
        let mut b = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut b);
        URL_SAFE_NO_PAD.encode(b)
    };
    let auth_url = build_auth_url(&redirect_uri, &challenge, &state);

    // Store pending session
    *oauth_pending().lock().unwrap() = Some(OAuthPending {
        verifier,
        state,
        redirect_uri,
    });

    Ok(serde_json::json!({
        "auth_url": auth_url,
    }))
}

/// Complete OAuth login by parsing a callback URL the user pasted manually.
/// Extracts code+state, exchanges for tokens, saves and imports the account.
#[tauri::command]
async fn complete_oauth_manual(
    callback_url: String,
    label: Option<String>,
) -> Result<Value, String> {
    let pending = oauth_pending()
        .lock()
        .unwrap()
        .take()
        .ok_or("No pending OAuth session. Please generate a login URL first.")?;

    // Parse query string from full URL or bare query string
    let qs = if let Some(pos) = callback_url.find('?') {
        callback_url[pos + 1..].to_string()
    } else {
        callback_url.clone()
    };

    let params: HashMap<String, String> = qs
        .split('&')
        .filter_map(|p| {
            let mut kv = p.splitn(2, '=');
            let k = kv.next()?.to_string();
            let v = percent_encoding::percent_decode_str(kv.next()?)
                .decode_utf8_lossy()
                .to_string();
            Some((k, v))
        })
        .collect();

    let returned_state = params.get("state").map(|s| s.as_str()).unwrap_or("");
    if returned_state != pending.state {
        return Err("State mismatch — the callback URL does not match this session.".into());
    }
    let code = params
        .get("code")
        .ok_or("No authorization code found in the URL.")?;

    let token_resp = exchange_code(code, &pending.redirect_uri, &pending.verifier).await?;
    let account = save_oauth_tokens(&token_resp)?;
    let import_result = import_current(label)?;

    Ok(serde_json::json!({
        "success": true,
        "email": account.email,
        "plan": account.plan,
        "id": import_result["id"]
    }))
}

/// Start in-app OAuth login flow. Opens browser, waits for callback,
/// exchanges code, saves auth.json, returns the new account.
#[tauri::command]
async fn oauth_login(label: Option<String>) -> Result<Value, String> {
    let port = OAUTH_CALLBACK_PORT;
    let redirect_uri = format!("http://localhost:{port}/auth/callback");
    let verifier = pkce_verifier();
    let challenge = pkce_challenge(&verifier);
    let state: String = {
        let mut b = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut b);
        URL_SAFE_NO_PAD.encode(b)
    };

    let auth_url = build_auth_url(&redirect_uri, &challenge, &state);

    // Open browser
    open::that(&auth_url).map_err(|e| format!("Cannot open browser: {e}"))?;

    // Bind on both IPv4 and IPv6 — macOS may resolve `localhost` to ::1
    let listener_v4 = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .map_err(|e| e.to_string())?;
    let listener_v6 = tokio::net::TcpListener::bind(format!("[::1]:{port}"))
        .await
        .ok();

    // We only need one request; use a channel to get the query string
    let (tx, rx) = tokio::sync::oneshot::channel::<String>();
    let tx = Arc::new(Mutex::new(Some(tx)));

    async fn handle_listener(
        listener: tokio::net::TcpListener,
        tx: Arc<Mutex<Option<tokio::sync::oneshot::Sender<String>>>>,
        state_check: String,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        while let Ok((mut stream, _)) = listener.accept().await {
            let mut buf = vec![0u8; 4096];
            let n = stream.read(&mut buf).await.unwrap_or(0);
            let req = String::from_utf8_lossy(&buf[..n]);
            if let Some(line) = req.lines().next() {
                if let Some(qs_start) = line.find('?') {
                    let qs = &line[qs_start + 1..];
                    let qs = qs.split_whitespace().next().unwrap_or(qs);
                    let params: HashMap<_, _> = qs
                        .split('&')
                        .filter_map(|p| {
                            let mut kv = p.splitn(2, '=');
                            Some((kv.next()?, kv.next()?))
                        })
                        .collect();

                    let ok_state = params.get("state").map_or(false, |s| *s == state_check);
                    let has_code = params.contains_key("code");

                    let html = if ok_state && has_code {
                        "<html><body><h2>✅ 登录成功，可关闭此页面返回应用。</h2></body></html>"
                    } else {
                        "<html><body><h2>❌ 登录失败，请重试。</h2></body></html>"
                    };

                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        html.len(),
                        html
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                    let _ = stream.shutdown().await;

                    if ok_state && has_code {
                        if let Some(sender) = tx.lock().unwrap().take() {
                            let _ = sender.send(qs.to_string());
                        }
                        break;
                    }
                }
            }
        }
    }

    let server = {
        let tx4 = tx.clone();
        let tx6 = tx.clone();
        let state4 = state.clone();
        let state6 = state.clone();
        let v4 = tokio::spawn(handle_listener(listener_v4, tx4, state4));
        let v6 = if let Some(l) = listener_v6 {
            tokio::spawn(handle_listener(l, tx6, state6))
        } else {
            tokio::spawn(async {})
        };
        (v4, v6)
    };

    // Wait for callback (max 3 minutes)
    let qs = tokio::time::timeout(std::time::Duration::from_secs(180), rx)
        .await
        .map_err(|_| "Login timed out (3 min). Please try again.")?
        .map_err(|_| "Login cancelled")?;

    server.0.abort();
    server.1.abort();

    // Parse code from query string
    let params: HashMap<_, _> = qs
        .split('&')
        .filter_map(|p| {
            let mut kv = p.splitn(2, '=');
            Some((kv.next()?.to_string(), kv.next()?.to_string()))
        })
        .collect();
    let code = params.get("code").ok_or("No code in callback")?;

    // Exchange code for tokens
    let token_resp = exchange_code(code, &redirect_uri, &verifier).await?;

    // Save auth.json
    let account = save_oauth_tokens(&token_resp)?;

    // Import into managed accounts
    let import_result = import_current(label)?;

    Ok(serde_json::json!({
        "success": true,
        "email": account.email,
        "plan": account.plan,
        "id": import_result["id"]
    }))
}

/// Refresh tokens for a specific managed account by account id.
#[tauri::command]
async fn refresh_account_token(id: String) -> Result<Value, String> {
    let auth_path = accounts_dir().join(&id).join("auth.json");
    if !auth_path.exists() {
        return Err(format!("Account {id} not found"));
    }
    let content = fs::read_to_string(&auth_path).map_err(|e| e.to_string())?;
    let auth_data: Value = serde_json::from_str(&content).map_err(|e| e.to_string())?;

    let empty = Value::Object(Default::default());
    let tokens = auth_data.get("tokens").unwrap_or(&empty);
    let refresh_token = tokens
        .get("refresh_token")
        .and_then(|v| v.as_str())
        .ok_or("No refresh token stored for this account")?;

    let token_resp = do_token_refresh(refresh_token).await?;

    // Merge new tokens, keep refresh_token if not returned
    let new_access = token_resp
        .get("access_token")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let new_id = token_resp
        .get("id_token")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let new_refresh = token_resp
        .get("refresh_token")
        .and_then(|v| v.as_str())
        .unwrap_or(refresh_token)
        .to_string();

    let account_id = tokens
        .get("account_id")
        .and_then(|v| v.as_str())
        .unwrap_or(&id)
        .to_string();

    let now_iso = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();

    let updated = serde_json::json!({
        "tokens": {
            "access_token": new_access,
            "id_token": new_id,
            "refresh_token": new_refresh,
            "account_id": account_id,
        },
        "last_refresh": now_iso,
    });

    fs::write(&auth_path, serde_json::to_string_pretty(&updated).unwrap())
        .map_err(|e| e.to_string())?;

    // If this is the active account, update auth.json too
    if let Ok(current_content) = fs::read_to_string(auth_file()) {
        if let Ok(current) = serde_json::from_str::<Value>(&current_content) {
            let curr_tokens = current.get("tokens").unwrap_or(&empty);
            let curr_rt = curr_tokens.get("refresh_token").and_then(|v| v.as_str());
            if curr_rt == Some(refresh_token) {
                let _ = fs::write(auth_file(), serde_json::to_string_pretty(&updated).unwrap());
            }
        }
    }

    let updated_account = parse_auth_data(&updated, &id);
    Ok(serde_json::json!({
        "success": true,
        "email": updated_account.email,
        "expires_at": updated_account.expires_at,
    }))
}

// ─── Tauri commands: API reverse proxy ───────────────────────────────────────

const COOLDOWN_SECS: u64 = 60; // 429 cooldown window
const DEFAULT_FRONT_PROXY_MAX_BODY_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_UPSTREAM_BASE_URL: &str = "https://chatgpt.com/backend-api/codex";
const DEFAULT_MODELS_CLIENT_VERSION: &str = "0.98.0";
const CODEX_CLIENT_VERSION: &str = "0.101.0";
const CODEX_USER_AGENT: &str = "codex_cli_rs/0.101.0 (Mac OS 26.0.1; arm64) Apple_Terminal/464";
const CODEX_OPENAI_BETA: &str = "responses=experimental";
const CODEX_ORIGINATOR: &str = "codex_cli_rs";
const CLAUDE_CODE_SYSTEM_PROMPT: &str = "You are Claude Code, Anthropic's official CLI for Claude.";
static PROXY_REQ_ID: AtomicUsize = AtomicUsize::new(1);

fn front_proxy_max_body_bytes() -> usize {
    std::env::var("CODEXMANAGER_FRONT_PROXY_MAX_BODY_BYTES")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_FRONT_PROXY_MAX_BODY_BYTES)
}

fn upstream_base_url() -> String {
    std::env::var("CODEXMANAGER_UPSTREAM_BASE_URL")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| DEFAULT_UPSTREAM_BASE_URL.to_string())
}

fn build_upstream_url_with_base(base: &str, path_and_query: &str) -> String {
    let base = base.trim_end_matches('/');
    if base.contains("/backend-api/codex") && path_and_query.starts_with("/v1/") {
        format!("{base}{}", path_and_query.trim_start_matches("/v1"))
    } else if base.ends_with("/v1") && path_and_query.starts_with("/v1") {
        format!("{}{}", base.trim_end_matches("/v1"), path_and_query)
    } else {
        format!("{base}{path_and_query}")
    }
}

fn build_upstream_url(path_and_query: &str) -> String {
    build_upstream_url_with_base(&upstream_base_url(), path_and_query)
}

fn custom_openai_base_url(cfg: &ProxyConfig) -> Option<String> {
    normalized_custom_base_url(cfg.custom_openai_base_url.as_ref())
}

fn custom_openai_api_key(cfg: &ProxyConfig) -> Option<String> {
    normalized_custom_api_key(cfg.custom_openai_api_key.as_ref())
}

fn normalize_models_path(path: &str) -> String {
    let is_models_path = path == "/v1/models" || path.starts_with("/v1/models?");
    if !is_models_path {
        return path.to_string();
    }
    let has_client_version = path
        .split_once('?')
        .map(|(_, query)| {
            query.split('&').any(|part| {
                part.split('=')
                    .next()
                    .map(|key| key.eq_ignore_ascii_case("client_version"))
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false);
    if has_client_version {
        return path.to_string();
    }
    let separator = if path.contains('?') { '&' } else { '?' };
    format!("{path}{separator}client_version={DEFAULT_MODELS_CLIENT_VERSION}")
}

fn upstream_cookie() -> Option<String> {
    std::env::var("CODEXMANAGER_UPSTREAM_COOKIE")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn parse_bool_env(key: &str) -> bool {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_lowercase())
        .map(|v| matches!(v.as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

fn derive_session_id(key_material: Option<&str>, salt: &str) -> Option<String> {
    let key_material = key_material?;
    let digest = Sha256::digest(format!("{salt}:{key_material}").as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Some(format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5],
        bytes[6], bytes[7],
        bytes[8], bytes[9],
        bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
    ))
}

fn random_session_id() -> String {
    let mut bytes = [0u8; 16];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5],
        bytes[6], bytes[7],
        bytes[8], bytes[9],
        bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
    )
}

fn apply_upstream_headers(
    headers: &mut reqwest::header::HeaderMap,
    auth_token: &str,
    account_id: Option<&str>,
    incoming_headers: &axum::http::HeaderMap,
    has_body: bool,
    is_stream: bool,
) {
    let strip_session_affinity = parse_bool_env("CODEXMANAGER_STRIP_SESSION_AFFINITY");
    let incoming_session_id = incoming_headers
        .get("session_id")
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.trim().is_empty());
    let incoming_conversation_id = incoming_headers
        .get("conversation_id")
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.trim().is_empty());
    let fallback_session_id = if strip_session_affinity {
        None
    } else {
        derive_session_id(
            incoming_headers
                .get("x-codex-sticky")
                .and_then(|v| v.to_str().ok()),
            "session",
        )
    };
    let resolved_session_id = if strip_session_affinity {
        random_session_id()
    } else if let Some(value) = incoming_session_id {
        value.to_string()
    } else if let Some(value) = &fallback_session_id {
        value.clone()
    } else {
        random_session_id()
    };

    headers.insert(
        reqwest::header::AUTHORIZATION,
        reqwest::header::HeaderValue::from_str(&format!("Bearer {}", auth_token))
            .unwrap_or_else(|_| reqwest::header::HeaderValue::from_static("")),
    );
    if has_body {
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            reqwest::header::HeaderValue::from_static("application/json"),
        );
    }
    headers.insert(
        reqwest::header::ACCEPT,
        reqwest::header::HeaderValue::from_static(if is_stream {
            "text/event-stream"
        } else {
            "application/json"
        }),
    );
    headers.insert(
        reqwest::header::CONNECTION,
        reqwest::header::HeaderValue::from_static("Keep-Alive"),
    );
    headers.insert(
        reqwest::header::HeaderName::from_static("version"),
        reqwest::header::HeaderValue::from_static(CODEX_CLIENT_VERSION),
    );
    headers.insert(
        reqwest::header::HeaderName::from_static("openai-beta"),
        reqwest::header::HeaderValue::from_static(CODEX_OPENAI_BETA),
    );
    headers.insert(
        reqwest::header::USER_AGENT,
        reqwest::header::HeaderValue::from_static(CODEX_USER_AGENT),
    );
    headers.insert(
        reqwest::header::HeaderName::from_static("originator"),
        reqwest::header::HeaderValue::from_static(CODEX_ORIGINATOR),
    );
    headers.insert(
        reqwest::header::HeaderName::from_static("session_id"),
        reqwest::header::HeaderValue::from_str(&resolved_session_id)
            .unwrap_or_else(|_| reqwest::header::HeaderValue::from_static("")),
    );
    if !strip_session_affinity {
        if let Some(conversation_id) = incoming_conversation_id {
            headers.insert(
                reqwest::header::HeaderName::from_static("conversation_id"),
                reqwest::header::HeaderValue::from_str(conversation_id)
                    .unwrap_or_else(|_| reqwest::header::HeaderValue::from_static("")),
            );
        }
    }
    if let Some(account_id) = account_id {
        headers.insert(
            reqwest::header::HeaderName::from_static("chatgpt-account-id"),
            reqwest::header::HeaderValue::from_str(account_id)
                .unwrap_or_else(|_| reqwest::header::HeaderValue::from_static("")),
        );
    }
    if let Some(cookie) = upstream_cookie() {
        headers.insert(
            reqwest::header::COOKIE,
            reqwest::header::HeaderValue::from_str(&cookie)
                .unwrap_or_else(|_| reqwest::header::HeaderValue::from_static("")),
        );
    }
}

fn apply_custom_openai_headers(
    headers: &mut reqwest::header::HeaderMap,
    api_key: &str,
    has_body: bool,
    is_stream: bool,
) {
    headers.clear();
    headers.insert(
        reqwest::header::AUTHORIZATION,
        reqwest::header::HeaderValue::from_str(&format!("Bearer {api_key}"))
            .unwrap_or_else(|_| reqwest::header::HeaderValue::from_static("")),
    );
    if has_body {
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            reqwest::header::HeaderValue::from_static("application/json"),
        );
    }
    headers.insert(
        reqwest::header::ACCEPT,
        reqwest::header::HeaderValue::from_static(if is_stream {
            "text/event-stream"
        } else {
            "application/json"
        }),
    );
}

async fn serve_proxy_on_listener(
    listener: tokio::net::TcpListener,
    app: axum::Router,
    shutdown: Arc<Notify>,
) -> io::Result<()> {
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown.notified().await;
        })
        .await
        .map_err(|err| io::Error::new(io::ErrorKind::Other, err))
}

async fn run_proxy_server(addr: &str, app: axum::Router, shutdown: Arc<Notify>) -> io::Result<()> {
    // Bind dual-stack when using localhost.
    let addr_trimmed = addr.trim();
    if addr_trimmed.len() > "localhost:".len()
        && addr_trimmed[..("localhost:".len())].eq_ignore_ascii_case("localhost:")
    {
        let port = &addr_trimmed["localhost:".len()..];
        log_proxy(&format!(
            "binding listeners: 127.0.0.1:{port}, [::1]:{port}"
        ));
        let v4 = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}")).await;
        let v6 = tokio::net::TcpListener::bind(format!("[::1]:{port}")).await;
        return match (v4, v6) {
            (Ok(v4_listener), Ok(v6_listener)) => {
                log_proxy(&format!("bound listeners: 127.0.0.1:{port}, [::1]:{port}"));
                let v4_task = serve_proxy_on_listener(v4_listener, app.clone(), shutdown.clone());
                let v6_task = serve_proxy_on_listener(v6_listener, app, shutdown);
                let (v4_result, v6_result) = tokio::join!(v4_task, v6_task);
                v4_result.and(v6_result)
            }
            (Ok(listener), Err(_)) | (Err(_), Ok(listener)) => {
                log_proxy(&format!("bound listener: {addr_trimmed} (single stack)"));
                serve_proxy_on_listener(listener, app, shutdown).await
            }
            (Err(err), Err(_)) => Err(err),
        };
    }

    log_proxy(&format!("binding listener: {addr_trimmed}"));
    let listener = tokio::net::TcpListener::bind(addr_trimmed).await?;
    log_proxy(&format!("bound listener: {addr_trimmed}"));
    serve_proxy_on_listener(listener, app, shutdown).await
}

/// Load all valid accounts from disk into memory pool
fn load_proxy_accounts() -> Result<Vec<ProxyAccount>, String> {
    let mut pool = Vec::new();
    let accounts_path = accounts_dir();
    let meta = read_meta();

    if !accounts_path.exists() {
        return Err("No accounts directory found. Please login at least one account.".into());
    }

    let entries = fs::read_dir(&accounts_path).map_err(|e| e.to_string())?;
    for entry in entries.flatten() {
        let id = entry.file_name().to_string_lossy().to_string();
        if let Some(m) = meta.get(&id) {
            if !m.proxy_enabled {
                continue;
            }
        }
        let auth_path = entry.path().join("auth.json");
        if !auth_path.exists() {
            continue;
        }

        if let Ok(content) = fs::read_to_string(&auth_path) {
            if let Ok(auth_data) = serde_json::from_str::<Value>(&content) {
                let empty = Value::Object(Default::default());
                let tokens = auth_data.get("tokens").unwrap_or(&empty);
                if let Some(access_token) = tokens.get("access_token").and_then(|v| v.as_str()) {
                    let refresh_token = tokens
                        .get("refresh_token")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    let account_id = tokens
                        .get("account_id")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    pool.push(ProxyAccount {
                        id,
                        account_id,
                        access_token: access_token.to_string(),
                        refresh_token,
                        health: AccountHealth::Active,
                    });
                }
            }
        }
    }

    if pool.is_empty() {
        Err("No enabled accounts in pool. Please enable at least one account.".into())
    } else {
        Ok(pool)
    }
}

/// Headers that should NOT be forwarded to upstream
fn skip_request_header(name: &str) -> bool {
    matches!(
        name.to_lowercase().as_str(),
        "host"
            | "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "authorization"
            | "te"
            | "trailers"
            | "transfer-encoding"
            | "upgrade"
            | "content-length"
    )
}

/// Headers that should NOT be forwarded back to client
fn skip_response_header(name: &str) -> bool {
    matches!(
        name.to_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "transfer-encoding"
            | "upgrade"
            | "proxy-authenticate"
            | "content-length"
    )
}

/// Try to refresh the token for an account and persist to disk, returns new access_token on success
async fn try_refresh_account(account_id: &str, refresh_token: &str) -> Option<String> {
    let token_resp = do_token_refresh(refresh_token).await.ok()?;

    let new_access = token_resp
        .get("access_token")
        .and_then(|v| v.as_str())?
        .to_string();
    let new_refresh = token_resp
        .get("refresh_token")
        .and_then(|v| v.as_str())
        .unwrap_or(refresh_token)
        .to_string();
    let new_id = token_resp
        .get("id_token")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let now_iso = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let auth_path = accounts_dir().join(account_id).join("auth.json");

    // Read existing to preserve account_id field
    let existing: Value = fs::read_to_string(&auth_path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_else(|| serde_json::json!({}));
    let empty = Value::Object(Default::default());
    let old_tokens = existing.get("tokens").unwrap_or(&empty);
    let stored_account_id = old_tokens
        .get("account_id")
        .and_then(|v| v.as_str())
        .unwrap_or(account_id)
        .to_string();

    let updated = serde_json::json!({
        "tokens": {
            "access_token": new_access,
            "id_token": new_id,
            "refresh_token": new_refresh,
            "account_id": stored_account_id,
        },
        "last_refresh": now_iso,
    });

    let _ = fs::write(&auth_path, serde_json::to_string_pretty(&updated).unwrap());

    // Also update ~/.codex/auth.json if this is the active account
    if let Ok(current_content) = fs::read_to_string(auth_file()) {
        if let Ok(current) = serde_json::from_str::<Value>(&current_content) {
            let curr_rt = current
                .pointer("/tokens/refresh_token")
                .and_then(|v| v.as_str());
            if curr_rt == Some(refresh_token) {
                let _ = fs::write(auth_file(), serde_json::to_string_pretty(&updated).unwrap());
            }
        }
    }

    Some(new_access)
}

/// Start a local HTTP server that proxies OpenAI API requests with multi-account round-robin,
/// auto-refresh on 401, cooldown recovery on 429, CORS support, and SSE streaming.
#[tauri::command]
async fn start_api_proxy(port: Option<u16>) -> Result<Value, String> {
    // Stop existing proxy if any
    {
        let mut lock = PROXY_SHUTDOWN.lock().unwrap();
        if let Some(tx) = lock.take() {
            let _ = tx.send(());
        }
    }

    let proxy_port = port.unwrap_or(8520);
    let cfg = proxy_config_snapshot();
    let accounts = match load_proxy_accounts() {
        Ok(accounts) => accounts,
        Err(err) if proxy_custom_openai_ready(&cfg) => {
            log_proxy(&format!("start with custom openai only: {err}"));
            Vec::new()
        }
        Err(err) => {
            log_proxy(&format!("start failed: {err}"));
            return Err(err);
        }
    };
    let account_count = accounts.len();
    log_proxy(&format!(
        "start requested: port={proxy_port} accounts={account_count}"
    ));

    log_proxy("init shutdown channel");
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    {
        let mut lock = PROXY_SHUTDOWN.lock().unwrap();
        *lock = Some(shutdown_tx);
    }

    use axum::{
        body::Body, extract::State, http::StatusCode, response::Response, routing::any, Router,
    };

    log_proxy("building reqwest client");
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .build()
        .map_err(|e| {
            log_proxy(&format!("reqwest client build failed: {e}"));
            e.to_string()
        })?;
    log_proxy("reqwest client ready");

    log_proxy("building proxy state");
    let proxy_state = Arc::new(ProxyState {
        client,
        accounts: Arc::new(RwLock::new(accounts)),
        req_counter: AtomicUsize::new(0),
    });
    log_proxy("proxy state ready");

    async fn retry_usage_limit_across_accounts(
        state: Arc<ProxyState>,
        request_id: usize,
        initial_idx: usize,
        method: &reqwest::Method,
        target: &str,
        forward_headers: &reqwest::header::HeaderMap,
        req_headers: &axum::http::HeaderMap,
        upstream_body_bytes: &Bytes,
        is_stream: bool,
        is_anthropic: bool,
        request_model: &Option<String>,
        estimated_input_tokens: Option<i64>,
        request_body_text: &Option<String>,
        request_headers_json: &Option<String>,
        request_url: &Option<String>,
        anthropic_reverse_tool_map: &Option<HashMap<String, String>>,
        method_label: &str,
        path: &str,
        started_at: std::time::Instant,
    ) -> Option<Response<Body>> {
        let mut attempted: HashSet<usize> = HashSet::new();
        attempted.insert(initial_idx);

        loop {
            let Some((fallback_idx, fallback_id, fallback_token, fallback_account_id)) =
                select_highest_quota_account_excluding(state.clone(), &attempted).await
            else {
                return None;
            };
            attempted.insert(fallback_idx);

            let mut retry_headers = forward_headers.clone();
            apply_upstream_headers(
                &mut retry_headers,
                &fallback_token,
                fallback_account_id.as_deref(),
                req_headers,
                !upstream_body_bytes.is_empty(),
                is_stream,
            );

            let retry_resp = match state
                .client
                .request(method.clone(), target)
                .headers(retry_headers)
                .body(upstream_body_bytes.clone())
                .send()
                .await
            {
                Ok(resp) => resp,
                Err(err) => {
                    log_proxy(&format!(
                        "req#{request_id} usage-limit retry error on {fallback_id}: {err}"
                    ));
                    continue;
                }
            };

            let retry_status = retry_resp.status();
            log_proxy(&format!(
                "req#{request_id} usage-limit retry status: {}",
                retry_status.as_u16()
            ));

            if is_stream {
                if is_anthropic && retry_status.is_success() {
                    let entry = ProxyLogEntry {
                        timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                        method: method_label.to_string(),
                        path: path.to_string(),
                        request_url: request_url.clone(),
                        status: retry_status.as_u16(),
                        duration_ms: started_at.elapsed().as_millis() as u64,
                        proxy_account_id: fallback_id.clone(),
                        account_id: fallback_account_id.clone(),
                        error: None,
                        model: request_model.clone(),
                        request_headers: request_headers_json.clone(),
                        response_headers: headers_to_json_string(vec![(
                            "content-type".to_string(),
                            "text/event-stream".to_string(),
                        )]),
                        request_body: request_body_text.clone(),
                        response_body: None,
                        input_tokens: estimated_input_tokens,
                        output_tokens: None,
                        cache_status: None,
                        cache_key: None,
                        cache_eligible: None,
                        cache_bypass_reason: None,
                        local_cached_input_tokens: None,
                        provider_cached_input_tokens: None,
                    };
                    let reverse_map = anthropic_reverse_tool_map.clone().unwrap_or_default();
                    return Some(
                        build_anthropic_stream_response(retry_resp, reverse_map, entry).await,
                    );
                }

                let entry = ProxyLogEntry {
                    timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                    method: method_label.to_string(),
                    path: path.to_string(),
                    request_url: request_url.clone(),
                    status: retry_status.as_u16(),
                    duration_ms: started_at.elapsed().as_millis() as u64,
                    proxy_account_id: fallback_id.clone(),
                    account_id: fallback_account_id.clone(),
                    error: None,
                    model: request_model.clone(),
                    request_headers: request_headers_json.clone(),
                    response_headers: headers_to_json_string(sanitize_reqwest_headers(
                        retry_resp.headers(),
                    )),
                    request_body: request_body_text.clone(),
                    response_body: None,
                    input_tokens: estimated_input_tokens,
                    output_tokens: None,
                    cache_status: None,
                    cache_key: None,
                    cache_eligible: None,
                    cache_bypass_reason: None,
                    local_cached_input_tokens: None,
                    provider_cached_input_tokens: None,
                };
                let _ = insert_proxy_log(&entry);
                return Some(build_proxy_response(retry_resp).await);
            }

            let resp_hdrs_json =
                headers_to_json_string(sanitize_reqwest_headers(retry_resp.headers()));
            let headers = retry_resp.headers().clone();
            let bytes = retry_resp.bytes().await.unwrap_or_default();

            if (retry_status == reqwest::StatusCode::BAD_REQUEST
                || retry_status == reqwest::StatusCode::TOO_MANY_REQUESTS)
                && parse_usage_limit_error(&bytes).is_some()
            {
                if let Some((resets_at, resets_in_seconds)) = parse_usage_limit_error(&bytes) {
                    let until = usage_limit_cooldown_until(resets_at, resets_in_seconds);
                    apply_usage_limit_policy(&state, fallback_idx, &fallback_id, until);
                    log_proxy(&format!(
                        "req#{request_id} usage_limit_reached on {fallback_id}, retrying next account"
                    ));
                    continue;
                }
            }

            if is_anthropic {
                let reverse_map = anthropic_reverse_tool_map.clone().unwrap_or_default();
                let model_name = request_model.clone().unwrap_or_default();
                let (converted, input_tokens, output_tokens, response_body_text, error_message) =
                    match build_claude_response_or_error_body(
                        retry_status,
                        &bytes,
                        &reverse_map,
                        &model_name,
                    ) {
                        Ok(v) => v,
                        Err(err) => {
                            let entry = ProxyLogEntry {
                                timestamp: chrono::Utc::now()
                                    .format("%Y-%m-%dT%H:%M:%SZ")
                                    .to_string(),
                                method: method_label.to_string(),
                                path: path.to_string(),
                                request_url: request_url.clone(),
                                status: StatusCode::BAD_GATEWAY.as_u16(),
                                duration_ms: started_at.elapsed().as_millis() as u64,
                                proxy_account_id: fallback_id.clone(),
                                account_id: fallback_account_id.clone(),
                                error: Some(err),
                                model: request_model.clone(),
                                request_headers: request_headers_json.clone(),
                                response_headers: resp_hdrs_json.clone(),
                                request_body: request_body_text.clone(),
                                response_body: None,
                                input_tokens: estimated_input_tokens,
                                output_tokens: None,
                                cache_status: None,
                                cache_key: None,
                                cache_eligible: None,
                                cache_bypass_reason: None,
                                local_cached_input_tokens: None,
                                provider_cached_input_tokens: None,
                            };
                            let _ = insert_proxy_log(&entry);
                            return Some(
                                Response::builder()
                                    .status(StatusCode::BAD_GATEWAY)
                                    .header("Access-Control-Allow-Origin", "*")
                                    .body(Body::from("Anthropic response conversion failed"))
                                    .unwrap(),
                            );
                        }
                    };
                let entry = ProxyLogEntry {
                    timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                    method: method_label.to_string(),
                    path: path.to_string(),
                    request_url: request_url.clone(),
                    status: retry_status.as_u16(),
                    duration_ms: started_at.elapsed().as_millis() as u64,
                    proxy_account_id: fallback_id.clone(),
                    account_id: fallback_account_id.clone(),
                    error: error_message,
                    model: request_model.clone(),
                    request_headers: request_headers_json.clone(),
                    response_headers: headers_to_json_string(vec![(
                        "content-type".to_string(),
                        "application/json".to_string(),
                    )]),
                    request_body: request_body_text.clone(),
                    response_body: response_body_text,
                    input_tokens: input_tokens.or(estimated_input_tokens),
                    output_tokens,
                    ..ProxyLogEntry::default()
                };
                let _ = insert_proxy_log(&entry);
                let status = axum::http::StatusCode::from_u16(retry_status.as_u16())
                    .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
                return Some(
                    Response::builder()
                        .status(status)
                        .header("Content-Type", "application/json")
                        .header("Access-Control-Allow-Origin", "*")
                        .header("Access-Control-Allow-Headers", "*")
                        .body(Body::from(converted))
                        .unwrap_or_else(|_| {
                            Response::builder()
                                .status(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
                                .body(Body::empty())
                                .unwrap()
                        }),
                );
            }

            let normalized_bytes =
                maybe_normalize_chat_completion_response_bytes(path, retry_status, &bytes);
            let response_body_text = if normalized_bytes.is_empty() {
                None
            } else {
                Some(truncate_body(&normalized_bytes))
            };
            let (input_tokens, output_tokens) = extract_usage(&normalized_bytes);
            let entry = ProxyLogEntry {
                timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                method: method_label.to_string(),
                path: path.to_string(),
                request_url: request_url.clone(),
                status: retry_status.as_u16(),
                duration_ms: started_at.elapsed().as_millis() as u64,
                proxy_account_id: fallback_id.clone(),
                account_id: fallback_account_id.clone(),
                error: None,
                model: request_model.clone(),
                request_headers: request_headers_json.clone(),
                response_headers: resp_hdrs_json,
                request_body: request_body_text.clone(),
                response_body: response_body_text,
                input_tokens,
                output_tokens,
                ..ProxyLogEntry::default()
            };
            let _ = insert_proxy_log(&entry);
            return Some(build_proxy_response_from_bytes(
                retry_status,
                &headers,
                normalized_bytes,
            ));
        }
    }

    async fn proxy_handler(
        State(state): State<Arc<ProxyState>>,
        req: axum::http::Request<Body>,
    ) -> Response<Body> {
        let request_id = PROXY_REQ_ID.fetch_add(1, Ordering::SeqCst);
        // Handle CORS preflight
        if req.method() == axum::http::Method::OPTIONS {
            return Response::builder()
                .status(StatusCode::NO_CONTENT)
                .header("Access-Control-Allow-Origin", "*")
                .header(
                    "Access-Control-Allow-Methods",
                    "GET, POST, PUT, DELETE, PATCH, OPTIONS",
                )
                .header("Access-Control-Allow-Headers", "*")
                .header("Access-Control-Max-Age", "86400")
                .body(Body::empty())
                .unwrap();
        }

        let req_headers = req.headers().clone();
        let path = req
            .uri()
            .path_and_query()
            .map(|p| p.as_str().to_string())
            .unwrap_or_else(|| "/".to_string());
        let path = normalize_models_path(&path);
        let mut upstream_path = path.to_string();
        let mut is_anthropic = upstream_path.starts_with("/v1/messages");
        let is_count_tokens = upstream_path.starts_with("/v1/messages/count_tokens");
        let method = reqwest::Method::from_bytes(req.method().as_str().as_bytes())
            .unwrap_or(reqwest::Method::GET);
        let method_label = method.to_string();
        let started_at = std::time::Instant::now();
        let mut target = build_upstream_url(&upstream_path);
        let mut request_url = Some(target.clone());

        // Collect and filter incoming headers (pass them through, except hop-by-hop)
        let mut forward_headers = reqwest::header::HeaderMap::new();
        for (k, v) in req.headers() {
            if skip_request_header(k.as_str()) {
                continue;
            }
            if let (Ok(name), Ok(val)) = (
                reqwest::header::HeaderName::from_bytes(k.as_str().as_bytes()),
                reqwest::header::HeaderValue::from_bytes(v.as_bytes()),
            ) {
                forward_headers.insert(name, val);
            }
        }
        let request_headers_json = headers_to_json_string(sanitize_headers(&req_headers));
        let request_body_text = None;

        let max_body_bytes = front_proxy_max_body_bytes();
        if let Some(content_length) = req
            .headers()
            .get(axum::http::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<u64>().ok())
        {
            if content_length > max_body_bytes as u64 {
                let entry = ProxyLogEntry {
                    timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                    method: method_label.clone(),
                    path: path.to_string(),
                    request_url: request_url.clone(),
                    status: StatusCode::PAYLOAD_TOO_LARGE.as_u16(),
                    duration_ms: started_at.elapsed().as_millis() as u64,
                    proxy_account_id: "".to_string(),
                    account_id: None,
                    error: Some("request body too large".to_string()),
                    model: None,
                    request_headers: request_headers_json.clone(),
                    response_headers: headers_to_json_string(vec![(
                        "content-type".to_string(),
                        "text/plain".to_string(),
                    )]),
                    request_body: None,
                    response_body: Some("Request body too large".to_string()),
                    input_tokens: None,
                    output_tokens: None,
                    cache_status: None,
                    cache_key: None,
                    cache_eligible: None,
                    cache_bypass_reason: None,
                    local_cached_input_tokens: None,
                    provider_cached_input_tokens: None,
                };
                let _ = insert_proxy_log(&entry);
                log_proxy_error_detail(
                    request_id,
                    StatusCode::PAYLOAD_TOO_LARGE.as_u16(),
                    &method_label,
                    &path,
                    &request_url,
                    &request_headers_json,
                    &request_body_text,
                    &entry.response_body,
                    &entry.error,
                );
                return Response::builder()
                    .status(StatusCode::PAYLOAD_TOO_LARGE)
                    .header("Access-Control-Allow-Origin", "*")
                    .body(Body::from("Request body too large"))
                    .unwrap();
            }
        }

        // Buffer body (up to max_body_bytes)
        let body_bytes = match axum::body::to_bytes(req.into_body(), max_body_bytes).await {
            Ok(b) => b,
            Err(_) => {
                let entry = ProxyLogEntry {
                    timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                    method: method_label.clone(),
                    path: path.to_string(),
                    request_url: request_url.clone(),
                    status: StatusCode::PAYLOAD_TOO_LARGE.as_u16(),
                    duration_ms: started_at.elapsed().as_millis() as u64,
                    proxy_account_id: "".to_string(),
                    account_id: None,
                    error: Some("request body too large".to_string()),
                    model: None,
                    request_headers: request_headers_json.clone(),
                    response_headers: headers_to_json_string(vec![(
                        "content-type".to_string(),
                        "text/plain".to_string(),
                    )]),
                    request_body: None,
                    response_body: Some("Request body too large".to_string()),
                    input_tokens: None,
                    output_tokens: None,
                    cache_status: None,
                    cache_key: None,
                    cache_eligible: None,
                    cache_bypass_reason: None,
                    local_cached_input_tokens: None,
                    provider_cached_input_tokens: None,
                };
                let _ = insert_proxy_log(&entry);
                log_proxy_error_detail(
                    request_id,
                    StatusCode::PAYLOAD_TOO_LARGE.as_u16(),
                    &method_label,
                    &path,
                    &request_url,
                    &request_headers_json,
                    &request_body_text,
                    &entry.response_body,
                    &entry.error,
                );
                return Response::builder()
                    .status(StatusCode::PAYLOAD_TOO_LARGE)
                    .header("Access-Control-Allow-Origin", "*")
                    .body(Body::from("Request body too large"))
                    .unwrap();
            }
        };

        let mut upstream_body_bytes = body_bytes.clone();
        let mut request_body_text = if body_bytes.is_empty() {
            None
        } else {
            Some(truncate_body(&body_bytes))
        };
        let mut request_model = extract_model(&body_bytes);
        let mut anthropic_reverse_tool_map: Option<HashMap<String, String>> = None;
        let mut anthropic_stream = None;
        let mut anthropic_body_json: Option<Value> = None;
        let mut estimated_input_tokens: Option<i64> = None;
        let cfg = proxy_config_snapshot();
        let (cache_eligible, cache_key, cache_model, _cache_bypass_reason) =
            if cfg.enable_exact_cache {
                evaluate_local_cache_request(&method_label, &path, &body_bytes)
            } else {
                (false, None, None, Some("exact_cache_disabled".to_string()))
            };
        if request_model.is_none() {
            request_model = cache_model.clone();
        }
        if cache_eligible {
            if let Some(key) = cache_key.as_ref() {
                if let Ok(Some(hit)) = lookup_local_cache(key) {
                    let entry = ProxyLogEntry {
                        timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                        method: method_label.clone(),
                        path: path.to_string(),
                        request_url: Some("local:ai-cache".to_string()),
                        status: hit.status,
                        duration_ms: started_at.elapsed().as_millis() as u64,
                        proxy_account_id: "local-cache".to_string(),
                        account_id: None,
                        error: None,
                        model: hit.model.clone().or_else(|| request_model.clone()),
                        request_headers: request_headers_json.clone(),
                        response_headers: headers_to_json_string(vec![
                            ("content-type".to_string(), hit.content_type.clone()),
                            ("x-codex-manager-cache".to_string(), "HIT".to_string()),
                        ]),
                        request_body: request_body_text.clone(),
                        response_body: Some(truncate_body(&hit.body)),
                        input_tokens: Some(hit.input_tokens),
                        output_tokens: Some(hit.output_tokens),
                        cache_status: Some("local_hit".to_string()),
                        cache_key: Some(hit.cache_key.clone()),
                        cache_eligible: Some(true),
                        cache_bypass_reason: None,
                        local_cached_input_tokens: Some(hit.input_tokens),
                        provider_cached_input_tokens: Some(hit.provider_cached_input_tokens),
                    };
                    let _ = insert_proxy_log(&entry);
                    return build_cached_response(hit.status, &hit.content_type, hit.body);
                }
            }
        }

        if is_anthropic {
            if method != reqwest::Method::POST {
                let entry = ProxyLogEntry {
                    timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                    method: method_label.clone(),
                    path: path.to_string(),
                    request_url: request_url.clone(),
                    status: StatusCode::METHOD_NOT_ALLOWED.as_u16(),
                    duration_ms: started_at.elapsed().as_millis() as u64,
                    proxy_account_id: "".to_string(),
                    account_id: None,
                    error: Some("method not allowed".to_string()),
                    model: None,
                    request_headers: request_headers_json.clone(),
                    response_headers: headers_to_json_string(vec![(
                        "content-type".to_string(),
                        "text/plain".to_string(),
                    )]),
                    request_body: request_body_text.clone(),
                    response_body: Some("Method not allowed".to_string()),
                    input_tokens: None,
                    output_tokens: None,
                    cache_status: None,
                    cache_key: None,
                    cache_eligible: None,
                    cache_bypass_reason: None,
                    local_cached_input_tokens: None,
                    provider_cached_input_tokens: None,
                };
                let _ = insert_proxy_log(&entry);
                log_proxy_error_detail(
                    request_id,
                    StatusCode::METHOD_NOT_ALLOWED.as_u16(),
                    &method_label,
                    &path,
                    &request_url,
                    &request_headers_json,
                    &request_body_text,
                    &entry.response_body,
                    &entry.error,
                );
                return Response::builder()
                    .status(StatusCode::METHOD_NOT_ALLOWED)
                    .header("Access-Control-Allow-Origin", "*")
                    .body(Body::from("Method not allowed"))
                    .unwrap();
            }
            let body_json: Value = match serde_json::from_slice(&body_bytes) {
                Ok(v) => v,
                Err(_) => {
                    let entry = ProxyLogEntry {
                        timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                        method: method_label.clone(),
                        path: path.to_string(),
                        request_url: request_url.clone(),
                        status: StatusCode::BAD_REQUEST.as_u16(),
                        duration_ms: started_at.elapsed().as_millis() as u64,
                        proxy_account_id: "".to_string(),
                        account_id: None,
                        error: Some("invalid json".to_string()),
                        model: None,
                        request_headers: request_headers_json.clone(),
                        response_headers: headers_to_json_string(vec![(
                            "content-type".to_string(),
                            "text/plain".to_string(),
                        )]),
                        request_body: request_body_text.clone(),
                        response_body: Some("Invalid JSON".to_string()),
                        input_tokens: None,
                        output_tokens: None,
                        cache_status: None,
                        cache_key: None,
                        cache_eligible: None,
                        cache_bypass_reason: None,
                        local_cached_input_tokens: None,
                        provider_cached_input_tokens: None,
                    };
                    let _ = insert_proxy_log(&entry);
                    log_proxy_error_detail(
                        request_id,
                        StatusCode::BAD_REQUEST.as_u16(),
                        &method_label,
                        &path,
                        &request_url,
                        &request_headers_json,
                        &request_body_text,
                        &entry.response_body,
                        &entry.error,
                    );
                    return Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .header("Access-Control-Allow-Origin", "*")
                        .body(Body::from("Invalid JSON"))
                        .unwrap();
                }
            };
            anthropic_body_json = Some(body_json.clone());
            request_model = body_json
                .get("model")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            if !is_count_tokens {
                match convert_claude_to_codex(&body_json) {
                    Ok((codex_body, reverse_map, stream)) => {
                        estimated_input_tokens = Some(count_codex_input_tokens(&codex_body));
                        request_model = codex_body
                            .get("model")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                        upstream_body_bytes =
                            serde_json::to_vec(&codex_body).unwrap_or_default().into();
                        upstream_path = "/v1/responses".to_string();
                        target = build_upstream_url(&upstream_path);
                        request_url = Some(target.clone());
                        anthropic_reverse_tool_map = Some(reverse_map);
                        anthropic_stream = Some(stream);
                        if let Some(estimate) = estimated_input_tokens {
                            if estimate > 20_000 {
                                log_proxy(&format!(
                                    "req#{request_id} large anthropic request estimated_input_tokens={estimate} request_len={}",
                                    request_body_text.as_ref().map(|v| v.len()).unwrap_or(0)
                                ));
                            }
                        }
                    }
                    Err(err) => {
                        let entry = ProxyLogEntry {
                            timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                            method: method_label.clone(),
                            path: path.to_string(),
                            request_url: request_url.clone(),
                            status: StatusCode::BAD_REQUEST.as_u16(),
                            duration_ms: started_at.elapsed().as_millis() as u64,
                            proxy_account_id: "".to_string(),
                            account_id: None,
                            error: Some(err.clone()),
                            model: request_model.clone(),
                            request_headers: request_headers_json.clone(),
                            response_headers: headers_to_json_string(vec![(
                                "content-type".to_string(),
                                "text/plain".to_string(),
                            )]),
                            request_body: request_body_text.clone(),
                            response_body: Some(err.clone()),
                            input_tokens: None,
                            output_tokens: None,
                            cache_status: None,
                            cache_key: None,
                            cache_eligible: None,
                            cache_bypass_reason: None,
                            local_cached_input_tokens: None,
                            provider_cached_input_tokens: None,
                        };
                        let _ = insert_proxy_log(&entry);
                        log_proxy_error_detail(
                            request_id,
                            StatusCode::BAD_REQUEST.as_u16(),
                            &method_label,
                            &path,
                            &request_url,
                            &request_headers_json,
                            &request_body_text,
                            &entry.response_body,
                            &entry.error,
                        );
                        return Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .header("Access-Control-Allow-Origin", "*")
                            .body(Body::from(err))
                            .unwrap();
                    }
                }
            }
        } else {
            target = build_upstream_url(&upstream_path);
            request_url = Some(target.clone());
        }

        log_proxy(&format!(
            "req#{request_id} start {method_label} {path} -> {target}"
        ));

        if !proxy_api_key_valid(&req_headers) {
            let entry = ProxyLogEntry {
                timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                method: method_label.clone(),
                path: path.to_string(),
                request_url: request_url.clone(),
                status: StatusCode::UNAUTHORIZED.as_u16(),
                duration_ms: started_at.elapsed().as_millis() as u64,
                proxy_account_id: "".to_string(),
                account_id: None,
                error: Some("missing or invalid api key".to_string()),
                model: request_model.clone(),
                request_headers: request_headers_json.clone(),
                response_headers: None,
                request_body: request_body_text.clone(),
                response_body: None,
                input_tokens: None,
                output_tokens: None,
                cache_status: None,
                cache_key: None,
                cache_eligible: None,
                cache_bypass_reason: None,
                local_cached_input_tokens: None,
                provider_cached_input_tokens: None,
            };
            let _ = insert_proxy_log(&entry);
            return Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .header("Access-Control-Allow-Origin", "*")
                .body(Body::from("Unauthorized"))
                .unwrap();
        }

        if is_anthropic && is_count_tokens {
            let body_json = match anthropic_body_json.as_ref() {
                Some(v) => v,
                None => {
                    return Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .header("Access-Control-Allow-Origin", "*")
                        .body(Body::from("Invalid JSON"))
                        .unwrap();
                }
            };
            let codex_body = match convert_claude_to_codex(body_json) {
                Ok((v, _, _)) => v,
                Err(err) => {
                    return Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .header("Access-Control-Allow-Origin", "*")
                        .body(Body::from(err))
                        .unwrap();
                }
            };
            let count = count_codex_input_tokens(&codex_body);
            let response_payload = serde_json::json!({ "input_tokens": count });
            let response_body =
                serde_json::to_vec(&response_payload).unwrap_or_else(|_| b"{}".to_vec());
            let entry = ProxyLogEntry {
                timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                method: method_label.clone(),
                path: path.to_string(),
                request_url: Some("local:count_tokens".to_string()),
                status: StatusCode::OK.as_u16(),
                duration_ms: started_at.elapsed().as_millis() as u64,
                proxy_account_id: "".to_string(),
                account_id: None,
                error: None,
                model: request_model.clone(),
                request_headers: request_headers_json.clone(),
                response_headers: headers_to_json_string(vec![(
                    "content-type".to_string(),
                    "application/json".to_string(),
                )]),
                request_body: request_body_text.clone(),
                response_body: Some(truncate_body(&response_body)),
                input_tokens: Some(count),
                output_tokens: Some(0),
                cache_status: None,
                cache_key: None,
                cache_eligible: None,
                cache_bypass_reason: None,
                local_cached_input_tokens: None,
                provider_cached_input_tokens: None,
            };
            let _ = insert_proxy_log(&entry);
            return Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/json")
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Headers", "*")
                .body(Body::from(response_body))
                .unwrap_or_else(|_| {
                    Response::builder()
                        .status(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::empty())
                        .unwrap()
                });
        }

        let proxy_cfg = proxy_config_snapshot();
        if !is_anthropic && proxy_uses_custom_openai(&proxy_cfg) {
            let Some(custom_base_url) = custom_openai_base_url(&proxy_cfg) else {
                return Response::builder()
                    .status(StatusCode::SERVICE_UNAVAILABLE)
                    .header("Access-Control-Allow-Origin", "*")
                    .body(Body::from("自定义 OpenAI 地址未配置"))
                    .unwrap();
            };
            let Some(custom_api_key) = custom_openai_api_key(&proxy_cfg) else {
                return Response::builder()
                    .status(StatusCode::SERVICE_UNAVAILABLE)
                    .header("Access-Control-Allow-Origin", "*")
                    .body(Body::from("自定义 OpenAI API Key 未配置"))
                    .unwrap();
            };

            let configured_models = configured_proxy_models(&proxy_cfg);
            if upstream_path == "/v1/models" || upstream_path.starts_with("/v1/models?") {
                if configured_models.is_empty() {
                    return Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .header("Content-Type", "application/json")
                        .header("Access-Control-Allow-Origin", "*")
                        .header("Access-Control-Allow-Headers", "*")
                        .body(Body::from(
                            serde_json::to_vec(&serde_json::json!({
                                "error": {
                                    "message": "请先在代理设置中填写模型名称，例如 glm-5。",
                                    "type": "invalid_request_error"
                                }
                            }))
                            .unwrap_or_else(|_| {
                                "{\"error\":{\"message\":\"model_not_configured\"}}"
                                    .as_bytes()
                                    .to_vec()
                            }),
                        ))
                        .unwrap();
                }
                let response_body = build_local_models_response(&configured_models);
                let entry = ProxyLogEntry {
                    timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                    method: method_label.clone(),
                    path: path.to_string(),
                    request_url: Some("local:/v1/models".to_string()),
                    status: StatusCode::OK.as_u16(),
                    duration_ms: started_at.elapsed().as_millis() as u64,
                    proxy_account_id: "custom-openai".to_string(),
                    account_id: None,
                    error: None,
                    model: configured_models.first().cloned(),
                    request_headers: request_headers_json.clone(),
                    response_headers: headers_to_json_string(vec![(
                        "content-type".to_string(),
                        "application/json".to_string(),
                    )]),
                    request_body: request_body_text.clone(),
                    response_body: Some(truncate_body(&response_body)),
                    input_tokens: None,
                    output_tokens: None,
                    cache_status: None,
                    cache_key: None,
                    cache_eligible: None,
                    cache_bypass_reason: None,
                    local_cached_input_tokens: None,
                    provider_cached_input_tokens: None,
                };
                let _ = insert_proxy_log(&entry);
                return Response::builder()
                    .status(StatusCode::OK)
                    .header("Content-Type", "application/json")
                    .header("Access-Control-Allow-Origin", "*")
                    .header("Access-Control-Allow-Headers", "*")
                    .body(Body::from(response_body))
                    .unwrap();
            }

            let custom_responses_path =
                upstream_path == "/v1/responses" || upstream_path.starts_with("/v1/responses?");
            let mut custom_request_json: Option<Value> = None;
            let empty_request_json = serde_json::json!({});
            let mut custom_stream = forward_headers
                .get(reqwest::header::ACCEPT)
                .and_then(|v| v.to_str().ok())
                .map(|v| v.contains("text/event-stream"))
                .unwrap_or(false);
            if custom_responses_path {
                if method != reqwest::Method::POST {
                    return Response::builder()
                        .status(StatusCode::METHOD_NOT_ALLOWED)
                        .header("Access-Control-Allow-Origin", "*")
                        .body(Body::from("Method not allowed"))
                        .unwrap();
                }
                let body_json: Value = match serde_json::from_slice(&body_bytes) {
                    Ok(v) => v,
                    Err(err) => {
                        return Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .header("Access-Control-Allow-Origin", "*")
                            .body(Body::from(format!("Invalid JSON: {err}")))
                            .unwrap();
                    }
                };
                match convert_responses_request_to_chat_completions(&body_json, &proxy_cfg) {
                    Ok((chat_body, stream)) => {
                        request_model = chat_body
                            .get("model")
                            .and_then(|v| v.as_str())
                            .map(|v| v.to_string());
                        custom_stream = stream;
                        request_body_text = Some(truncate_body(
                            &serde_json::to_vec(&chat_body).unwrap_or_default(),
                        ));
                        upstream_body_bytes =
                            serde_json::to_vec(&chat_body).unwrap_or_default().into();
                        custom_request_json = Some(chat_body);
                        upstream_path = "/v1/chat/completions".to_string();
                    }
                    Err(err) => {
                        return Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .header("Access-Control-Allow-Origin", "*")
                            .body(Body::from(err))
                            .unwrap();
                    }
                }
            }

            target = build_upstream_url_with_base(&custom_base_url, &upstream_path);
            request_url = Some(target.clone());
            log_proxy(&format!(
                "req#{request_id} start {method_label} {path} -> {target}"
            ));

            let mut custom_headers = reqwest::header::HeaderMap::new();
            apply_custom_openai_headers(
                &mut custom_headers,
                &custom_api_key,
                !upstream_body_bytes.is_empty(),
                custom_stream,
            );

            let upstream_resp = match state
                .client
                .request(method.clone(), &target)
                .headers(custom_headers)
                .body(upstream_body_bytes.clone())
                .send()
                .await
            {
                Ok(resp) => resp,
                Err(err) => {
                    let entry = ProxyLogEntry {
                        timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                        method: method_label.clone(),
                        path: path.to_string(),
                        request_url: request_url.clone(),
                        status: StatusCode::BAD_GATEWAY.as_u16(),
                        duration_ms: started_at.elapsed().as_millis() as u64,
                        proxy_account_id: "custom-openai".to_string(),
                        account_id: None,
                        error: Some(err.to_string()),
                        model: request_model.clone(),
                        request_headers: request_headers_json.clone(),
                        response_headers: None,
                        request_body: request_body_text.clone(),
                        response_body: None,
                        input_tokens: None,
                        output_tokens: None,
                        cache_status: None,
                        cache_key: None,
                        cache_eligible: None,
                        cache_bypass_reason: None,
                        local_cached_input_tokens: None,
                        provider_cached_input_tokens: None,
                    };
                    let _ = insert_proxy_log(&entry);
                    return Response::builder()
                        .status(StatusCode::BAD_GATEWAY)
                        .header("Access-Control-Allow-Origin", "*")
                        .body(Body::from(format!("Upstream error: {err}")))
                        .unwrap();
                }
            };

            let upstream_status = upstream_resp.status();
            log_proxy(&format!(
                "req#{request_id} upstream status: {}",
                upstream_status.as_u16()
            ));

            if custom_responses_path && custom_stream && upstream_status.is_success() {
                let entry = ProxyLogEntry {
                    timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                    method: method_label.clone(),
                    path: path.to_string(),
                    request_url: request_url.clone(),
                    status: upstream_status.as_u16(),
                    duration_ms: started_at.elapsed().as_millis() as u64,
                    proxy_account_id: "custom-openai".to_string(),
                    account_id: None,
                    error: None,
                    model: request_model.clone(),
                    request_headers: request_headers_json.clone(),
                    response_headers: headers_to_json_string(vec![(
                        "content-type".to_string(),
                        "text/event-stream".to_string(),
                    )]),
                    request_body: request_body_text.clone(),
                    response_body: None,
                    input_tokens: None,
                    output_tokens: None,
                    cache_status: None,
                    cache_key: None,
                    cache_eligible: None,
                    cache_bypass_reason: None,
                    local_cached_input_tokens: None,
                    provider_cached_input_tokens: None,
                };
                return build_custom_openai_stream_response(
                    upstream_resp,
                    custom_request_json.unwrap_or_else(|| empty_request_json.clone()),
                    entry,
                )
                .await;
            }

            let response_headers_json =
                headers_to_json_string(sanitize_reqwest_headers(upstream_resp.headers()));
            let headers = upstream_resp.headers().clone();
            let bytes = upstream_resp.bytes().await.unwrap_or_default();

            if custom_responses_path && upstream_status.is_success() {
                match convert_chat_completions_non_stream_to_responses(
                    custom_request_json.as_ref().unwrap_or(&empty_request_json),
                    &bytes,
                ) {
                    Ok((converted, input_tokens, output_tokens, response_body_text)) => {
                        let entry = ProxyLogEntry {
                            timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                            method: method_label.clone(),
                            path: path.to_string(),
                            request_url: request_url.clone(),
                            status: upstream_status.as_u16(),
                            duration_ms: started_at.elapsed().as_millis() as u64,
                            proxy_account_id: "custom-openai".to_string(),
                            account_id: None,
                            error: None,
                            model: request_model.clone(),
                            request_headers: request_headers_json.clone(),
                            response_headers: headers_to_json_string(vec![(
                                "content-type".to_string(),
                                "application/json".to_string(),
                            )]),
                            request_body: request_body_text.clone(),
                            response_body: response_body_text,
                            input_tokens,
                            output_tokens,
                            ..ProxyLogEntry::default()
                        };
                        let _ = insert_proxy_log(&entry);
                        return Response::builder()
                            .status(
                                axum::http::StatusCode::from_u16(upstream_status.as_u16())
                                    .unwrap_or(StatusCode::OK),
                            )
                            .header("Content-Type", "application/json")
                            .header("Access-Control-Allow-Origin", "*")
                            .header("Access-Control-Allow-Headers", "*")
                            .body(Body::from(converted))
                            .unwrap();
                    }
                    Err(err) => {
                        let entry = ProxyLogEntry {
                            timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                            method: method_label.clone(),
                            path: path.to_string(),
                            request_url: request_url.clone(),
                            status: StatusCode::BAD_GATEWAY.as_u16(),
                            duration_ms: started_at.elapsed().as_millis() as u64,
                            proxy_account_id: "custom-openai".to_string(),
                            account_id: None,
                            error: Some(err.clone()),
                            model: request_model.clone(),
                            request_headers: request_headers_json.clone(),
                            response_headers: response_headers_json.clone(),
                            request_body: request_body_text.clone(),
                            response_body: Some(truncate_body(&bytes)),
                            input_tokens: None,
                            output_tokens: None,
                            cache_status: None,
                            cache_key: None,
                            cache_eligible: None,
                            cache_bypass_reason: None,
                            local_cached_input_tokens: None,
                            provider_cached_input_tokens: None,
                        };
                        let _ = insert_proxy_log(&entry);
                        return Response::builder()
                            .status(StatusCode::BAD_GATEWAY)
                            .header("Access-Control-Allow-Origin", "*")
                            .body(Body::from(err))
                            .unwrap();
                    }
                }
            }

            let normalized_bytes =
                maybe_normalize_chat_completion_response_bytes(&upstream_path, upstream_status, &bytes);
            let response_body_text = if normalized_bytes.is_empty() {
                None
            } else {
                Some(truncate_body(&normalized_bytes))
            };
            let (input_tokens, output_tokens) = extract_usage(&normalized_bytes);
            let entry = ProxyLogEntry {
                timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                method: method_label.clone(),
                path: path.to_string(),
                request_url: request_url.clone(),
                status: upstream_status.as_u16(),
                duration_ms: started_at.elapsed().as_millis() as u64,
                proxy_account_id: "custom-openai".to_string(),
                account_id: None,
                error: None,
                model: request_model.clone(),
                request_headers: request_headers_json.clone(),
                response_headers: response_headers_json,
                request_body: request_body_text.clone(),
                response_body: response_body_text,
                input_tokens,
                output_tokens,
                ..ProxyLogEntry::default()
            };
            let _ = insert_proxy_log(&entry);
            return build_proxy_response_from_bytes(upstream_status, &headers, normalized_bytes);
        }

        // Pick a healthy account (skip cooldown-expired accounts, revive if cooldown elapsed)
        let (chosen_token, chosen_account_id, chosen_idx, chosen_id, chosen_refresh) = {
            let now = std::time::Instant::now();
            let mut accounts_lock = state.accounts.write().unwrap();
            let pool_size = accounts_lock.len();

            if pool_size == 0 {
                return Response::builder()
                    .status(StatusCode::SERVICE_UNAVAILABLE)
                    .header("Access-Control-Allow-Origin", "*")
                    .body(Body::from("No accounts in pool"))
                    .unwrap();
            }

            // Revive any accounts whose cooldown has elapsed
            for acc in accounts_lock.iter_mut() {
                if let AccountHealth::Cooldown(until) = &acc.health {
                    if now >= *until {
                        acc.health = AccountHealth::Active;
                    }
                }
            }

            let start_count = state.req_counter.fetch_add(1, Ordering::SeqCst);
            let mut found = None;
            for i in 0..pool_size {
                let idx = (start_count + i) % pool_size;
                if accounts_lock[idx].health == AccountHealth::Active {
                    found = Some((
                        accounts_lock[idx].access_token.clone(),
                        accounts_lock[idx].account_id.clone(),
                        idx,
                        accounts_lock[idx].id.clone(),
                        accounts_lock[idx].refresh_token.clone(),
                    ));
                    break;
                }
            }

            match found {
                Some(f) => f,
                None => {
                    return Response::builder()
                        .status(StatusCode::TOO_MANY_REQUESTS)
                        .header("Access-Control-Allow-Origin", "*")
                        .header("Retry-After", "60")
                        .body(Body::from("All accounts are rate-limited or blocked"))
                        .unwrap();
                }
            }
        };

        // Send request upstream with the chosen account's token
        let mut is_stream = forward_headers
            .get(reqwest::header::ACCEPT)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.contains("text/event-stream"))
            .unwrap_or(false);
        if let Some(stream) = anthropic_stream {
            is_stream = stream;
        }
        let mut upstream_headers = forward_headers.clone();
        apply_upstream_headers(
            &mut upstream_headers,
            &chosen_token,
            chosen_account_id.as_deref(),
            &req_headers,
            !upstream_body_bytes.is_empty(),
            is_stream,
        );

        let upstream_result = state
            .client
            .request(method.clone(), &target)
            .headers(upstream_headers)
            .body(upstream_body_bytes.clone())
            .send()
            .await;

        let upstream_resp = match upstream_result {
            Ok(r) => r,
            Err(e) => {
                log_proxy(&format!("req#{request_id} upstream error: {e}"));
                let entry = ProxyLogEntry {
                    timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                    method: method_label.clone(),
                    path: path.to_string(),
                    request_url: request_url.clone(),
                    status: StatusCode::BAD_GATEWAY.as_u16(),
                    duration_ms: started_at.elapsed().as_millis() as u64,
                    proxy_account_id: chosen_id.clone(),
                    account_id: chosen_account_id.clone(),
                    error: Some(format!("{e}")),
                    model: request_model.clone(),
                    request_headers: request_headers_json.clone(),
                    response_headers: None,
                    request_body: request_body_text.clone(),
                    response_body: None,
                    input_tokens: None,
                    output_tokens: None,
                    cache_status: None,
                    cache_key: None,
                    cache_eligible: None,
                    cache_bypass_reason: None,
                    local_cached_input_tokens: None,
                    provider_cached_input_tokens: None,
                };
                let _ = insert_proxy_log(&entry);
                return Response::builder()
                    .status(StatusCode::BAD_GATEWAY)
                    .header("Access-Control-Allow-Origin", "*")
                    .body(Body::from(format!("Upstream error: {e}")))
                    .unwrap();
            }
        };

        let upstream_status = upstream_resp.status();
        log_proxy(&format!(
            "req#{request_id} upstream status: {}",
            upstream_status.as_u16()
        ));

        if is_stream && upstream_status == reqwest::StatusCode::BAD_REQUEST {
            let headers = upstream_resp.headers().clone();
            let bytes = upstream_resp.bytes().await.unwrap_or_default();
            if let Some((resets_at, resets_in_seconds)) = parse_usage_limit_error(&bytes) {
                let until = usage_limit_cooldown_until(resets_at, resets_in_seconds);
                apply_usage_limit_policy(&state, chosen_idx, &chosen_id, until);
                log_proxy(&format!("req#{request_id} usage_limit_reached on {chosen_id} (status 400), selecting highest quota account"));
                if let Some(resp) = retry_usage_limit_across_accounts(
                    state.clone(),
                    request_id,
                    chosen_idx,
                    &method,
                    &target,
                    &forward_headers,
                    &req_headers,
                    &upstream_body_bytes,
                    is_stream,
                    is_anthropic,
                    &request_model,
                    estimated_input_tokens,
                    &request_body_text,
                    &request_headers_json,
                    &request_url,
                    &anthropic_reverse_tool_map,
                    &method_label,
                    &path,
                    started_at,
                )
                .await
                {
                    return resp;
                }
            }
            return build_proxy_response_from_bytes(upstream_status, &headers, bytes);
        }

        // Handle 401: try token refresh once, then retry
        if upstream_status == reqwest::StatusCode::UNAUTHORIZED {
            if let Some(rt) = &chosen_refresh {
                if let Some(new_token) = try_refresh_account(&chosen_id, rt).await {
                    // Update pool with new token
                    {
                        let mut accounts_lock = state.accounts.write().unwrap();
                        if let Some(acc) = accounts_lock.get_mut(chosen_idx) {
                            acc.access_token = new_token.clone();
                            acc.health = AccountHealth::Active;
                        }
                    }
                    // Retry with refreshed token
                    let mut retry_headers = forward_headers.clone();
                    apply_upstream_headers(
                        &mut retry_headers,
                        &new_token,
                        chosen_account_id.as_deref(),
                        &req_headers,
                        !upstream_body_bytes.is_empty(),
                        is_stream,
                    );
                    if let Ok(retry_resp) = state
                        .client
                        .request(method.clone(), &target)
                        .headers(retry_headers)
                        .body(upstream_body_bytes.clone())
                        .send()
                        .await
                    {
                        let response_headers_json =
                            headers_to_json_string(sanitize_reqwest_headers(retry_resp.headers()));
                        if !is_stream {
                            let status = retry_resp.status();
                            let headers = retry_resp.headers().clone();
                            let bytes = match retry_resp.bytes().await {
                                Ok(b) => b,
                                Err(e) => {
                                    let entry = ProxyLogEntry {
                                        timestamp: chrono::Utc::now()
                                            .format("%Y-%m-%dT%H:%M:%SZ")
                                            .to_string(),
                                        method: method_label.clone(),
                                        path: path.to_string(),
                                        request_url: request_url.clone(),
                                        status: StatusCode::BAD_GATEWAY.as_u16(),
                                        duration_ms: started_at.elapsed().as_millis() as u64,
                                        proxy_account_id: chosen_id.clone(),
                                        account_id: chosen_account_id.clone(),
                                        error: Some(format!("{e}")),
                                        model: request_model.clone(),
                                        request_headers: request_headers_json.clone(),
                                        response_headers: response_headers_json.clone(),
                                        request_body: request_body_text.clone(),
                                        response_body: None,
                                        input_tokens: None,
                                        output_tokens: None,
                                        cache_status: None,
                                        cache_key: None,
                                        cache_eligible: None,
                                        cache_bypass_reason: None,
                                        local_cached_input_tokens: None,
                                        provider_cached_input_tokens: None,
                                    };
                                    let _ = insert_proxy_log(&entry);
                                    return Response::builder()
                                        .status(StatusCode::BAD_GATEWAY)
                                        .header("Access-Control-Allow-Origin", "*")
                                        .body(Body::from(format!("Upstream error: {e}")))
                                        .unwrap();
                                }
                            };
                            if is_anthropic {
                                let reverse_map =
                                    anthropic_reverse_tool_map.clone().unwrap_or_default();
                                let model_name = request_model.clone().unwrap_or_default();
                                let (
                                    converted,
                                    input_tokens,
                                    output_tokens,
                                    response_body_text,
                                    error_message,
                                ) = match build_claude_response_or_error_body(
                                        status,
                                        &bytes,
                                        &reverse_map,
                                        &model_name,
                                    ) {
                                        Ok(v) => v,
                                        Err(err) => {
                                            let entry = ProxyLogEntry {
                                                timestamp: chrono::Utc::now()
                                                    .format("%Y-%m-%dT%H:%M:%SZ")
                                                    .to_string(),
                                                method: method_label.clone(),
                                                path: path.to_string(),
                                                request_url: request_url.clone(),
                                                status: StatusCode::BAD_GATEWAY.as_u16(),
                                                duration_ms: started_at.elapsed().as_millis()
                                                    as u64,
                                                proxy_account_id: chosen_id.clone(),
                                                account_id: chosen_account_id.clone(),
                                                error: Some(err),
                                                model: request_model.clone(),
                                                request_headers: request_headers_json.clone(),
                                                response_headers: response_headers_json.clone(),
                                                request_body: request_body_text.clone(),
                                                response_body: None,
                                                input_tokens: estimated_input_tokens,
                                                output_tokens: None,
                                                cache_status: None,
                                                cache_key: None,
                                                cache_eligible: None,
                                                cache_bypass_reason: None,
                                                local_cached_input_tokens: None,
                                                provider_cached_input_tokens: None,
                                            };
                                            let _ = insert_proxy_log(&entry);
                                            return Response::builder()
                                                .status(StatusCode::BAD_GATEWAY)
                                                .header("Access-Control-Allow-Origin", "*")
                                                .body(Body::from(
                                                    "Anthropic response conversion failed",
                                                ))
                                                .unwrap();
                                        }
                                    };
                                let entry = ProxyLogEntry {
                                    timestamp: chrono::Utc::now()
                                        .format("%Y-%m-%dT%H:%M:%SZ")
                                        .to_string(),
                                    method: method_label.clone(),
                                    path: path.to_string(),
                                    request_url: request_url.clone(),
                                    status: status.as_u16(),
                                    duration_ms: started_at.elapsed().as_millis() as u64,
                                    proxy_account_id: chosen_id.clone(),
                                    account_id: chosen_account_id.clone(),
                                    error: error_message,
                                    model: request_model.clone(),
                                    request_headers: request_headers_json.clone(),
                                    response_headers: headers_to_json_string(vec![(
                                        "content-type".to_string(),
                                        "application/json".to_string(),
                                    )]),
                                    request_body: request_body_text.clone(),
                                    response_body: response_body_text,
                                    input_tokens: input_tokens.or(estimated_input_tokens),
                                    output_tokens,
                                    ..ProxyLogEntry::default()
                                };
                                let _ = insert_proxy_log(&entry);
                                let status = axum::http::StatusCode::from_u16(status.as_u16())
                                    .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
                                return Response::builder()
                                    .status(status)
                                    .header("Content-Type", "application/json")
                                    .header("Access-Control-Allow-Origin", "*")
                                    .header("Access-Control-Allow-Headers", "*")
                                    .body(Body::from(converted))
                                    .unwrap_or_else(|_| {
                                        Response::builder()
                                            .status(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
                                            .body(Body::empty())
                                            .unwrap()
                                    });
                            }
                            let response_body_text = if bytes.is_empty() {
                                None
                            } else {
                                Some(truncate_body(&bytes))
                            };
                            let (input_tokens, output_tokens) = extract_usage(&bytes);
                            let entry = ProxyLogEntry {
                                timestamp: chrono::Utc::now()
                                    .format("%Y-%m-%dT%H:%M:%SZ")
                                    .to_string(),
                                method: method_label.clone(),
                                path: path.to_string(),
                                request_url: request_url.clone(),
                                status: status.as_u16(),
                                duration_ms: started_at.elapsed().as_millis() as u64,
                                proxy_account_id: chosen_id.clone(),
                                account_id: chosen_account_id.clone(),
                                error: None,
                                model: request_model.clone(),
                                request_headers: request_headers_json.clone(),
                                response_headers: response_headers_json.clone(),
                                request_body: request_body_text.clone(),
                                response_body: response_body_text,
                                input_tokens,
                                output_tokens,
                                ..ProxyLogEntry::default()
                            };
                            let _ = insert_proxy_log(&entry);
                            return build_proxy_response_from_bytes(status, &headers, bytes);
                        }
                        if is_anthropic {
                            let entry = ProxyLogEntry {
                                timestamp: chrono::Utc::now()
                                    .format("%Y-%m-%dT%H:%M:%SZ")
                                    .to_string(),
                                method: method_label.clone(),
                                path: path.to_string(),
                                request_url: request_url.clone(),
                                status: retry_resp.status().as_u16(),
                                duration_ms: started_at.elapsed().as_millis() as u64,
                                proxy_account_id: chosen_id.clone(),
                                account_id: chosen_account_id.clone(),
                                error: None,
                                model: request_model.clone(),
                                request_headers: request_headers_json.clone(),
                                response_headers: headers_to_json_string(vec![(
                                    "content-type".to_string(),
                                    "text/event-stream".to_string(),
                                )]),
                                request_body: request_body_text.clone(),
                                response_body: None,
                                input_tokens: None,
                                output_tokens: None,
                                cache_status: None,
                                cache_key: None,
                                cache_eligible: None,
                                cache_bypass_reason: None,
                                local_cached_input_tokens: None,
                                provider_cached_input_tokens: None,
                            };
                            let reverse_map =
                                anthropic_reverse_tool_map.clone().unwrap_or_default();
                            return build_anthropic_stream_response(retry_resp, reverse_map, entry)
                                .await;
                        }

                        let entry = ProxyLogEntry {
                            timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                            method: method_label.clone(),
                            path: path.to_string(),
                            request_url: request_url.clone(),
                            status: retry_resp.status().as_u16(),
                            duration_ms: started_at.elapsed().as_millis() as u64,
                            proxy_account_id: chosen_id.clone(),
                            account_id: chosen_account_id.clone(),
                            error: None,
                            model: request_model.clone(),
                            request_headers: request_headers_json.clone(),
                            response_headers: response_headers_json,
                            request_body: request_body_text.clone(),
                            response_body: None,
                            input_tokens: None,
                            output_tokens: None,
                            cache_status: None,
                            cache_key: None,
                            cache_eligible: None,
                            cache_bypass_reason: None,
                            local_cached_input_tokens: None,
                            provider_cached_input_tokens: None,
                        };
                        let _ = insert_proxy_log(&entry);
                        return build_proxy_response(retry_resp).await;
                    }
                }
            }
            // Refresh failed or no refresh token → mark blocked
            {
                let mut accounts_lock = state.accounts.write().unwrap();
                if let Some(acc) = accounts_lock.get_mut(chosen_idx) {
                    acc.health = AccountHealth::Blocked;
                }
            }
        } else if upstream_status == reqwest::StatusCode::FORBIDDEN {
            let mut accounts_lock = state.accounts.write().unwrap();
            if let Some(acc) = accounts_lock.get_mut(chosen_idx) {
                acc.health = AccountHealth::Blocked;
            }
        } else if upstream_status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let headers = upstream_resp.headers().clone();
            let bytes = match upstream_resp.bytes().await {
                Ok(b) => b,
                Err(e) => {
                    let entry = ProxyLogEntry {
                        timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                        method: method_label.clone(),
                        path: path.to_string(),
                        request_url: request_url.clone(),
                        status: StatusCode::BAD_GATEWAY.as_u16(),
                        duration_ms: started_at.elapsed().as_millis() as u64,
                        proxy_account_id: chosen_id.clone(),
                        account_id: chosen_account_id.clone(),
                        error: Some(format!("{e}")),
                        model: request_model.clone(),
                        request_headers: request_headers_json.clone(),
                        response_headers: None,
                        request_body: request_body_text.clone(),
                        response_body: None,
                        input_tokens: None,
                        output_tokens: None,
                        cache_status: None,
                        cache_key: None,
                        cache_eligible: None,
                        cache_bypass_reason: None,
                        local_cached_input_tokens: None,
                        provider_cached_input_tokens: None,
                    };
                    let _ = insert_proxy_log(&entry);
                    return Response::builder()
                        .status(StatusCode::BAD_GATEWAY)
                        .header("Access-Control-Allow-Origin", "*")
                        .body(Body::from(format!("Upstream error: {e}")))
                        .unwrap();
                }
            };

            let usage_limit = parse_usage_limit_error(&bytes);
            if let Some((resets_at, resets_in_seconds)) = usage_limit {
                let until = usage_limit_cooldown_until(resets_at, resets_in_seconds);
                apply_usage_limit_policy(&state, chosen_idx, &chosen_id, until);
                log_proxy(&format!("req#{request_id} usage_limit_reached on {chosen_id}, selecting highest quota account"));
                if let Some(resp) = retry_usage_limit_across_accounts(
                    state.clone(),
                    request_id,
                    chosen_idx,
                    &method,
                    &target,
                    &forward_headers,
                    &req_headers,
                    &upstream_body_bytes,
                    is_stream,
                    is_anthropic,
                    &request_model,
                    estimated_input_tokens,
                    &request_body_text,
                    &request_headers_json,
                    &request_url,
                    &anthropic_reverse_tool_map,
                    &method_label,
                    &path,
                    started_at,
                )
                .await
                {
                    return resp;
                }
            } else {
                let until =
                    std::time::Instant::now() + std::time::Duration::from_secs(COOLDOWN_SECS);
                {
                    let mut accounts_lock = state.accounts.write().unwrap();
                    if let Some(acc) = accounts_lock.get_mut(chosen_idx) {
                        acc.health = AccountHealth::Cooldown(until);
                    }
                }
                log_proxy(&format!(
                    "req#{request_id} 429 on {chosen_id}, trying another account"
                ));

                // Pick a different healthy account from the pool and retry immediately
                let fallback = {
                    let now = std::time::Instant::now();
                    let mut accounts_lock = state.accounts.write().unwrap();
                    for acc in accounts_lock.iter_mut() {
                        if let AccountHealth::Cooldown(u) = &acc.health {
                            if now >= *u {
                                acc.health = AccountHealth::Active;
                            }
                        }
                    }
                    let pool_size = accounts_lock.len();
                    let start = state.req_counter.load(Ordering::SeqCst);
                    let mut found = None;
                    for i in 0..pool_size {
                        let idx = (start + i) % pool_size;
                        if idx != chosen_idx && accounts_lock[idx].health == AccountHealth::Active {
                            found = Some((
                                accounts_lock[idx].access_token.clone(),
                                accounts_lock[idx].account_id.clone(),
                            ));
                            break;
                        }
                    }
                    found
                };

                if let Some((fallback_token, fallback_account_id)) = fallback {
                    let mut retry_headers = forward_headers.clone();
                    apply_upstream_headers(
                        &mut retry_headers,
                        &fallback_token,
                        fallback_account_id.as_deref(),
                        &req_headers,
                        !upstream_body_bytes.is_empty(),
                        is_stream,
                    );
                    if let Ok(retry_resp) = state
                        .client
                        .request(method.clone(), &target)
                        .headers(retry_headers)
                        .body(upstream_body_bytes.clone())
                        .send()
                        .await
                    {
                        let retry_status = retry_resp.status();
                        log_proxy(&format!(
                            "req#{request_id} 429-retry status: {}",
                            retry_status.as_u16()
                        ));
                        let resp_hdrs_json =
                            headers_to_json_string(sanitize_reqwest_headers(retry_resp.headers()));
                        if !is_stream {
                            let headers = retry_resp.headers().clone();
                            let bytes = retry_resp.bytes().await.unwrap_or_default();
                            if is_anthropic {
                                let reverse_map =
                                    anthropic_reverse_tool_map.clone().unwrap_or_default();
                                let model_name = request_model.clone().unwrap_or_default();
                                let (
                                    converted,
                                    input_tokens,
                                    output_tokens,
                                    response_body_text,
                                    error_message,
                                ) = match build_claude_response_or_error_body(
                                        retry_status,
                                        &bytes,
                                        &reverse_map,
                                        &model_name,
                                    ) {
                                        Ok(v) => v,
                                        Err(err) => {
                                            let entry = ProxyLogEntry {
                                                timestamp: chrono::Utc::now()
                                                    .format("%Y-%m-%dT%H:%M:%SZ")
                                                    .to_string(),
                                                method: method_label.clone(),
                                                path: path.to_string(),
                                                request_url: request_url.clone(),
                                                status: StatusCode::BAD_GATEWAY.as_u16(),
                                                duration_ms: started_at.elapsed().as_millis()
                                                    as u64,
                                                proxy_account_id: chosen_id.clone(),
                                                account_id: fallback_account_id.clone(),
                                                error: Some(err),
                                                model: request_model.clone(),
                                                request_headers: request_headers_json.clone(),
                                                response_headers: resp_hdrs_json.clone(),
                                                request_body: request_body_text.clone(),
                                                response_body: None,
                                                input_tokens: estimated_input_tokens,
                                                output_tokens: None,
                                                cache_status: None,
                                                cache_key: None,
                                                cache_eligible: None,
                                                cache_bypass_reason: None,
                                                local_cached_input_tokens: None,
                                                provider_cached_input_tokens: None,
                                            };
                                            let _ = insert_proxy_log(&entry);
                                            return Response::builder()
                                                .status(StatusCode::BAD_GATEWAY)
                                                .header("Access-Control-Allow-Origin", "*")
                                                .body(Body::from(
                                                    "Anthropic response conversion failed",
                                                ))
                                                .unwrap();
                                        }
                                    };
                                let entry = ProxyLogEntry {
                                    timestamp: chrono::Utc::now()
                                        .format("%Y-%m-%dT%H:%M:%SZ")
                                        .to_string(),
                                    method: method_label.clone(),
                                    path: path.to_string(),
                                    request_url: request_url.clone(),
                                    status: retry_status.as_u16(),
                                    duration_ms: started_at.elapsed().as_millis() as u64,
                                    proxy_account_id: chosen_id.clone(),
                                    account_id: fallback_account_id.clone(),
                                    error: error_message,
                                    model: request_model.clone(),
                                    request_headers: request_headers_json.clone(),
                                    response_headers: headers_to_json_string(vec![(
                                        "content-type".to_string(),
                                        "application/json".to_string(),
                                    )]),
                                    request_body: request_body_text.clone(),
                                    response_body: response_body_text,
                                    input_tokens: input_tokens.or(estimated_input_tokens),
                                    output_tokens,
                                    ..ProxyLogEntry::default()
                                };
                                let _ = insert_proxy_log(&entry);
                                let status =
                                    axum::http::StatusCode::from_u16(retry_status.as_u16())
                                        .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
                                return Response::builder()
                                    .status(status)
                                    .header("Content-Type", "application/json")
                                    .header("Access-Control-Allow-Origin", "*")
                                    .header("Access-Control-Allow-Headers", "*")
                                    .body(Body::from(converted))
                                    .unwrap_or_else(|_| {
                                        Response::builder()
                                            .status(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
                                            .body(Body::empty())
                                            .unwrap()
                                    });
                            }
                            let response_body_text = if bytes.is_empty() {
                                None
                            } else {
                                Some(truncate_body(&bytes))
                            };
                            let (input_tokens, output_tokens) = extract_usage(&bytes);
                            let entry = ProxyLogEntry {
                                timestamp: chrono::Utc::now()
                                    .format("%Y-%m-%dT%H:%M:%SZ")
                                    .to_string(),
                                method: method_label.clone(),
                                path: path.to_string(),
                                request_url: request_url.clone(),
                                status: retry_status.as_u16(),
                                duration_ms: started_at.elapsed().as_millis() as u64,
                                proxy_account_id: chosen_id.clone(),
                                account_id: fallback_account_id.clone(),
                                error: None,
                                model: request_model.clone(),
                                request_headers: request_headers_json.clone(),
                                response_headers: resp_hdrs_json,
                                request_body: request_body_text.clone(),
                                response_body: response_body_text,
                                input_tokens,
                                output_tokens,
                                ..ProxyLogEntry::default()
                            };
                            let _ = insert_proxy_log(&entry);
                            return build_proxy_response_from_bytes(retry_status, &headers, bytes);
                        }
                        if is_anthropic {
                            let entry = ProxyLogEntry {
                                timestamp: chrono::Utc::now()
                                    .format("%Y-%m-%dT%H:%M:%SZ")
                                    .to_string(),
                                method: method_label.clone(),
                                path: path.to_string(),
                                request_url: request_url.clone(),
                                status: retry_resp.status().as_u16(),
                                duration_ms: started_at.elapsed().as_millis() as u64,
                                proxy_account_id: chosen_id.clone(),
                                account_id: fallback_account_id.clone(),
                                error: None,
                                model: request_model.clone(),
                                request_headers: request_headers_json.clone(),
                                response_headers: headers_to_json_string(vec![(
                                    "content-type".to_string(),
                                    "text/event-stream".to_string(),
                                )]),
                                request_body: request_body_text.clone(),
                                response_body: None,
                                input_tokens: None,
                                output_tokens: None,
                                cache_status: None,
                                cache_key: None,
                                cache_eligible: None,
                                cache_bypass_reason: None,
                                local_cached_input_tokens: None,
                                provider_cached_input_tokens: None,
                            };
                            let reverse_map =
                                anthropic_reverse_tool_map.clone().unwrap_or_default();
                            return build_anthropic_stream_response(retry_resp, reverse_map, entry)
                                .await;
                        }
                        let entry = ProxyLogEntry {
                            timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                            method: method_label.clone(),
                            path: path.to_string(),
                            request_url: request_url.clone(),
                            status: retry_resp.status().as_u16(),
                            duration_ms: started_at.elapsed().as_millis() as u64,
                            proxy_account_id: chosen_id.clone(),
                            account_id: fallback_account_id.clone(),
                            error: None,
                            model: request_model.clone(),
                            request_headers: request_headers_json.clone(),
                            response_headers: resp_hdrs_json,
                            request_body: request_body_text.clone(),
                            response_body: None,
                            input_tokens: None,
                            output_tokens: None,
                            cache_status: None,
                            cache_key: None,
                            cache_eligible: None,
                            cache_bypass_reason: None,
                            local_cached_input_tokens: None,
                            provider_cached_input_tokens: None,
                        };
                        let _ = insert_proxy_log(&entry);
                        return build_proxy_response(retry_resp).await;
                    }
                }
            }

            let response_body_text = if bytes.is_empty() {
                None
            } else {
                Some(truncate_body(&bytes))
            };
            let (input_tokens, output_tokens) = extract_usage(&bytes);
            let entry = ProxyLogEntry {
                timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                method: method_label.clone(),
                path: path.to_string(),
                request_url: request_url.clone(),
                status: upstream_status.as_u16(),
                duration_ms: started_at.elapsed().as_millis() as u64,
                proxy_account_id: chosen_id.clone(),
                account_id: chosen_account_id.clone(),
                error: None,
                model: request_model.clone(),
                request_headers: request_headers_json.clone(),
                response_headers: headers_to_json_string(sanitize_reqwest_headers(&headers)),
                request_body: request_body_text.clone(),
                response_body: response_body_text,
                input_tokens,
                output_tokens,
                ..ProxyLogEntry::default()
            };
            let _ = insert_proxy_log(&entry);
            return build_proxy_response_from_bytes(upstream_status, &headers, bytes);
        }

        let response_headers_json =
            headers_to_json_string(sanitize_reqwest_headers(upstream_resp.headers()));
        if !is_stream {
            let status = upstream_resp.status();
            let headers = upstream_resp.headers().clone();
            let bytes = match upstream_resp.bytes().await {
                Ok(b) => b,
                Err(e) => {
                    let entry = ProxyLogEntry {
                        timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                        method: method_label.clone(),
                        path: path.to_string(),
                        request_url: request_url.clone(),
                        status: StatusCode::BAD_GATEWAY.as_u16(),
                        duration_ms: started_at.elapsed().as_millis() as u64,
                        proxy_account_id: chosen_id.clone(),
                        account_id: chosen_account_id.clone(),
                        error: Some(format!("{e}")),
                        model: request_model.clone(),
                        request_headers: request_headers_json.clone(),
                        response_headers: response_headers_json.clone(),
                        request_body: request_body_text.clone(),
                        response_body: None,
                        input_tokens: None,
                        output_tokens: None,
                        cache_status: None,
                        cache_key: None,
                        cache_eligible: None,
                        cache_bypass_reason: None,
                        local_cached_input_tokens: None,
                        provider_cached_input_tokens: None,
                    };
                    let _ = insert_proxy_log(&entry);
                    return Response::builder()
                        .status(StatusCode::BAD_GATEWAY)
                        .header("Access-Control-Allow-Origin", "*")
                        .body(Body::from(format!("Upstream error: {e}")))
                        .unwrap();
                }
            };
            if status == reqwest::StatusCode::BAD_REQUEST {
                if let Some((resets_at, resets_in_seconds)) = parse_usage_limit_error(&bytes) {
                    let until = usage_limit_cooldown_until(resets_at, resets_in_seconds);
                    apply_usage_limit_policy(&state, chosen_idx, &chosen_id, until);
                    log_proxy(&format!(
                        "req#{request_id} usage_limit_reached on {chosen_id} (status 400), selecting highest quota account"
                    ));
                    if let Some(resp) = retry_usage_limit_across_accounts(
                        state.clone(),
                        request_id,
                        chosen_idx,
                        &method,
                        &target,
                        &forward_headers,
                        &req_headers,
                        &upstream_body_bytes,
                        is_stream,
                        is_anthropic,
                        &request_model,
                        estimated_input_tokens,
                        &request_body_text,
                        &request_headers_json,
                        &request_url,
                        &anthropic_reverse_tool_map,
                        &method_label,
                        &path,
                        started_at,
                    )
                    .await
                    {
                        return resp;
                    }
                }
            }
            if is_anthropic {
                let reverse_map = anthropic_reverse_tool_map.clone().unwrap_or_default();
                let model_name = request_model.clone().unwrap_or_default();
                let (converted, input_tokens, output_tokens, response_body_text, error_message) =
                    match build_claude_response_or_error_body(
                        status,
                        &bytes,
                        &reverse_map,
                        &model_name,
                    ) {
                        Ok(v) => v,
                        Err(err) => {
                            let entry = ProxyLogEntry {
                                timestamp: chrono::Utc::now()
                                    .format("%Y-%m-%dT%H:%M:%SZ")
                                    .to_string(),
                                method: method_label.clone(),
                                path: path.to_string(),
                                request_url: request_url.clone(),
                                status: StatusCode::BAD_GATEWAY.as_u16(),
                                duration_ms: started_at.elapsed().as_millis() as u64,
                                proxy_account_id: chosen_id.clone(),
                                account_id: chosen_account_id.clone(),
                                error: Some(err),
                                model: request_model.clone(),
                                request_headers: request_headers_json.clone(),
                                response_headers: response_headers_json.clone(),
                                request_body: request_body_text.clone(),
                                response_body: None,
                                input_tokens: estimated_input_tokens,
                                output_tokens: None,
                                cache_status: None,
                                cache_key: None,
                                cache_eligible: None,
                                cache_bypass_reason: None,
                                local_cached_input_tokens: None,
                                provider_cached_input_tokens: None,
                            };
                            let _ = insert_proxy_log(&entry);
                            return Response::builder()
                                .status(StatusCode::BAD_GATEWAY)
                                .header("Access-Control-Allow-Origin", "*")
                                .body(Body::from("Anthropic response conversion failed"))
                                .unwrap();
                        }
                    };
                let entry = ProxyLogEntry {
                    timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                    method: method_label.clone(),
                    path: path.to_string(),
                    request_url: request_url.clone(),
                    status: status.as_u16(),
                    duration_ms: started_at.elapsed().as_millis() as u64,
                    proxy_account_id: chosen_id.clone(),
                    account_id: chosen_account_id.clone(),
                    error: error_message,
                    model: request_model.clone(),
                    request_headers: request_headers_json.clone(),
                    response_headers: headers_to_json_string(vec![(
                        "content-type".to_string(),
                        "application/json".to_string(),
                    )]),
                    request_body: request_body_text.clone(),
                    response_body: response_body_text,
                    input_tokens: input_tokens.or(estimated_input_tokens),
                    output_tokens,
                    ..ProxyLogEntry::default()
                };
                let _ = insert_proxy_log(&entry);
                let status = axum::http::StatusCode::from_u16(status.as_u16())
                    .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
                return Response::builder()
                    .status(status)
                    .header("Content-Type", "application/json")
                    .header("Access-Control-Allow-Origin", "*")
                    .header("Access-Control-Allow-Headers", "*")
                    .body(Body::from(converted))
                    .unwrap_or_else(|_| {
                        Response::builder()
                            .status(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
                            .body(Body::empty())
                            .unwrap()
                    });
            }
            let response_body_text = if bytes.is_empty() {
                None
            } else {
                Some(truncate_body(&bytes))
            };
            let (input_tokens, output_tokens) = extract_usage(&bytes);
            let entry = ProxyLogEntry {
                timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                method: method_label.clone(),
                path: path.to_string(),
                request_url: request_url.clone(),
                status: status.as_u16(),
                duration_ms: started_at.elapsed().as_millis() as u64,
                proxy_account_id: chosen_id.clone(),
                account_id: chosen_account_id.clone(),
                error: None,
                model: request_model.clone(),
                request_headers: request_headers_json.clone(),
                response_headers: response_headers_json.clone(),
                request_body: request_body_text.clone(),
                response_body: response_body_text,
                input_tokens,
                output_tokens,
                ..ProxyLogEntry::default()
            };
            let _ = insert_proxy_log(&entry);
            return build_proxy_response_from_bytes(status, &headers, bytes);
        }

        if is_anthropic && upstream_status.is_success() {
            let entry = ProxyLogEntry {
                timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                method: method_label.clone(),
                path: path.to_string(),
                request_url: request_url.clone(),
                status: upstream_status.as_u16(),
                duration_ms: started_at.elapsed().as_millis() as u64,
                proxy_account_id: chosen_id.clone(),
                account_id: chosen_account_id.clone(),
                error: None,
                model: request_model.clone(),
                request_headers: request_headers_json.clone(),
                response_headers: headers_to_json_string(vec![(
                    "content-type".to_string(),
                    "text/event-stream".to_string(),
                )]),
                request_body: request_body_text.clone(),
                response_body: None,
                input_tokens: estimated_input_tokens,
                output_tokens: None,
                cache_status: None,
                cache_key: None,
                cache_eligible: None,
                cache_bypass_reason: None,
                local_cached_input_tokens: None,
                provider_cached_input_tokens: None,
            };
            let reverse_map = anthropic_reverse_tool_map.clone().unwrap_or_default();
            return build_anthropic_stream_response(upstream_resp, reverse_map, entry).await;
        }

        let entry = ProxyLogEntry {
            timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            method: method_label.clone(),
            path: path.to_string(),
            request_url: request_url.clone(),
            status: upstream_status.as_u16(),
            duration_ms: started_at.elapsed().as_millis() as u64,
            proxy_account_id: chosen_id.clone(),
            account_id: chosen_account_id.clone(),
            error: None,
            model: request_model.clone(),
            request_headers: request_headers_json.clone(),
            response_headers: response_headers_json,
            request_body: request_body_text.clone(),
            response_body: None,
            input_tokens: None,
            output_tokens: None,
            cache_status: None,
            cache_key: None,
            cache_eligible: None,
            cache_bypass_reason: None,
            local_cached_input_tokens: None,
            provider_cached_input_tokens: None,
        };
        let _ = insert_proxy_log(&entry);
        build_proxy_response(upstream_resp).await
    }

    async fn build_proxy_response(upstream_resp: reqwest::Response) -> Response<Body> {
        let upstream_status = upstream_resp.status();
        let status = axum::http::StatusCode::from_u16(upstream_status.as_u16())
            .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);

        let mut builder = Response::builder().status(status);
        for (k, v) in upstream_resp.headers() {
            if skip_response_header(k.as_str()) {
                continue;
            }
            if let (Ok(name), Ok(val)) = (
                axum::http::HeaderName::from_bytes(k.as_str().as_bytes()),
                axum::http::HeaderValue::from_bytes(v.as_bytes()),
            ) {
                builder = builder.header(name, val);
            }
        }
        builder = builder
            .header("Access-Control-Allow-Origin", "*")
            .header("Access-Control-Allow-Headers", "*");

        let stream = upstream_resp.bytes_stream();
        builder.body(Body::from_stream(stream)).unwrap_or_else(|_| {
            Response::builder()
                .status(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::empty())
                .unwrap()
        })
    }

    fn build_proxy_response_from_bytes(
        status: reqwest::StatusCode,
        headers: &reqwest::header::HeaderMap,
        body: Bytes,
    ) -> Response<Body> {
        let status = axum::http::StatusCode::from_u16(status.as_u16())
            .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        let mut builder = Response::builder().status(status);
        for (k, v) in headers.iter() {
            if skip_response_header(k.as_str()) {
                continue;
            }
            if let (Ok(name), Ok(val)) = (
                axum::http::HeaderName::from_bytes(k.as_str().as_bytes()),
                axum::http::HeaderValue::from_bytes(v.as_bytes()),
            ) {
                builder = builder.header(name, val);
            }
        }
        builder = builder
            .header("Access-Control-Allow-Origin", "*")
            .header("Access-Control-Allow-Headers", "*");
        builder.body(Body::from(body)).unwrap_or_else(|_| {
            Response::builder()
                .status(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::empty())
                .unwrap()
        })
    }

    fn build_cached_response(status: u16, content_type: &str, body: Bytes) -> Response<Body> {
        Response::builder()
            .status(axum::http::StatusCode::from_u16(status).unwrap_or(StatusCode::OK))
            .header("Content-Type", content_type)
            .header("X-Codex-Manager-Cache", "HIT")
            .header("Access-Control-Allow-Origin", "*")
            .header("Access-Control-Allow-Headers", "*")
            .body(Body::from(body))
            .unwrap_or_else(|_| {
                Response::builder()
                    .status(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::empty())
                    .unwrap()
            })
    }

    log_proxy("building router");
    let app = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        Router::new()
            .fallback(any(proxy_handler))
            .with_state(proxy_state.clone())
    })) {
        Ok(app) => app,
        Err(_) => {
            log_proxy("router build panicked");
            return Err("Router build panicked".to_string());
        }
    };
    log_proxy("router ready");

    log_proxy("router ready");

    let addr = format!("localhost:{proxy_port}");
    log_proxy(&format!("starting proxy server: addr={addr}"));
    let shutdown_notify = Arc::new(Notify::new());
    let shutdown_waiter = shutdown_notify.clone();
    tauri::async_runtime::spawn(async move {
        let _ = shutdown_rx.await;
        shutdown_waiter.notify_waiters();
    });

    // Store live state before running server.
    {
        let mut lock = PROXY_STATE.lock().unwrap();
        *lock = Some(proxy_state.clone());
    }
    {
        let mut lock = PROXY_PORT.lock().unwrap();
        *lock = Some(proxy_port);
    }

    tauri::async_runtime::spawn(async move {
        let serve_result = run_proxy_server(&addr, app, shutdown_notify).await;

        if let Err(err) = serve_result {
            log_proxy(&format!("server exited with error: {err}"));
        } else {
            log_proxy("server exited");
        }

        let mut lock = PROXY_PORT.lock().unwrap();
        *lock = None;
        let mut lock = PROXY_STATE.lock().unwrap();
        *lock = None;
    });

    Ok(serde_json::json!({
        "success": true,
        "port": proxy_port,
        "base_url": format!("http://127.0.0.1:{proxy_port}"),
        "account_count": account_count
    }))
}

#[tauri::command]
async fn start_anthropic_proxy(port: Option<u16>) -> Result<Value, String> {
    start_api_proxy(port).await
}

#[tauri::command]
fn stop_anthropic_proxy() -> Result<Value, String> {
    stop_api_proxy()
}

#[tauri::command]
fn get_anthropic_proxy_status() -> Result<Value, String> {
    let port = *PROXY_PORT.lock().unwrap();
    Ok(serde_json::json!({ "running": port.is_some(), "port": port }))
}

#[tauri::command]
fn list_openai_compat_configs() -> Result<Vec<OpenAICompatProviderConfig>, String> {
    Ok(load_openai_compat_configs())
}

#[tauri::command]
fn create_openai_compat_config(
    provider_name: String,
    base_url: String,
    api_key: String,
    default_model: Option<String>,
    model_mappings: Vec<OpenAICompatModelMapping>,
) -> Result<OpenAICompatProviderConfig, String> {
    let mut configs = load_openai_compat_configs();
    let config = build_openai_compat_provider_config(
        None,
        provider_name,
        base_url,
        api_key,
        default_model,
        model_mappings,
        None,
    )?;
    log_proxy(&format!(
        "openai compat config create provider={} base_url={}",
        config.provider_name, config.base_url
    ));
    configs.push(config.clone());
    save_openai_compat_configs(&configs)?;
    Ok(config)
}

#[tauri::command]
fn update_openai_compat_config(
    id: String,
    provider_name: String,
    base_url: String,
    api_key: String,
    default_model: Option<String>,
    model_mappings: Vec<OpenAICompatModelMapping>,
) -> Result<OpenAICompatProviderConfig, String> {
    let mut configs = load_openai_compat_configs();
    let idx = configs
        .iter()
        .position(|cfg| cfg.id == id)
        .ok_or_else(|| "配置不存在".to_string())?;
    let created_at = configs[idx].created_at;
    let config = build_openai_compat_provider_config(
        Some(id),
        provider_name,
        base_url,
        api_key,
        default_model,
        model_mappings,
        Some(created_at),
    )?;
    log_proxy(&format!(
        "openai compat config update id={} provider={} base_url={}",
        config.id, config.provider_name, config.base_url
    ));
    configs[idx] = config.clone();
    save_openai_compat_configs(&configs)?;
    if let Some(state) = OPENAI_COMPAT_PROXY_STATE.lock().unwrap().clone() {
        let mut lock = state.config.write().unwrap();
        if lock.id == config.id {
            *lock = config.clone();
        }
    }
    Ok(config)
}

#[tauri::command]
fn delete_openai_compat_config(id: String) -> Result<bool, String> {
    let mut configs = load_openai_compat_configs();
    let before = configs.len();
    configs.retain(|cfg| cfg.id != id);
    if configs.len() == before {
        return Ok(false);
    }
    save_openai_compat_configs(&configs)?;
    Ok(true)
}

pub fn create_temporary_openai_compat_config(
    provider_name: String,
    base_url: String,
    api_key: String,
    model: String,
) -> Result<String, String> {
    let mut configs = load_openai_compat_configs();
    let config = build_openai_compat_provider_config(
        None,
        provider_name,
        base_url,
        api_key,
        Some(model.clone()),
        vec![OpenAICompatModelMapping {
            alias: "*".to_string(),
            provider_model: model,
        }],
        None,
    )?;
    let id = config.id.clone();
    configs.push(config);
    save_openai_compat_configs(&configs)?;
    Ok(id)
}

pub fn remove_openai_compat_config(id: String) -> Result<bool, String> {
    delete_openai_compat_config(id)
}

#[tauri::command]
async fn list_openai_compat_provider_models(config_id: String) -> Result<Vec<String>, String> {
    let config = load_openai_compat_configs()
        .into_iter()
        .find(|cfg| cfg.id == config_id)
        .ok_or_else(|| "配置不存在".to_string())?;
    let client = reqwest::Client::new();
    let url = build_upstream_url_with_base(&config.base_url, "/v1/models");
    let mut headers = reqwest::header::HeaderMap::new();
    apply_custom_openai_headers(&mut headers, &config.api_key, false, false);
    log_proxy(&format!("openai-compat models request -> {url}"));
    let resp = client
        .get(&url)
        .headers(headers)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let status = resp.status().as_u16();
    let body_bytes = resp.bytes().await.map_err(|e| e.to_string())?;
    let body_text = truncate_body(&body_bytes);
    log_proxy(&format!(
        "openai-compat models status {status} body={body_text}"
    ));
    if (200..300).contains(&status) {
        let body: Value = serde_json::from_slice(&body_bytes).map_err(|e| e.to_string())?;
        let mut models: Vec<String> = Vec::new();
        if let Some(arr) = body.get("data").and_then(|v| v.as_array()) {
            for item in arr {
                for field in ["id", "model", "slug"] {
                    if let Some(model) = item
                        .get(field)
                        .and_then(|v| v.as_str())
                        .filter(|v| !v.is_empty())
                    {
                        models.push(model.to_string());
                        break;
                    }
                }
            }
        }
        models.extend(openai_compat_exposed_models(&config));
        models.sort();
        models.dedup();
        if !models.is_empty() {
            return Ok(models);
        }
    }
    let fallback = openai_compat_exposed_models(&config);
    if fallback.is_empty() {
        return Err("模型列表为空，请先配置默认模型或模型映射".to_string());
    }
    Ok(fallback)
}

#[tauri::command]
async fn probe_openai_compat_config(config_id: String) -> Result<OpenAICompatProbeResult, String> {
    let config = load_openai_compat_configs()
        .into_iter()
        .find(|cfg| cfg.id == config_id)
        .ok_or_else(|| "配置不存在".to_string())?;
    probe_openai_compat_upstream(&config).await
}

pub async fn start_openai_compat_proxy_runtime(
    config_id: String,
    port: Option<u16>,
) -> Result<Value, String> {
    {
        let mut lock = OPENAI_COMPAT_PROXY_SHUTDOWN.lock().unwrap();
        if let Some(tx) = lock.take() {
            let _ = tx.send(());
        }
    }

    let config = load_openai_compat_configs()
        .into_iter()
        .find(|cfg| cfg.id == config_id)
        .ok_or_else(|| "配置不存在".to_string())?;
    let proxy_port = port.unwrap_or(8521);

    use axum::{
        body::Body, extract::State, http::StatusCode, response::Response, routing::any, Router,
    };

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .build()
        .map_err(|e| e.to_string())?;
    let proxy_state = Arc::new(OpenAICompatProxyState {
        client,
        config: Arc::new(RwLock::new(config.clone())),
    });

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    {
        let mut lock = OPENAI_COMPAT_PROXY_SHUTDOWN.lock().unwrap();
        *lock = Some(shutdown_tx);
    }

    async fn openai_compat_proxy_handler(
        State(state): State<Arc<OpenAICompatProxyState>>,
        req: axum::http::Request<Body>,
    ) -> Response<Body> {
        if req.method() == axum::http::Method::OPTIONS {
            return Response::builder()
                .status(StatusCode::NO_CONTENT)
                .header("Access-Control-Allow-Origin", "*")
                .header(
                    "Access-Control-Allow-Methods",
                    "GET, POST, PUT, DELETE, PATCH, OPTIONS",
                )
                .header("Access-Control-Allow-Headers", "*")
                .header("Access-Control-Max-Age", "86400")
                .body(Body::empty())
                .unwrap();
        }

        let config = state.config.read().unwrap().clone();
        let request_id = PROXY_REQ_ID.fetch_add(1, Ordering::SeqCst);
        let req_headers = req.headers().clone();
        if !proxy_api_key_valid(&req_headers) {
            return Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .header("Access-Control-Allow-Origin", "*")
                .body(Body::from("Unauthorized"))
                .unwrap();
        }

        let path = req
            .uri()
            .path_and_query()
            .map(|p| p.as_str().to_string())
            .unwrap_or_else(|| "/".to_string());
        let method = reqwest::Method::from_bytes(req.method().as_str().as_bytes())
            .unwrap_or(reqwest::Method::GET);
        let method_label = method.to_string();
        let started_at = std::time::Instant::now();
        let request_headers_json = headers_to_json_string(sanitize_headers(&req_headers));

        if path == "/v1/models" || path.starts_with("/v1/models?") {
            let models = openai_compat_exposed_models(&config);
            let response_body = build_local_models_response(&models);
            let entry = ProxyLogEntry {
                timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                method: method_label,
                path: path.clone(),
                request_url: Some("local:/v1/models".to_string()),
                status: StatusCode::OK.as_u16(),
                duration_ms: started_at.elapsed().as_millis() as u64,
                proxy_account_id: config.provider_name.clone(),
                account_id: None,
                error: None,
                model: models.first().cloned(),
                request_headers: request_headers_json,
                response_headers: headers_to_json_string(vec![(
                    "content-type".to_string(),
                    "application/json".to_string(),
                )]),
                request_body: None,
                response_body: Some(truncate_body(&response_body)),
                input_tokens: None,
                output_tokens: None,
                cache_status: None,
                cache_key: None,
                cache_eligible: None,
                cache_bypass_reason: None,
                local_cached_input_tokens: None,
                provider_cached_input_tokens: None,
            };
            let _ = insert_proxy_log(&entry);
            return Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/json")
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Headers", "*")
                .body(Body::from(response_body))
                .unwrap();
        }

        let body_bytes =
            match axum::body::to_bytes(req.into_body(), front_proxy_max_body_bytes()).await {
                Ok(b) => b,
                Err(_) => {
                    return Response::builder()
                        .status(StatusCode::PAYLOAD_TOO_LARGE)
                        .header("Access-Control-Allow-Origin", "*")
                        .body(Body::from("Request body too large"))
                        .unwrap();
                }
            };
        let mut request_body_text = if body_bytes.is_empty() {
            None
        } else {
            Some(truncate_body(&body_bytes))
        };
        let mut request_model = extract_model(&body_bytes);
        let mut upstream_path = path.clone();
        let is_anthropic =
            upstream_path == "/v1/messages" || upstream_path.starts_with("/v1/messages?");
        let is_anthropic_count_tokens = upstream_path == "/v1/messages/count_tokens"
            || upstream_path.starts_with("/v1/messages/count_tokens?");
        let custom_responses_path =
            upstream_path == "/v1/responses" || upstream_path.starts_with("/v1/responses?");
        let mut upstream_body_bytes = body_bytes.clone();
        let mut is_stream = req_headers
            .get(axum::http::header::ACCEPT)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.contains("text/event-stream"))
            .unwrap_or(false);
        let mut translated_request_json: Option<Value> = None;
        let mut anthropic_reverse_tool_map: Option<HashMap<String, String>> = None;
        let mut estimated_input_tokens: Option<i64> = None;
        let proxy_cfg = proxy_config_snapshot();
        let (cache_eligible, cache_key, cache_model, _cache_bypass_reason) =
            if proxy_cfg.enable_exact_cache {
                evaluate_local_cache_request(&method_label, &path, &body_bytes)
            } else {
                (false, None, None, Some("exact_cache_disabled".to_string()))
            };
        if request_model.is_none() {
            request_model = cache_model.clone();
        }
        if cache_eligible {
            if let Some(key) = cache_key.as_ref() {
                if let Ok(Some(hit)) = lookup_local_cache(key) {
                    let entry = ProxyLogEntry {
                        timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                        method: method_label.clone(),
                        path: path.clone(),
                        request_url: Some("local:ai-cache".to_string()),
                        status: hit.status,
                        duration_ms: started_at.elapsed().as_millis() as u64,
                        proxy_account_id: "local-cache".to_string(),
                        account_id: None,
                        error: None,
                        model: hit.model.clone().or_else(|| request_model.clone()),
                        request_headers: request_headers_json.clone(),
                        response_headers: headers_to_json_string(vec![
                            ("content-type".to_string(), hit.content_type.clone()),
                            ("x-codex-manager-cache".to_string(), "HIT".to_string()),
                        ]),
                        request_body: request_body_text.clone(),
                        response_body: Some(truncate_body(&hit.body)),
                        input_tokens: Some(hit.input_tokens),
                        output_tokens: Some(hit.output_tokens),
                        cache_status: Some("local_hit".to_string()),
                        cache_key: Some(hit.cache_key.clone()),
                        cache_eligible: Some(true),
                        cache_bypass_reason: None,
                        local_cached_input_tokens: Some(hit.input_tokens),
                        provider_cached_input_tokens: Some(hit.provider_cached_input_tokens),
                    };
                    let _ = insert_proxy_log(&entry);
                    return Response::builder()
                        .status(StatusCode::from_u16(hit.status).unwrap_or(StatusCode::OK))
                        .header("Content-Type", hit.content_type)
                        .header("X-Codex-Manager-Cache", "HIT")
                        .header("Access-Control-Allow-Origin", "*")
                        .header("Access-Control-Allow-Headers", "*")
                        .body(Body::from(hit.body))
                        .unwrap();
                }
            }
        }

        if is_anthropic_count_tokens {
            let body_json: Value = match serde_json::from_slice(&body_bytes) {
                Ok(v) => v,
                Err(err) => {
                    return Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .header("Access-Control-Allow-Origin", "*")
                        .body(Body::from(format!("Invalid JSON: {err}")))
                        .unwrap();
                }
            };
            let (codex_body, _, _) = match convert_claude_to_codex(&body_json) {
                Ok(v) => v,
                Err(err) => {
                    return Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .header("Access-Control-Allow-Origin", "*")
                        .body(Body::from(err))
                        .unwrap();
                }
            };
            let count = count_codex_input_tokens(&codex_body);
            let response_body = serde_json::to_vec(&serde_json::json!({ "input_tokens": count }))
                .unwrap_or_else(|_| b"{\"input_tokens\":0}".to_vec());
            let entry = ProxyLogEntry {
                timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                method: method_label.clone(),
                path: path.clone(),
                request_url: Some("local:/v1/messages/count_tokens".to_string()),
                status: StatusCode::OK.as_u16(),
                duration_ms: started_at.elapsed().as_millis() as u64,
                proxy_account_id: config.provider_name.clone(),
                account_id: None,
                error: None,
                model: body_json
                    .get("model")
                    .and_then(|v| v.as_str())
                    .map(|v| v.to_string()),
                request_headers: request_headers_json.clone(),
                response_headers: headers_to_json_string(vec![(
                    "content-type".to_string(),
                    "application/json".to_string(),
                )]),
                request_body: request_body_text.clone(),
                response_body: Some(truncate_body(&response_body)),
                input_tokens: Some(count),
                output_tokens: Some(0),
                cache_status: None,
                cache_key: None,
                cache_eligible: None,
                cache_bypass_reason: None,
                local_cached_input_tokens: None,
                provider_cached_input_tokens: None,
            };
            let _ = insert_proxy_log(&entry);
            return Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/json")
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Headers", "*")
                .body(Body::from(response_body))
                .unwrap();
        }

        if is_anthropic {
            if method != reqwest::Method::POST {
                return Response::builder()
                    .status(StatusCode::METHOD_NOT_ALLOWED)
                    .header("Access-Control-Allow-Origin", "*")
                    .body(Body::from("Method not allowed"))
                    .unwrap();
            }
            let body_json: Value = match serde_json::from_slice(&body_bytes) {
                Ok(v) => v,
                Err(err) => {
                    return Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .header("Access-Control-Allow-Origin", "*")
                        .body(Body::from(format!("Invalid JSON: {err}")))
                        .unwrap();
                }
            };
            let (mut responses_request, reverse_map, stream) =
                match convert_claude_to_codex(&body_json) {
                    Ok(v) => v,
                    Err(err) => {
                        return Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .header("Access-Control-Allow-Origin", "*")
                            .body(Body::from(err))
                            .unwrap();
                    }
                };
            estimated_input_tokens = Some(count_codex_input_tokens(&responses_request));
            let mapped_model = map_openai_compat_model(
                &config,
                responses_request.get("model").and_then(|v| v.as_str()),
            )
            .ok_or_else(|| "请先配置默认模型或模型映射".to_string())
            .unwrap_or_default();
            let mut temp_cfg = proxy_config_snapshot();
            temp_cfg.model_override = Some(mapped_model);
            let (mut chat_request, _) = match convert_responses_request_to_chat_completions(
                &responses_request,
                &temp_cfg,
            ) {
                Ok(v) => v,
                Err(err) => {
                    return Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .header("Access-Control-Allow-Origin", "*")
                        .body(Body::from(err))
                        .unwrap();
                }
            };
            let _ = maybe_inject_glm_coding_agent_bias(
                &config,
                responses_request.get("model").and_then(|v| v.as_str()),
                &mut chat_request,
            );
            request_model = body_json
                .get("model")
                .and_then(|v| v.as_str())
                .map(|v| v.to_string());
            responses_request["model"] = serde_json::json!(body_json
                .get("model")
                .and_then(|v| v.as_str())
                .unwrap_or(config.default_model.as_deref().unwrap_or("")));
            request_body_text = Some(truncate_body(
                &serde_json::to_vec(&chat_request).unwrap_or_default(),
            ));
            upstream_body_bytes = serde_json::to_vec(&chat_request).unwrap_or_default().into();
            upstream_path = "/v1/chat/completions".to_string();
            translated_request_json = Some(responses_request);
            anthropic_reverse_tool_map = Some(reverse_map);
            is_stream = stream;
            if let Some(estimate) = estimated_input_tokens {
                if estimate > 20_000 {
                    log_proxy(&format!(
                        "openai-compat large anthropic request estimated_input_tokens={estimate} request_len={}",
                        request_body_text.as_ref().map(|v| v.len()).unwrap_or(0)
                    ));
                }
            }
        } else if custom_responses_path {
            if method != reqwest::Method::POST {
                return Response::builder()
                    .status(StatusCode::METHOD_NOT_ALLOWED)
                    .header("Access-Control-Allow-Origin", "*")
                    .body(Body::from("Method not allowed"))
                    .unwrap();
            }
            let mut request_json: Value = match serde_json::from_slice(&body_bytes) {
                Ok(v) => v,
                Err(err) => {
                    return Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .header("Access-Control-Allow-Origin", "*")
                        .body(Body::from(format!("Invalid JSON: {err}")))
                        .unwrap();
                }
            };
            let mapped_model = map_openai_compat_model(
                &config,
                request_json.get("model").and_then(|v| v.as_str()),
            )
            .ok_or_else(|| "请先配置默认模型或模型映射".to_string())
            .unwrap_or_default();
            let mut temp_cfg = proxy_config_snapshot();
            temp_cfg.model_override = Some(mapped_model);
            let (mut chat_request, stream) =
                match convert_responses_request_to_chat_completions(&request_json, &temp_cfg) {
                    Ok(v) => v,
                    Err(err) => {
                        return Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .header("Access-Control-Allow-Origin", "*")
                            .body(Body::from(err))
                            .unwrap();
                    }
                };
            let _ = maybe_inject_glm_coding_agent_bias(
                &config,
                request_json.get("model").and_then(|v| v.as_str()),
                &mut chat_request,
            );
            request_json["model"] = serde_json::json!(request_json
                .get("model")
                .and_then(|v| v.as_str())
                .unwrap_or(config.default_model.as_deref().unwrap_or("")));
            request_model = request_json
                .get("model")
                .and_then(|v| v.as_str())
                .map(|v| v.to_string());
            request_body_text = Some(truncate_body(
                &serde_json::to_vec(&chat_request).unwrap_or_default(),
            ));
            upstream_body_bytes = serde_json::to_vec(&chat_request).unwrap_or_default().into();
            upstream_path = "/v1/chat/completions".to_string();
            translated_request_json = Some(chat_request);
            is_stream = stream;
        } else if upstream_path == "/v1/chat/completions"
            || upstream_path.starts_with("/v1/chat/completions?")
        {
            if let Ok(mut request_json) = serde_json::from_slice::<Value>(&body_bytes) {
                if let Some(mapped_model) = map_openai_compat_model(
                    &config,
                    request_json.get("model").and_then(|v| v.as_str()),
                ) {
                    request_json["model"] = serde_json::json!(mapped_model);
                    let request_model_str = request_json
                        .get("model")
                        .and_then(|v| v.as_str())
                        .map(|v| v.to_string());
                    let _ = maybe_inject_glm_coding_agent_bias(
                        &config,
                        request_model_str.as_deref(),
                        &mut request_json,
                    );
                    request_model = request_json
                        .get("model")
                        .and_then(|v| v.as_str())
                        .map(|v| v.to_string());
                    request_body_text = Some(truncate_body(
                        &serde_json::to_vec(&request_json).unwrap_or_default(),
                    ));
                    upstream_body_bytes =
                        serde_json::to_vec(&request_json).unwrap_or_default().into();
                }
            }
        }

        let target = build_upstream_url_with_base(&config.base_url, &upstream_path);
        log_proxy(&format!(
            "req#{request_id} start {method_label} {path} -> {target}"
        ));
        let request_url = Some(target.clone());

        let mut upstream_headers = reqwest::header::HeaderMap::new();
        apply_custom_openai_headers(
            &mut upstream_headers,
            &config.api_key,
            !upstream_body_bytes.is_empty(),
            is_stream,
        );

        let upstream_resp = match state
            .client
            .request(method.clone(), &target)
            .headers(upstream_headers)
            .body(upstream_body_bytes.clone())
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(err) => {
                let entry = ProxyLogEntry {
                    timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                    method: method_label,
                    path: path.clone(),
                    request_url,
                    status: StatusCode::BAD_GATEWAY.as_u16(),
                    duration_ms: started_at.elapsed().as_millis() as u64,
                    proxy_account_id: config.provider_name.clone(),
                    account_id: None,
                    error: Some(err.to_string()),
                    model: request_model,
                    request_headers: request_headers_json,
                    response_headers: None,
                    request_body: request_body_text,
                    response_body: None,
                    input_tokens: None,
                    output_tokens: None,
                    cache_status: None,
                    cache_key: None,
                    cache_eligible: None,
                    cache_bypass_reason: None,
                    local_cached_input_tokens: None,
                    provider_cached_input_tokens: None,
                };
                let _ = insert_proxy_log(&entry);
                return Response::builder()
                    .status(StatusCode::BAD_GATEWAY)
                    .header("Access-Control-Allow-Origin", "*")
                    .body(Body::from(format!("Upstream error: {err}")))
                    .unwrap();
            }
        };

        let upstream_status = upstream_resp.status();
        let response_headers_json =
            headers_to_json_string(sanitize_reqwest_headers(upstream_resp.headers()));

        if is_anthropic && is_stream && upstream_status.is_success() {
            let entry = ProxyLogEntry {
                timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                method: method_label.clone(),
                path: path.clone(),
                request_url,
                status: upstream_status.as_u16(),
                duration_ms: started_at.elapsed().as_millis() as u64,
                proxy_account_id: config.provider_name.clone(),
                account_id: None,
                error: None,
                model: request_model,
                request_headers: request_headers_json,
                response_headers: headers_to_json_string(vec![(
                    "content-type".to_string(),
                    "text/event-stream".to_string(),
                )]),
                request_body: request_body_text,
                response_body: None,
                input_tokens: None,
                output_tokens: None,
                cache_status: None,
                cache_key: None,
                cache_eligible: None,
                cache_bypass_reason: None,
                local_cached_input_tokens: None,
                provider_cached_input_tokens: None,
            };
            return build_openai_chat_to_claude_stream_response(
                upstream_resp,
                anthropic_reverse_tool_map.unwrap_or_default(),
                entry,
            )
            .await;
        }

        if custom_responses_path && is_stream && upstream_status.is_success() {
            let entry = ProxyLogEntry {
                timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                method: method_label,
                path: path.clone(),
                request_url,
                status: upstream_status.as_u16(),
                duration_ms: started_at.elapsed().as_millis() as u64,
                proxy_account_id: config.provider_name.clone(),
                account_id: None,
                error: None,
                model: request_model,
                request_headers: request_headers_json,
                response_headers: headers_to_json_string(vec![(
                    "content-type".to_string(),
                    "text/event-stream".to_string(),
                )]),
                request_body: request_body_text,
                response_body: None,
                input_tokens: None,
                output_tokens: None,
                cache_status: None,
                cache_key: None,
                cache_eligible: None,
                cache_bypass_reason: None,
                local_cached_input_tokens: None,
                provider_cached_input_tokens: None,
            };
            return build_custom_openai_stream_response(
                upstream_resp,
                translated_request_json.unwrap_or_else(|| serde_json::json!({})),
                entry,
            )
            .await;
        }

        let headers = upstream_resp.headers().clone();
        let bytes = upstream_resp.bytes().await.unwrap_or_default();
        if is_anthropic && upstream_status.is_success() {
            let request_json = translated_request_json.unwrap_or_else(|| serde_json::json!({}));
            match convert_chat_completions_non_stream_to_responses(&request_json, &bytes) {
                Ok((responses_bytes, _, _, _)) => {
                    match build_claude_response_or_error_body(
                        upstream_status,
                        &responses_bytes,
                        &anthropic_reverse_tool_map.unwrap_or_default(),
                        request_model.as_deref().unwrap_or("unknown"),
                    ) {
                        Ok((
                            converted,
                            input_tokens,
                            output_tokens,
                            response_body_text,
                            error_message,
                        )) => {
                            let entry = ProxyLogEntry {
                                timestamp: chrono::Utc::now()
                                    .format("%Y-%m-%dT%H:%M:%SZ")
                                    .to_string(),
                                method: method_label,
                                path: path.clone(),
                                request_url,
                                status: upstream_status.as_u16(),
                                duration_ms: started_at.elapsed().as_millis() as u64,
                                proxy_account_id: config.provider_name.clone(),
                                account_id: None,
                                error: error_message,
                                model: request_model,
                                request_headers: request_headers_json,
                                response_headers: headers_to_json_string(vec![(
                                    "content-type".to_string(),
                                    "application/json".to_string(),
                                )]),
                                request_body: request_body_text,
                                response_body: response_body_text,
                                input_tokens: input_tokens.or(estimated_input_tokens),
                                output_tokens,
                                ..ProxyLogEntry::default()
                            };
                            let _ = insert_proxy_log(&entry);
                            return Response::builder()
                                .status(
                                    axum::http::StatusCode::from_u16(upstream_status.as_u16())
                                        .unwrap_or(StatusCode::OK),
                                )
                                .header("Content-Type", "application/json")
                                .header("Access-Control-Allow-Origin", "*")
                                .header("Access-Control-Allow-Headers", "*")
                                .body(Body::from(converted))
                                .unwrap();
                        }
                        Err(err) => {
                            let entry = ProxyLogEntry {
                                timestamp: chrono::Utc::now()
                                    .format("%Y-%m-%dT%H:%M:%SZ")
                                    .to_string(),
                                method: method_label,
                                path: path.clone(),
                                request_url,
                                status: StatusCode::BAD_GATEWAY.as_u16(),
                                duration_ms: started_at.elapsed().as_millis() as u64,
                                proxy_account_id: config.provider_name.clone(),
                                account_id: None,
                                error: Some(err.clone()),
                                model: request_model,
                                request_headers: request_headers_json,
                                response_headers: response_headers_json,
                                request_body: request_body_text,
                                response_body: Some(truncate_body(&bytes)),
                                input_tokens: None,
                                output_tokens: None,
                                cache_status: None,
                                cache_key: None,
                                cache_eligible: None,
                                cache_bypass_reason: None,
                                local_cached_input_tokens: None,
                                provider_cached_input_tokens: None,
                            };
                            let _ = insert_proxy_log(&entry);
                            return Response::builder()
                                .status(StatusCode::BAD_GATEWAY)
                                .header("Access-Control-Allow-Origin", "*")
                                .body(Body::from(err))
                                .unwrap();
                        }
                    }
                }
                Err(err) => {
                    return Response::builder()
                        .status(StatusCode::BAD_GATEWAY)
                        .header("Access-Control-Allow-Origin", "*")
                        .body(Body::from(err))
                        .unwrap();
                }
            }
        }
        if is_anthropic && !upstream_status.is_success() {
            let (converted, error_message, response_body_text) =
                build_anthropic_error_response_bytes(upstream_status, &bytes);
            let entry = ProxyLogEntry {
                timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                method: method_label,
                path: path.clone(),
                request_url,
                status: upstream_status.as_u16(),
                duration_ms: started_at.elapsed().as_millis() as u64,
                proxy_account_id: config.provider_name.clone(),
                account_id: None,
                error: Some(error_message),
                model: request_model,
                request_headers: request_headers_json,
                response_headers: headers_to_json_string(vec![(
                    "content-type".to_string(),
                    "application/json".to_string(),
                )]),
                request_body: request_body_text,
                response_body: response_body_text,
                input_tokens: estimated_input_tokens,
                output_tokens: None,
                ..ProxyLogEntry::default()
            };
            let _ = insert_proxy_log(&entry);
            return Response::builder()
                .status(
                    axum::http::StatusCode::from_u16(upstream_status.as_u16())
                        .unwrap_or(StatusCode::BAD_GATEWAY),
                )
                .header("Content-Type", "application/json")
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Headers", "*")
                .body(Body::from(converted))
                .unwrap();
        }
        if custom_responses_path && upstream_status.is_success() {
            let request_json = translated_request_json.unwrap_or_else(|| serde_json::json!({}));
            match convert_chat_completions_non_stream_to_responses(&request_json, &bytes) {
                Ok((converted, input_tokens, output_tokens, response_body_text)) => {
                    let entry = ProxyLogEntry {
                        timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                        method: method_label,
                        path: path.clone(),
                        request_url,
                        status: upstream_status.as_u16(),
                        duration_ms: started_at.elapsed().as_millis() as u64,
                        proxy_account_id: config.provider_name.clone(),
                        account_id: None,
                        error: None,
                        model: request_model,
                        request_headers: request_headers_json,
                        response_headers: headers_to_json_string(vec![(
                            "content-type".to_string(),
                            "application/json".to_string(),
                        )]),
                        request_body: request_body_text,
                        response_body: response_body_text,
                        input_tokens,
                        output_tokens,
                        ..ProxyLogEntry::default()
                    };
                    let _ = insert_proxy_log(&entry);
                    return Response::builder()
                        .status(
                            axum::http::StatusCode::from_u16(upstream_status.as_u16())
                                .unwrap_or(StatusCode::OK),
                        )
                        .header("Content-Type", "application/json")
                        .header("Access-Control-Allow-Origin", "*")
                        .header("Access-Control-Allow-Headers", "*")
                        .body(Body::from(converted))
                        .unwrap();
                }
                Err(err) => {
                    let entry = ProxyLogEntry {
                        timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                        method: method_label,
                        path: path.clone(),
                        request_url,
                        status: StatusCode::BAD_GATEWAY.as_u16(),
                        duration_ms: started_at.elapsed().as_millis() as u64,
                        proxy_account_id: config.provider_name.clone(),
                        account_id: None,
                        error: Some(err.clone()),
                        model: request_model,
                        request_headers: request_headers_json,
                        response_headers: response_headers_json,
                        request_body: request_body_text,
                        response_body: Some(truncate_body(&bytes)),
                        input_tokens: None,
                        output_tokens: None,
                        cache_status: None,
                        cache_key: None,
                        cache_eligible: None,
                        cache_bypass_reason: None,
                        local_cached_input_tokens: None,
                        provider_cached_input_tokens: None,
                    };
                    let _ = insert_proxy_log(&entry);
                    return Response::builder()
                        .status(StatusCode::BAD_GATEWAY)
                        .header("Access-Control-Allow-Origin", "*")
                        .body(Body::from(err))
                        .unwrap();
                }
            }
        }

        let normalized_bytes =
            maybe_normalize_chat_completion_response_bytes(&upstream_path, upstream_status, &bytes);
        let response_body_text = if normalized_bytes.is_empty() {
            None
        } else {
            Some(truncate_body(&normalized_bytes))
        };
        let (input_tokens, output_tokens) = extract_usage(&normalized_bytes);
        let entry = ProxyLogEntry {
            timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            method: method_label,
            path: path.clone(),
            request_url,
            status: upstream_status.as_u16(),
            duration_ms: started_at.elapsed().as_millis() as u64,
            proxy_account_id: config.provider_name.clone(),
            account_id: None,
            error: None,
            model: request_model,
            request_headers: request_headers_json,
            response_headers: response_headers_json,
            request_body: request_body_text,
            response_body: response_body_text,
            input_tokens,
            output_tokens,
            ..ProxyLogEntry::default()
        };
        let _ = insert_proxy_log(&entry);
        let mut builder = Response::builder()
            .status(
                axum::http::StatusCode::from_u16(upstream_status.as_u16())
                    .unwrap_or(StatusCode::OK),
            )
            .header("Access-Control-Allow-Origin", "*")
            .header("Access-Control-Allow-Headers", "*");
        for (k, v) in headers.iter() {
            if skip_response_header(k.as_str()) {
                continue;
            }
            if let (Ok(name), Ok(val)) = (
                axum::http::HeaderName::from_bytes(k.as_str().as_bytes()),
                axum::http::HeaderValue::from_bytes(v.as_bytes()),
            ) {
                builder = builder.header(name, val);
            }
        }
        builder.body(Body::from(normalized_bytes)).unwrap()
    }

    let app = Router::new()
        .fallback(any(openai_compat_proxy_handler))
        .with_state(proxy_state.clone());

    let addr = format!("localhost:{proxy_port}");
    let shutdown_notify = Arc::new(Notify::new());
    let shutdown_waiter = shutdown_notify.clone();
    tokio::spawn(async move {
        let _ = shutdown_rx.await;
        shutdown_waiter.notify_waiters();
    });

    {
        let mut lock = OPENAI_COMPAT_PROXY_STATE.lock().unwrap();
        *lock = Some(proxy_state.clone());
    }
    {
        let mut lock = OPENAI_COMPAT_PROXY_PORT.lock().unwrap();
        *lock = Some(proxy_port);
    }

    tokio::spawn(async move {
        let serve_result = run_proxy_server(&addr, app, shutdown_notify).await;
        if let Err(err) = serve_result {
            log_proxy(&format!("openai compat proxy exited with error: {err}"));
        }
        let mut lock = OPENAI_COMPAT_PROXY_PORT.lock().unwrap();
        *lock = None;
        let mut lock = OPENAI_COMPAT_PROXY_STATE.lock().unwrap();
        *lock = None;
    });

    Ok(serde_json::json!({
        "success": true,
        "port": proxy_port,
        "base_url": format!("http://127.0.0.1:{proxy_port}"),
        "config_id": config.id,
        "provider_name": config.provider_name,
    }))
}

pub fn stop_openai_compat_proxy_runtime() -> Result<Value, String> {
    let mut lock = OPENAI_COMPAT_PROXY_SHUTDOWN.lock().unwrap();
    if let Some(tx) = lock.take() {
        let _ = tx.send(());
        Ok(serde_json::json!({ "success": true }))
    } else {
        Ok(serde_json::json!({ "success": false }))
    }
}

#[tauri::command]
async fn start_openai_compat_proxy(config_id: String, port: Option<u16>) -> Result<Value, String> {
    start_openai_compat_proxy_runtime(config_id, port).await
}

#[tauri::command]
fn stop_openai_compat_proxy() -> Result<Value, String> {
    stop_openai_compat_proxy_runtime()
}

#[tauri::command]
fn get_openai_compat_proxy_status() -> Result<Value, String> {
    let port = *OPENAI_COMPAT_PROXY_PORT.lock().unwrap();
    let running = if let Some(port) = port {
        let addr = format!("127.0.0.1:{port}");
        let socket_addr: std::net::SocketAddr = addr
            .parse()
            .map_err(|e: std::net::AddrParseError| e.to_string())?;
        std::net::TcpStream::connect_timeout(&socket_addr, std::time::Duration::from_millis(200))
            .is_ok()
    } else {
        false
    };
    let state = OPENAI_COMPAT_PROXY_STATE.lock().unwrap();
    let (config_id, provider_name) = if let Some(state) = &*state {
        let config = state.config.read().unwrap();
        (Some(config.id.clone()), Some(config.provider_name.clone()))
    } else {
        (None, None)
    };
    Ok(serde_json::json!({
        "running": running,
        "port": port,
        "config_id": config_id,
        "provider_name": provider_name,
    }))
}

#[tauri::command]
fn stop_api_proxy() -> Result<Value, String> {
    let mut lock = PROXY_SHUTDOWN.lock().unwrap();
    if let Some(tx) = lock.take() {
        let _ = tx.send(());
        Ok(serde_json::json!({ "success": true, "message": "代理已停止" }))
    } else {
        Ok(serde_json::json!({ "success": false, "message": "代理未在运行" }))
    }
}

/// Hot-reload accounts from disk into the running proxy pool without restart
#[tauri::command]
fn reload_proxy_accounts() -> Result<Value, String> {
    let state = {
        let lock = PROXY_STATE.lock().unwrap();
        lock.clone()
    };
    let state = state.ok_or("代理未在运行")?;

    let new_accounts = load_proxy_accounts()?;
    let count = new_accounts.len();
    {
        let mut accounts_lock = state.accounts.write().unwrap();
        *accounts_lock = new_accounts;
    }
    Ok(serde_json::json!({ "success": true, "account_count": count }))
}

#[tauri::command]
fn get_proxy_status() -> Result<Value, String> {
    let port = *PROXY_PORT.lock().unwrap();
    let running = if let Some(port) = port {
        let addr = format!("127.0.0.1:{port}");
        let socket_addr: std::net::SocketAddr = addr
            .parse()
            .map_err(|e: std::net::AddrParseError| e.to_string())?;
        std::net::TcpStream::connect_timeout(&socket_addr, std::time::Duration::from_millis(200))
            .is_ok()
    } else {
        false
    };

    let (account_count, active, cooldown, blocked) = {
        let lock = PROXY_STATE.lock().unwrap();
        if let Some(state) = &*lock {
            let now = std::time::Instant::now();
            let accounts = state.accounts.read().unwrap();
            let total = accounts.len();
            let active = accounts
                .iter()
                .filter(|a| a.health == AccountHealth::Active)
                .count();
            let cd = accounts
                .iter()
                .filter(|a| matches!(&a.health, AccountHealth::Cooldown(u) if now < *u))
                .count();
            let bl = accounts
                .iter()
                .filter(|a| a.health == AccountHealth::Blocked)
                .count();
            (total, active, cd, bl)
        } else {
            (0, 0, 0, 0)
        }
    };

    Ok(serde_json::json!({
        "running": running,
        "port": port,
        "account_count": account_count,
        "active": active,
        "cooldown": cooldown,
        "blocked": blocked,
    }))
}

#[tauri::command]
fn get_proxy_config() -> Result<ProxyConfig, String> {
    Ok(proxy_config_snapshot())
}

#[tauri::command]
fn update_proxy_config(
    api_key: Option<String>,
    enable_logging: Option<bool>,
    max_logs: Option<usize>,
    disable_on_usage_limit: Option<bool>,
    model_override: Option<String>,
    reasoning_effort_override: Option<String>,
    upstream_mode: Option<String>,
    custom_openai_base_url: Option<String>,
    custom_openai_api_key: Option<String>,
    enable_exact_cache: Option<bool>,
    exact_cache_ttl_minutes: Option<i64>,
    exact_cache_max_entries: Option<usize>,
    enable_semantic_cache: Option<bool>,
    semantic_cache_threshold: Option<f64>,
    vector_provider_mode: Option<String>,
    vector_api_base_url: Option<String>,
    vector_api_key: Option<String>,
    vector_model: Option<String>,
) -> Result<ProxyConfig, String> {
    let mut cfg = proxy_config_snapshot();
    if let Some(value) = api_key {
        let trimmed = value.trim().to_string();
        cfg.api_key = if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        };
    }
    if let Some(value) = enable_logging {
        cfg.enable_logging = value;
    }
    if let Some(value) = max_logs {
        cfg.max_logs = value.max(1);
    }
    if let Some(value) = disable_on_usage_limit {
        cfg.disable_on_usage_limit = value;
    }
    if let Some(value) = model_override {
        let trimmed = value.trim().to_string();
        cfg.model_override = if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        };
    }
    if let Some(value) = reasoning_effort_override {
        let trimmed = value.trim().to_string();
        cfg.reasoning_effort_override = if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        };
    }
    if let Some(value) = upstream_mode {
        cfg.upstream_mode = normalize_proxy_upstream_mode(&value).to_string();
    }
    if let Some(value) = custom_openai_base_url {
        let trimmed = value.trim().trim_end_matches('/').to_string();
        cfg.custom_openai_base_url = if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        };
    }
    if let Some(value) = custom_openai_api_key {
        let trimmed = value.trim().to_string();
        cfg.custom_openai_api_key = if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        };
    }
    if let Some(value) = enable_exact_cache {
        cfg.enable_exact_cache = value;
    }
    if let Some(value) = exact_cache_ttl_minutes {
        cfg.exact_cache_ttl_minutes = value.clamp(1, 24 * 60);
    }
    if let Some(value) = exact_cache_max_entries {
        cfg.exact_cache_max_entries = value.clamp(100, 20_000);
    }
    if let Some(value) = enable_semantic_cache {
        cfg.enable_semantic_cache = value;
    }
    if let Some(value) = semantic_cache_threshold {
        cfg.semantic_cache_threshold = value.clamp(0.5, 0.9999);
    }
    if let Some(value) = vector_provider_mode {
        let trimmed = value.trim().to_ascii_lowercase();
        cfg.vector_provider_mode = if trimmed.is_empty() {
            default_vector_provider_mode()
        } else {
            trimmed
        };
    }
    if let Some(value) = vector_api_base_url {
        let trimmed = value.trim().trim_end_matches('/').to_string();
        cfg.vector_api_base_url = if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        };
    }
    if let Some(value) = vector_api_key {
        let trimmed = value.trim().to_string();
        cfg.vector_api_key = if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        };
    }
    if let Some(value) = vector_model {
        let trimmed = value.trim().to_string();
        cfg.vector_model = if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        };
    }
    save_proxy_config(&cfg)?;
    let mut lock = proxy_config().lock().unwrap();
    *lock = cfg.clone();
    Ok(cfg)
}

#[tauri::command]
fn generate_proxy_api_key() -> Result<String, String> {
    let mut bytes = [0u8; 16];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    let key = format!(
        "sk-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5], bytes[6], bytes[7],
        bytes[8], bytes[9], bytes[10], bytes[11],
        bytes[12], bytes[13], bytes[14], bytes[15]
    );
    Ok(key)
}

#[tauri::command]
fn clear_proxy_logs() -> Result<Value, String> {
    let conn = proxy_log_db()?;
    conn.execute("DELETE FROM request_logs", [])
        .map_err(|e| e.to_string())?;
    Ok(serde_json::json!({ "success": true }))
}

#[tauri::command]
fn get_proxy_logs_count_filtered(
    filter: Option<String>,
    errors_only: Option<bool>,
) -> Result<usize, String> {
    let filter = filter.unwrap_or_default();
    let errors_only = errors_only.unwrap_or(false);
    let conn = proxy_log_db()?;
    let mut clauses: Vec<String> = Vec::new();
    if errors_only {
        clauses.push("(status < 200 OR status >= 400)".to_string());
    }
    if !filter.is_empty() {
        clauses.push("(method LIKE ?1 OR path LIKE ?1 OR CAST(status AS TEXT) LIKE ?1 OR proxy_account_id LIKE ?1 OR account_id LIKE ?1 OR error LIKE ?1 OR model LIKE ?1)".to_string());
    }
    let sql = if clauses.is_empty() {
        "SELECT COUNT(*) FROM request_logs".to_string()
    } else {
        format!(
            "SELECT COUNT(*) FROM request_logs WHERE {}",
            clauses.join(" AND ")
        )
    };
    let count: i64 = if filter.is_empty() {
        conn.query_row(&sql, [], |row| row.get(0))
            .map_err(|e| e.to_string())?
    } else {
        let pattern = format!("%{}%", filter);
        conn.query_row(&sql, params![pattern], |row| row.get(0))
            .map_err(|e| e.to_string())?
    };
    Ok(count as usize)
}

#[tauri::command]
fn get_proxy_logs_filtered(
    filter: Option<String>,
    errors_only: Option<bool>,
    limit: Option<usize>,
    offset: Option<usize>,
) -> Result<Vec<ProxyLogSummary>, String> {
    let filter = filter.unwrap_or_default();
    let errors_only = errors_only.unwrap_or(false);
    let limit = limit.unwrap_or(50) as i64;
    let offset = offset.unwrap_or(0) as i64;
    let conn = proxy_log_db()?;
    let sql_base = "SELECT id, timestamp, method, path, status, duration_ms, proxy_account_id, account_id, error, model FROM request_logs";
    let filter_clause = "(method LIKE ?1 OR path LIKE ?1 OR CAST(status AS TEXT) LIKE ?1 OR proxy_account_id LIKE ?1 OR account_id LIKE ?1 OR error LIKE ?1 OR model LIKE ?1)";
    let (sql, params_vec): (String, Vec<rusqlite::types::Value>) = if filter.is_empty() {
        if errors_only {
            (format!("{sql_base} WHERE (status < 200 OR status >= 400) ORDER BY id DESC LIMIT ?1 OFFSET ?2"), vec![limit.into(), offset.into()])
        } else {
            (
                format!("{sql_base} ORDER BY id DESC LIMIT ?1 OFFSET ?2"),
                vec![limit.into(), offset.into()],
            )
        }
    } else {
        let pattern = format!("%{}%", filter).into();
        if errors_only {
            (format!("{sql_base} WHERE (status < 200 OR status >= 400) AND {filter_clause} ORDER BY id DESC LIMIT ?2 OFFSET ?3"), vec![pattern, limit.into(), offset.into()])
        } else {
            (
                format!("{sql_base} WHERE {filter_clause} ORDER BY id DESC LIMIT ?2 OFFSET ?3"),
                vec![pattern, limit.into(), offset.into()],
            )
        }
    };
    let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
    let logs_iter = stmt
        .query_map(rusqlite::params_from_iter(params_vec), |row| {
            Ok(ProxyLogSummary {
                id: row.get(0)?,
                timestamp: row.get(1)?,
                method: row.get(2)?,
                path: row.get(3)?,
                status: row.get::<_, i64>(4)? as u16,
                duration_ms: row.get::<_, i64>(5)? as u64,
                proxy_account_id: row.get(6)?,
                account_id: row.get(7)?,
                error: row.get(8)?,
                model: row.get(9)?,
            })
        })
        .map_err(|e| e.to_string())?;
    let mut logs = Vec::new();
    for log in logs_iter {
        logs.push(log.map_err(|e| e.to_string())?);
    }
    Ok(logs)
}

#[tauri::command]
fn get_proxy_log_detail(log_id: i64) -> Result<ProxyLogDetail, String> {
    let conn = proxy_log_db()?;
    let mut stmt = conn.prepare(
        "SELECT id, timestamp, method, path, request_url, status, duration_ms, proxy_account_id, account_id, error, model, request_headers, response_headers, request_body, response_body, input_tokens, output_tokens, cache_status, cache_key, cache_eligible, cache_bypass_reason, local_cached_input_tokens, provider_cached_input_tokens FROM request_logs WHERE id = ?1",
    ).map_err(|e| e.to_string())?;
    let log = stmt
        .query_row(params![log_id], |row| {
            Ok(ProxyLogDetail {
                id: row.get(0)?,
                timestamp: row.get(1)?,
                method: row.get(2)?,
                path: row.get(3)?,
                request_url: row.get(4)?,
                status: row.get::<_, i64>(5)? as u16,
                duration_ms: row.get::<_, i64>(6)? as u64,
                proxy_account_id: row.get(7)?,
                account_id: row.get(8)?,
                error: row.get(9)?,
                model: row.get(10)?,
                request_headers: row.get(11)?,
                response_headers: row.get(12)?,
                request_body: row.get(13)?,
                response_body: row.get(14)?,
                input_tokens: row.get(15)?,
                output_tokens: row.get(16)?,
                cache_status: row.get(17)?,
                cache_key: row.get(18)?,
                cache_eligible: row.get::<_, Option<i64>>(19)?.map(|v| v != 0),
                cache_bypass_reason: row.get(20)?,
                local_cached_input_tokens: row.get(21)?,
                provider_cached_input_tokens: row.get(22)?,
            })
        })
        .map_err(|e| {
            let msg = format!("日志详情查询失败 (id={log_id}): {e}");
            log_proxy(&msg);
            msg
        })?;
    Ok(log)
}

#[tauri::command]
fn get_proxy_token_stats(hours: Option<i64>) -> Result<ProxyTokenStats, String> {
    let hours = hours.unwrap_or(24).clamp(1, 24 * 30);
    let conn = proxy_log_db()?;
    let since = (chrono::Utc::now() - chrono::Duration::hours(hours))
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string();

    let (total_requests, success_requests, error_requests, input_tokens, output_tokens, avg_duration_ms): (i64, i64, i64, i64, i64, f64) = conn
        .query_row(
            "SELECT
                COUNT(*) as total_requests,
                COALESCE(SUM(CASE WHEN status >= 200 AND status < 400 THEN 1 ELSE 0 END), 0) as success_requests,
                COALESCE(SUM(CASE WHEN status < 200 OR status >= 400 THEN 1 ELSE 0 END), 0) as error_requests,
                COALESCE(SUM(input_tokens), 0) as input_tokens,
                COALESCE(SUM(output_tokens), 0) as output_tokens,
                COALESCE(AVG(duration_ms), 0) as avg_duration_ms
             FROM request_logs
             WHERE timestamp >= ?1",
            params![since],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .map_err(|e| e.to_string())?;

    let mut top_models_stmt = conn
        .prepare(
            "SELECT
                model,
                COUNT(*) as requests,
                COALESCE(SUM(input_tokens), 0) as input_tokens,
                COALESCE(SUM(output_tokens), 0) as output_tokens,
                COALESCE(SUM(input_tokens), 0) + COALESCE(SUM(output_tokens), 0) as total_tokens
             FROM request_logs
             WHERE timestamp >= ?1 AND model IS NOT NULL AND model != ''
             GROUP BY model
             ORDER BY total_tokens DESC, requests DESC
             LIMIT 8",
        )
        .map_err(|e| e.to_string())?;
    let top_models_iter = top_models_stmt
        .query_map(params![since], |row| {
            Ok(ProxyTokenStatsItem {
                name: row.get(0)?,
                requests: row.get(1)?,
                input_tokens: row.get(2)?,
                output_tokens: row.get(3)?,
                total_tokens: row.get(4)?,
            })
        })
        .map_err(|e| e.to_string())?;
    let mut top_models = Vec::new();
    for item in top_models_iter {
        top_models.push(item.map_err(|e| e.to_string())?);
    }

    let mut top_accounts_stmt = conn
        .prepare(
            "SELECT
                COALESCE(NULLIF(account_id, ''), proxy_account_id) as account_name,
                COUNT(*) as requests,
                COALESCE(SUM(input_tokens), 0) as input_tokens,
                COALESCE(SUM(output_tokens), 0) as output_tokens,
                COALESCE(SUM(input_tokens), 0) + COALESCE(SUM(output_tokens), 0) as total_tokens
             FROM request_logs
             WHERE timestamp >= ?1
             GROUP BY account_name
             ORDER BY total_tokens DESC, requests DESC
             LIMIT 8",
        )
        .map_err(|e| e.to_string())?;
    let top_accounts_iter = top_accounts_stmt
        .query_map(params![since], |row| {
            Ok(ProxyTokenStatsItem {
                name: row.get(0)?,
                requests: row.get(1)?,
                input_tokens: row.get(2)?,
                output_tokens: row.get(3)?,
                total_tokens: row.get(4)?,
            })
        })
        .map_err(|e| e.to_string())?;
    let mut top_accounts = Vec::new();
    for item in top_accounts_iter {
        top_accounts.push(item.map_err(|e| e.to_string())?);
    }

    Ok(ProxyTokenStats {
        window_hours: hours,
        total_requests,
        success_requests,
        error_requests,
        input_tokens,
        output_tokens,
        total_tokens: input_tokens + output_tokens,
        avg_duration_ms,
        top_models,
        top_accounts,
    })
}

#[tauri::command]
fn get_ai_cache_overview(hours: Option<i64>) -> Result<AICacheOverview, String> {
    let hours = hours.unwrap_or(24).clamp(1, 24 * 30);
    let conn = proxy_log_db()?;
    let since = (chrono::Utc::now() - chrono::Duration::hours(hours))
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string();
    let (
        total_requests,
        cache_eligible_requests,
        local_hits,
        local_misses,
        bypassed_requests,
        provider_cached_requests,
        input_tokens,
        output_tokens,
        local_cached_input_tokens,
        provider_cached_input_tokens,
        avg_hit_duration_ms,
        avg_miss_duration_ms,
    ): (i64, i64, i64, i64, i64, i64, i64, i64, i64, i64, f64, f64) = conn
        .query_row(
            "SELECT
                COUNT(*) as total_requests,
                COALESCE(SUM(CASE WHEN cache_eligible = 1 THEN 1 ELSE 0 END), 0) as cache_eligible_requests,
                COALESCE(SUM(CASE WHEN cache_status = 'local_hit' THEN 1 ELSE 0 END), 0) as local_hits,
                COALESCE(SUM(CASE WHEN cache_status = 'miss' THEN 1 ELSE 0 END), 0) as local_misses,
                COALESCE(SUM(CASE WHEN cache_status = 'bypass' THEN 1 ELSE 0 END), 0) as bypassed_requests,
                COALESCE(SUM(CASE WHEN provider_cached_input_tokens > 0 THEN 1 ELSE 0 END), 0) as provider_cached_requests,
                COALESCE(SUM(input_tokens), 0) as input_tokens,
                COALESCE(SUM(output_tokens), 0) as output_tokens,
                COALESCE(SUM(local_cached_input_tokens), 0) as local_cached_input_tokens,
                COALESCE(SUM(provider_cached_input_tokens), 0) as provider_cached_input_tokens,
                COALESCE(AVG(CASE WHEN cache_status = 'local_hit' THEN duration_ms END), 0) as avg_hit_duration_ms,
                COALESCE(AVG(CASE WHEN cache_status = 'miss' THEN duration_ms END), 0) as avg_miss_duration_ms
             FROM request_logs
             WHERE timestamp >= ?1",
            params![since],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                    row.get(11)?,
                ))
            },
        )
        .map_err(|e| e.to_string())?;
    let local_hit_rate = if cache_eligible_requests > 0 {
        local_hits as f64 / cache_eligible_requests as f64
    } else {
        0.0
    };
    Ok(AICacheOverview {
        window_hours: hours,
        total_requests,
        cache_eligible_requests,
        local_hits,
        local_misses,
        bypassed_requests,
        provider_cached_requests,
        local_hit_rate,
        input_tokens,
        output_tokens,
        local_cached_input_tokens,
        provider_cached_input_tokens,
        total_cached_input_tokens: local_cached_input_tokens + provider_cached_input_tokens,
        avg_hit_duration_ms,
        avg_miss_duration_ms,
    })
}

#[tauri::command]
fn get_ai_cache_trend(hours: Option<i64>) -> Result<Vec<AICacheTrendPoint>, String> {
    let hours = hours.unwrap_or(24).clamp(1, 24 * 30);
    let conn = proxy_log_db()?;
    let since = (chrono::Utc::now() - chrono::Duration::hours(hours))
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string();
    let bucket_expr = if hours <= 48 {
        "substr(timestamp, 1, 13) || ':00'"
    } else {
        "substr(timestamp, 1, 10)"
    };
    let sql = format!(
        "SELECT
            {bucket_expr} as bucket,
            COUNT(*) as total_requests,
            COALESCE(SUM(CASE WHEN cache_eligible = 1 THEN 1 ELSE 0 END), 0) as cache_eligible_requests,
            COALESCE(SUM(CASE WHEN cache_status = 'local_hit' THEN 1 ELSE 0 END), 0) as local_hits,
            COALESCE(SUM(provider_cached_input_tokens), 0) as provider_cached_input_tokens,
            COALESCE(SUM(local_cached_input_tokens), 0) as local_cached_input_tokens,
            COALESCE(SUM(input_tokens), 0) as input_tokens,
            COALESCE(SUM(output_tokens), 0) as output_tokens
         FROM request_logs
         WHERE timestamp >= ?1
         GROUP BY bucket
         ORDER BY bucket DESC
         LIMIT 120"
    );
    let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map(params![since], |row| {
            Ok(AICacheTrendPoint {
                bucket: row.get(0)?,
                total_requests: row.get(1)?,
                cache_eligible_requests: row.get(2)?,
                local_hits: row.get(3)?,
                provider_cached_input_tokens: row.get(4)?,
                local_cached_input_tokens: row.get(5)?,
                input_tokens: row.get(6)?,
                output_tokens: row.get(7)?,
            })
        })
        .map_err(|e| e.to_string())?;
    let mut items = Vec::new();
    for row in rows {
        items.push(row.map_err(|e| e.to_string())?);
    }
    Ok(items)
}

#[tauri::command]
fn list_ai_cache_entries(
    limit: Option<i64>,
    offset: Option<i64>,
) -> Result<Vec<AICacheEntrySummary>, String> {
    let conn = proxy_log_db()?;
    let mut stmt = conn
        .prepare(
            "SELECT id, cache_key, path, model, cache_type, hit_count, input_tokens, output_tokens,
                    local_cached_input_tokens, provider_cached_input_tokens, created_at, last_hit_at, expires_at, response_preview
             FROM ai_cache_entries
             ORDER BY last_hit_at DESC, id DESC
             LIMIT ?1 OFFSET ?2",
        )
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map(
            params![
                limit.unwrap_or(50).clamp(1, 200),
                offset.unwrap_or(0).max(0)
            ],
            |row| {
                Ok(AICacheEntrySummary {
                    id: row.get(0)?,
                    cache_key: row.get(1)?,
                    path: row.get(2)?,
                    model: row.get(3)?,
                    cache_type: row.get(4)?,
                    hit_count: row.get(5)?,
                    input_tokens: row.get(6)?,
                    output_tokens: row.get(7)?,
                    local_cached_input_tokens: row.get(8)?,
                    provider_cached_input_tokens: row.get(9)?,
                    created_at: row.get(10)?,
                    last_hit_at: row.get(11)?,
                    expires_at: row.get(12)?,
                    response_preview: row.get(13)?,
                })
            },
        )
        .map_err(|e| e.to_string())?;
    let mut items = Vec::new();
    for row in rows {
        items.push(row.map_err(|e| e.to_string())?);
    }
    Ok(items)
}

#[tauri::command]
fn clear_ai_cache() -> Result<Value, String> {
    let conn = proxy_log_db()?;
    conn.execute("DELETE FROM ai_cache_entries", [])
        .map_err(|e| e.to_string())?;
    Ok(serde_json::json!({ "success": true }))
}

// ─── Tauri commands: account usage ───────────────────────────────────────────

#[derive(Serialize, Deserialize, Clone)]
struct AccountUsage {
    account_id: String,
    // primary window (短窗口，约5小时)
    used_percent: Option<f64>,
    window_minutes: Option<u64>,
    resets_at: Option<i64>, // unix timestamp seconds
    // secondary window (长窗口，约7天)
    secondary_used_percent: Option<f64>,
    secondary_window_minutes: Option<u64>,
    secondary_resets_at: Option<i64>,
    // meta
    availability: String, // "available" | "unavailable" | "primary_window_available_only" | "unknown"
    captured_at: i64,     // unix timestamp seconds when this data was fetched
}

async fn fetch_account_usage_by_id(id: &str) -> Result<AccountUsage, String> {
    let auth_path = accounts_dir().join(id).join("auth.json");
    if !auth_path.exists() {
        return Err(format!("Account {id} not found"));
    }
    let content = fs::read_to_string(&auth_path).map_err(|e| e.to_string())?;
    let auth_data: Value = serde_json::from_str(&content).map_err(|e| e.to_string())?;

    let empty = Value::Object(Default::default());
    let tokens = auth_data.get("tokens").unwrap_or(&empty);
    let access_token = tokens
        .get("access_token")
        .and_then(|v| v.as_str())
        .ok_or("No access token for this account")?;

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| e.to_string())?;

    let resp = client
        .get("https://chatgpt.com/backend-api/wham/usage")
        .header("Authorization", format!("Bearer {}", access_token))
        .header("Content-Type", "application/json")
        .send()
        .await
        .map_err(|e| format!("Request failed: {e}"))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("Usage API returned {status}: {body}"));
    }

    let json: Value = resp.json().await.map_err(|e| e.to_string())?;

    let pw = json.pointer("/rate_limit/primary_window");
    let sw = json.pointer("/rate_limit/secondary_window");

    let used_percent = pw
        .and_then(|v| v.get("used_percent"))
        .and_then(|v| v.as_f64());
    let window_secs = pw
        .and_then(|v| v.get("limit_window_seconds"))
        .and_then(|v| v.as_u64());
    let window_minutes = window_secs.map(|s| (s + 59) / 60);
    let resets_at = pw.and_then(|v| v.get("reset_at")).and_then(|v| v.as_i64());

    let secondary_used_percent = sw
        .and_then(|v| v.get("used_percent"))
        .and_then(|v| v.as_f64());
    let secondary_secs = sw
        .and_then(|v| v.get("limit_window_seconds"))
        .and_then(|v| v.as_u64());
    let secondary_window_minutes = secondary_secs.map(|s| (s + 59) / 60);
    let secondary_resets_at = sw.and_then(|v| v.get("reset_at")).and_then(|v| v.as_i64());

    let availability = match (used_percent, secondary_used_percent) {
        (None, _) => "unknown",
        (Some(p), _) if p >= 100.0 => "unavailable",
        (Some(_), None) => "primary_window_available_only",
        (Some(_), Some(s)) if s >= 100.0 => "unavailable",
        _ => "available",
    }
    .to_string();

    let captured_at = chrono::Utc::now().timestamp();

    Ok(AccountUsage {
        account_id: id.to_string(),
        used_percent,
        window_minutes,
        resets_at,
        secondary_used_percent,
        secondary_window_minutes,
        secondary_resets_at,
        availability,
        captured_at,
    })
}

fn usage_score(usage: &AccountUsage) -> f64 {
    if usage.availability == "unavailable" {
        return -1.0;
    }
    let primary = usage.used_percent.unwrap_or(100.0);
    let secondary = usage.secondary_used_percent.unwrap_or(primary);
    let max_used = primary.max(secondary);
    100.0 - max_used
}

async fn select_highest_quota_account(
    state: Arc<ProxyState>,
    exclude_idx: usize,
) -> Option<(usize, String, String, Option<String>)> {
    let mut exclude = HashSet::new();
    exclude.insert(exclude_idx);
    select_highest_quota_account_excluding(state, &exclude).await
}

async fn select_highest_quota_account_excluding(
    state: Arc<ProxyState>,
    exclude: &HashSet<usize>,
) -> Option<(usize, String, String, Option<String>)> {
    let candidates = {
        let accounts_lock = state.accounts.read().unwrap();
        accounts_lock
            .iter()
            .enumerate()
            .filter(|(idx, acc)| !exclude.contains(idx) && acc.health == AccountHealth::Active)
            .map(|(idx, acc)| {
                (
                    idx,
                    acc.id.clone(),
                    acc.access_token.clone(),
                    acc.account_id.clone(),
                )
            })
            .collect::<Vec<_>>()
    };

    let fallback = candidates.first().cloned();
    let mut best: Option<(usize, String, String, Option<String>, f64)> = None;
    for (idx, id, token, account_id) in candidates {
        if let Ok(usage) = fetch_account_usage_by_id(&id).await {
            let score = usage_score(&usage);
            match best {
                Some((_, _, _, _, best_score)) if score <= best_score => {}
                _ => best = Some((idx, id, token, account_id, score)),
            }
        }
    }

    match best {
        Some((idx, id, token, account_id, _)) => Some((idx, id, token, account_id)),
        None => fallback,
    }
}

/// Fetch rate-limit / usage snapshot for a managed account from chatgpt.com.
#[tauri::command]
async fn get_account_usage(id: String) -> Result<AccountUsage, String> {
    fetch_account_usage_by_id(&id).await
}

/// Fetch available Codex models from upstream for UI selection.
#[tauri::command]
async fn list_codex_models() -> Result<Vec<String>, String> {
    let cfg = proxy_config_snapshot();
    if proxy_uses_custom_openai(&cfg) {
        let configured = configured_proxy_models(&cfg);
        if !configured.is_empty() {
            return Ok(configured);
        }
        let base_url =
            custom_openai_base_url(&cfg).ok_or_else(|| "未配置自定义 OpenAI 地址".to_string())?;
        let api_key =
            custom_openai_api_key(&cfg).ok_or_else(|| "未配置自定义 OpenAI API Key".to_string())?;
        let client = reqwest::Client::new();
        let url = build_upstream_url_with_base(&base_url, "/v1/models");
        log_proxy(&format!("models: custom request -> {url}"));
        let mut headers = reqwest::header::HeaderMap::new();
        apply_custom_openai_headers(&mut headers, &api_key, false, false);
        let resp = client
            .get(&url)
            .headers(headers)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let status = resp.status().as_u16();
        let body_bytes = resp.bytes().await.map_err(|e| e.to_string())?;
        let body_text = truncate_body(&body_bytes);
        log_proxy(&format!("models: custom status {status} body={body_text}"));
        if !(200..300).contains(&status) {
            return Err(format!(
                "获取模型失败 (HTTP {status})，且未配置模型覆盖。请直接填写模型名称，例如 glm-5。"
            ));
        }
        let body: Value = serde_json::from_slice(&body_bytes).map_err(|e| e.to_string())?;
        let mut models: Vec<String> = Vec::new();
        if let Some(arr) = body.get("data").and_then(|v| v.as_array()) {
            for item in arr {
                for field in ["id", "model", "slug"] {
                    if let Some(id) = item
                        .get(field)
                        .and_then(|v| v.as_str())
                        .filter(|v| !v.is_empty())
                    {
                        models.push(id.to_string());
                        break;
                    }
                }
            }
        }
        models.sort();
        models.dedup();
        if models.is_empty() {
            return Err("模型列表为空，请直接填写模型名称，例如 glm-5。".to_string());
        }
        return Ok(models);
    }

    let (auth_data, is_managed) = load_any_auth_data()?;
    let (access_token, refresh_token, account_id) = extract_auth_tokens(&auth_data);
    if access_token.trim().is_empty() {
        return Err("账号缺少 access_token，请重新登录。".to_string());
    }

    let client = reqwest::Client::new();
    let path = normalize_models_path("/v1/models");
    let url = build_upstream_url(&path);
    log_proxy(&format!("models: request -> {url}"));
    let mut headers = reqwest::header::HeaderMap::new();
    let incoming_headers = axum::http::HeaderMap::new();
    apply_upstream_headers(
        &mut headers,
        &access_token,
        account_id.as_deref(),
        &incoming_headers,
        false,
        false,
    );

    let mut resp = client
        .get(&url)
        .headers(headers.clone())
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if resp.status() == reqwest::StatusCode::UNAUTHORIZED && is_managed {
        if let (Some(id), Some(rt)) = (account_id.as_deref(), refresh_token.as_deref()) {
            if let Some(new_token) = try_refresh_account(id, rt).await {
                headers.insert(
                    reqwest::header::AUTHORIZATION,
                    reqwest::header::HeaderValue::from_str(&format!("Bearer {}", new_token))
                        .unwrap_or_else(|_| reqwest::header::HeaderValue::from_static("")),
                );
                resp = client
                    .get(&url)
                    .headers(headers)
                    .send()
                    .await
                    .map_err(|e| e.to_string())?;
            }
        }
    }

    let status = resp.status().as_u16();
    let body_bytes = resp.bytes().await.map_err(|e| e.to_string())?;
    let body_text = truncate_body(&body_bytes);
    log_proxy(&format!("models: status {status} body={body_text}"));
    if !(200..300).contains(&status) {
        return Err(format!("获取模型失败 (HTTP {status}): {body_text}"));
    }

    let body: Value = serde_json::from_slice(&body_bytes).map_err(|e| e.to_string())?;
    let mut models: Vec<String> = Vec::new();
    if let Some(arr) = body.get("data").and_then(|v| v.as_array()) {
        for item in arr {
            if let Some(id) = item.get("id").and_then(|v| v.as_str()) {
                if !id.is_empty() {
                    models.push(id.to_string());
                    continue;
                }
            }
            if let Some(id) = item.get("model").and_then(|v| v.as_str()) {
                if !id.is_empty() {
                    models.push(id.to_string());
                }
            }
        }
    } else if let Some(arr) = body.get("models").and_then(|v| v.as_array()) {
        for item in arr {
            if let Some(id) = item.as_str() {
                if !id.is_empty() {
                    models.push(id.to_string());
                    continue;
                }
            }
            if let Some(id) = item.get("id").and_then(|v| v.as_str()) {
                if !id.is_empty() {
                    models.push(id.to_string());
                    continue;
                }
            }
            if let Some(id) = item.get("slug").and_then(|v| v.as_str()) {
                if !id.is_empty() {
                    models.push(id.to_string());
                }
            }
        }
    } else if let Some(arr) = body
        .get("result")
        .and_then(|v| v.get("models"))
        .and_then(|v| v.as_array())
    {
        for item in arr {
            if let Some(id) = item.as_str() {
                if !id.is_empty() {
                    models.push(id.to_string());
                }
            }
        }
    }
    models.sort();
    models.dedup();
    if models.is_empty() {
        return Err(format!(
            "模型列表为空，请检查账号权限或稍后重试。响应：{body_text}"
        ));
    }
    Ok(models)
}

// ─── App entry ───────────────────────────────────────────────────────────────

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .setup(|app| {
            let _ = APP_HANDLE.set(app.handle().clone());
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            list_accounts,
            get_current_account,
            switch_account,
            delete_account,
            update_label,
            update_proxy_enabled,
            import_current,
            get_config,
            launch_codex_login,
            oauth_login,
            get_oauth_url,
            complete_oauth_manual,
            refresh_account_token,
            get_account_usage,
            list_codex_models,
            start_api_proxy,
            stop_api_proxy,
            reload_proxy_accounts,
            get_proxy_status,
            list_openai_compat_configs,
            create_openai_compat_config,
            update_openai_compat_config,
            delete_openai_compat_config,
            list_openai_compat_provider_models,
            probe_openai_compat_config,
            start_openai_compat_proxy,
            stop_openai_compat_proxy,
            get_openai_compat_proxy_status,
            get_proxy_config,
            update_proxy_config,
            generate_proxy_api_key,
            clear_proxy_logs,
            get_proxy_logs_count_filtered,
            get_proxy_logs_filtered,
            get_proxy_log_detail,
            get_proxy_token_stats,
            get_ai_cache_overview,
            get_ai_cache_trend,
            list_ai_cache_entries,
            clear_ai_cache,
            list_anthropic_keys,
            add_anthropic_key,
            delete_anthropic_key,
            update_anthropic_key_label,
            start_anthropic_proxy,
            stop_anthropic_proxy,
            get_anthropic_proxy_status,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
