use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use rusqlite::{params, Connection};
use bytes::Bytes;
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::io;
use std::path::PathBuf;
use std::sync::{atomic::{AtomicUsize, Ordering}, Arc, Mutex, OnceLock, RwLock};
use tokio::sync::Notify;
use tokio::sync::oneshot;

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

#[derive(Serialize, Deserialize, Clone)]
struct ProxyConfig {
    api_key: Option<String>,
    enable_logging: bool,
    max_logs: usize,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            api_key: None,
            enable_logging: true,
            max_logs: 1000,
        }
    }
}

static PROXY_CONFIG: OnceLock<Mutex<ProxyConfig>> = OnceLock::new();

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

fn proxy_config_snapshot() -> ProxyConfig {
    proxy_config().lock().unwrap().clone()
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
}

struct ProxyLogEntry {
    timestamp: String,
    method: String,
    path: String,
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
        "CREATE TABLE IF NOT EXISTS request_logs (            id INTEGER PRIMARY KEY AUTOINCREMENT,            timestamp TEXT NOT NULL,            method TEXT NOT NULL,            path TEXT NOT NULL,            status INTEGER NOT NULL,            duration_ms INTEGER NOT NULL,            proxy_account_id TEXT NOT NULL,            account_id TEXT,            error TEXT,            request_headers TEXT,            response_headers TEXT,            request_body TEXT,            response_body TEXT,            model TEXT,            input_tokens INTEGER,            output_tokens INTEGER        )",
        [],
    )
    .map_err(|e| e.to_string())?;
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
    Ok(())
}

fn insert_proxy_log(entry: &ProxyLogEntry) -> Result<(), String> {
    let cfg = proxy_config_snapshot();
    if !cfg.enable_logging {
        return Ok(());
    }
    let conn = proxy_log_db()?;
    conn.execute(
        "INSERT INTO request_logs (timestamp, method, path, status, duration_ms, proxy_account_id, account_id, error, request_headers, response_headers, request_body, response_body, model, input_tokens, output_tokens)         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
        params![
            entry.timestamp,
            entry.method,
            entry.path,
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
        text.push_str(&format!("
...truncated {} bytes", bytes.len() - MAX_LOG_BODY_BYTES));
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
// ─── types ───────────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Clone)]
struct MetaEntry {
    label: Option<String>,
    added_at: u64,
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
    accounts_dir: PathBuf,
}

// Global proxy shutdown sender and live state
static PROXY_SHUTDOWN: Mutex<Option<oneshot::Sender<()>>> = Mutex::new(None);
static PROXY_PORT: Mutex<Option<u16>> = Mutex::new(None);
// Shared live proxy state for status queries and hot-reload
static PROXY_STATE: Mutex<Option<Arc<ProxyState>>> = Mutex::new(None);

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
    }
}

// ─── OAuth PKCE helpers ───────────────────────────────────────────────────────

// OAuth parameters for OpenAI
const AUTH0_DOMAIN: &str = "auth.openai.com";
const CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const SCOPE: &str = "openid profile email offline_access";
const AUDIENCE: &str = "https://api.openai.com/v1";
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
        redirect_uri = percent_encoding::utf8_percent_encode(
            redirect_uri,
            percent_encoding::NON_ALPHANUMERIC
        ),
        scope = percent_encoding::utf8_percent_encode(SCOPE, percent_encoding::NON_ALPHANUMERIC),
    )
}

async fn exchange_code(
    code: &str,
    redirect_uri: &str,
    verifier: &str,
) -> Result<Value, String> {
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

    let now_iso = chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string();

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
    let auth_path = accounts_dir().join(&id).join("auth.json");
    if !auth_path.exists() {
        return Err("Account not found".into());
    }
    fs::copy(&auth_path, auth_file()).map_err(|e| e.to_string())?;
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
    });
    entry.label = if label.is_empty() { None } else { Some(label) };
    write_meta(&meta);
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
    let listener_v6 = tokio::net::TcpListener::bind(format!("[::1]:{port}")).await.ok();

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

    let now_iso = chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string();

    let updated = serde_json::json!({
        "tokens": {
            "access_token": new_access,
            "id_token": new_id,
            "refresh_token": new_refresh,
            "account_id": account_id,
        },
        "last_refresh": now_iso,
    });

    fs::write(
        &auth_path,
        serde_json::to_string_pretty(&updated).unwrap(),
    )
    .map_err(|e| e.to_string())?;

    // If this is the active account, update auth.json too
    if let Ok(current_content) = fs::read_to_string(auth_file()) {
        if let Ok(current) = serde_json::from_str::<Value>(&current_content) {
            let curr_tokens = current.get("tokens").unwrap_or(&empty);
            let curr_rt = curr_tokens.get("refresh_token").and_then(|v| v.as_str());
            if curr_rt == Some(refresh_token) {
                let _ = fs::write(
                    auth_file(),
                    serde_json::to_string_pretty(&updated).unwrap(),
                );
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

fn build_upstream_url(path_and_query: &str) -> String {
    let base = upstream_base_url();
    let base = base.trim_end_matches('/');
    if base.contains("/backend-api/codex") && path_and_query.starts_with("/v1/") {
        format!("{base}{}", path_and_query.trim_start_matches("/v1"))
    } else if base.ends_with("/v1") && path_and_query.starts_with("/v1") {
        format!("{}{}", base.trim_end_matches("/v1"), path_and_query)
    } else {
        format!("{base}{path_and_query}")
    }
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

async fn run_proxy_server(
    addr: &str,
    app: axum::Router,
    shutdown: Arc<Notify>,
) -> io::Result<()> {
    // Bind dual-stack when using localhost.
    let addr_trimmed = addr.trim();
    if addr_trimmed.len() > "localhost:".len()
        && addr_trimmed[..("localhost:".len())].eq_ignore_ascii_case("localhost:")
    {
        let port = &addr_trimmed["localhost:".len()..];
        log_proxy(&format!("binding listeners: 127.0.0.1:{port}, [::1]:{port}"));
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

    if !accounts_path.exists() {
        return Err("No accounts directory found. Please login at least one account.".into());
    }

    let entries = fs::read_dir(&accounts_path).map_err(|e| e.to_string())?;
    for entry in entries.flatten() {
        let id = entry.file_name().to_string_lossy().to_string();
        let auth_path = entry.path().join("auth.json");
        if !auth_path.exists() { continue; }

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
        Err("No valid access tokens found. Please login at least one account.".into())
    } else {
        Ok(pool)
    }
}

/// Headers that should NOT be forwarded to upstream
fn skip_request_header(name: &str) -> bool {
    matches!(name.to_lowercase().as_str(),
        "host" | "connection" | "keep-alive" | "proxy-authenticate" |
        "proxy-authorization" | "authorization" | "te" | "trailers" | "transfer-encoding" |
        "upgrade" | "content-length"
    )
}

/// Headers that should NOT be forwarded back to client
fn skip_response_header(name: &str) -> bool {
    matches!(name.to_lowercase().as_str(),
        "connection" | "keep-alive" | "transfer-encoding" | "upgrade" |
        "proxy-authenticate" | "content-length"
    )
}

/// Try to refresh the token for an account and persist to disk, returns new access_token on success
async fn try_refresh_account(account_id: &str, refresh_token: &str) -> Option<String> {
    let token_resp = do_token_refresh(refresh_token).await.ok()?;

    let new_access = token_resp.get("access_token").and_then(|v| v.as_str())?.to_string();
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

    let proxy_port = port.unwrap_or(8080);
    let accounts = match load_proxy_accounts() {
        Ok(accounts) => accounts,
        Err(err) => {
            log_proxy(&format!("start failed: {err}"));
            return Err(err);
        }
    };
    let account_count = accounts.len();
    log_proxy(&format!("start requested: port={proxy_port} accounts={account_count}"));

    log_proxy("init shutdown channel");
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    {
        let mut lock = PROXY_SHUTDOWN.lock().unwrap();
        *lock = Some(shutdown_tx);
    }

    use axum::{
        body::Body,
        extract::State,
        http::{Request, StatusCode},
        response::Response,
        routing::any,
        Router,
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
        accounts_dir: accounts_dir(),
    });
    log_proxy("proxy state ready");

    async fn proxy_handler(
        State(state): State<Arc<ProxyState>>,
        req: Request<Body>,
    ) -> Response<Body> {
        let request_id = PROXY_REQ_ID.fetch_add(1, Ordering::SeqCst);
        // Handle CORS preflight
        if req.method() == axum::http::Method::OPTIONS {
            return Response::builder()
                .status(StatusCode::NO_CONTENT)
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, PATCH, OPTIONS")
                .header("Access-Control-Allow-Headers", "*")
                .header("Access-Control-Max-Age", "86400")
                .body(Body::empty())
                .unwrap();
        }

        let req_headers = req.headers().clone();
        let path = req.uri().path_and_query().map(|p| p.as_str()).unwrap_or("/");
        let path = normalize_models_path(path);
        let target = build_upstream_url(&path);
        let method = reqwest::Method::from_bytes(req.method().as_str().as_bytes())
            .unwrap_or(reqwest::Method::GET);
        let method_label = method.to_string();
        let started_at = std::time::Instant::now();
        log_proxy(&format!("req#{request_id} start {method_label} {path} -> {target}"));

        // Collect and filter incoming headers (pass them through, except hop-by-hop)
        let mut forward_headers = reqwest::header::HeaderMap::new();
        for (k, v) in req.headers() {
            if skip_request_header(k.as_str()) { continue; }
            if let (Ok(name), Ok(val)) = (
                reqwest::header::HeaderName::from_bytes(k.as_str().as_bytes()),
                reqwest::header::HeaderValue::from_bytes(v.as_bytes()),
            ) {
                forward_headers.insert(name, val);
            }
        }

        let max_body_bytes = front_proxy_max_body_bytes();
        if let Some(content_length) = req
            .headers()
            .get(axum::http::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<u64>().ok())
        {
            if content_length > max_body_bytes as u64 {
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
                return Response::builder()
                    .status(StatusCode::PAYLOAD_TOO_LARGE)
                    .header("Access-Control-Allow-Origin", "*")
                    .body(Body::from("Request body too large"))
                    .unwrap();
            }
        };

        let request_headers_json = headers_to_json_string(sanitize_headers(&req_headers));
        let request_body_text = if body_bytes.is_empty() {
            None
        } else {
            Some(truncate_body(&body_bytes))
        };
        let request_model = extract_model(&body_bytes);

        if !proxy_api_key_valid(&req_headers) {
            let entry = ProxyLogEntry {
                timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                method: method_label.clone(),
                path: path.to_string(),
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
            };
            let _ = insert_proxy_log(&entry);
            return Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .header("Access-Control-Allow-Origin", "*")
                .body(Body::from("Unauthorized"))
                .unwrap();
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
        let is_stream = forward_headers
            .get(reqwest::header::ACCEPT)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.contains("text/event-stream"))
            .unwrap_or(false);
        let mut upstream_headers = forward_headers.clone();
        apply_upstream_headers(
            &mut upstream_headers,
            &chosen_token,
            chosen_account_id.as_deref(),
            &req_headers,
            !body_bytes.is_empty(),
            is_stream,
        );

        let upstream_result = state.client
            .request(method.clone(), &target)
            .headers(upstream_headers)
            .body(body_bytes.clone())
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
        log_proxy(&format!("req#{request_id} upstream status: {}", upstream_status.as_u16()));

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
                    let mut retry_headers = forward_headers;
                    apply_upstream_headers(
                        &mut retry_headers,
                        &new_token,
                        chosen_account_id.as_deref(),
                        &req_headers,
                        !body_bytes.is_empty(),
                        is_stream,
                    );
                    if let Ok(retry_resp) = state.client
                        .request(method, &target)
                        .headers(retry_headers)
                        .body(body_bytes)
                        .send()
                        .await
                    {
                        let response_headers_json = headers_to_json_string(sanitize_reqwest_headers(retry_resp.headers()));
                        if !is_stream {
                            let status = retry_resp.status();
                            let headers = retry_resp.headers().clone();
                            let bytes = match retry_resp.bytes().await {
                                Ok(b) => b,
                                Err(e) => {
                                    let entry = ProxyLogEntry {
                                        timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                                        method: method_label.clone(),
                                        path: path.to_string(),
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
                                    };
                                    let _ = insert_proxy_log(&entry);
                                    return Response::builder()
                                        .status(StatusCode::BAD_GATEWAY)
                                        .header("Access-Control-Allow-Origin", "*")
                                        .body(Body::from(format!("Upstream error: {e}")))
                                        .unwrap();
                                }
                            };
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
                            };
                            let _ = insert_proxy_log(&entry);
                            return build_proxy_response_from_bytes(status, &headers, bytes);
                        }

                        let entry = ProxyLogEntry {
                            timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                            method: method_label.clone(),
                            path: path.to_string(),
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
            // Cooldown for COOLDOWN_SECS seconds
            let until = std::time::Instant::now()
                + std::time::Duration::from_secs(COOLDOWN_SECS);
            let mut accounts_lock = state.accounts.write().unwrap();
            if let Some(acc) = accounts_lock.get_mut(chosen_idx) {
                acc.health = AccountHealth::Cooldown(until);
            }
        }

        let response_headers_json = headers_to_json_string(sanitize_reqwest_headers(upstream_resp.headers()));
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
                    };
                    let _ = insert_proxy_log(&entry);
                    return Response::builder()
                        .status(StatusCode::BAD_GATEWAY)
                        .header("Access-Control-Allow-Origin", "*")
                        .body(Body::from(format!("Upstream error: {e}")))
                        .unwrap();
                }
            };
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
            };
            let _ = insert_proxy_log(&entry);
            return build_proxy_response_from_bytes(status, &headers, bytes);
        }

        let entry = ProxyLogEntry {
            timestamp: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            method: method_label.clone(),
            path: path.to_string(),
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
            if skip_response_header(k.as_str()) { continue; }
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
            if skip_response_header(k.as_str()) { continue; }
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
        std::net::TcpStream::connect_timeout(
            &socket_addr,
            std::time::Duration::from_millis(200),
        )
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
            let active = accounts.iter().filter(|a| a.health == AccountHealth::Active).count();
            let cd = accounts.iter().filter(|a| matches!(&a.health, AccountHealth::Cooldown(u) if now < *u)).count();
            let bl = accounts.iter().filter(|a| a.health == AccountHealth::Blocked).count();
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
) -> Result<ProxyConfig, String> {
    let mut cfg = proxy_config_snapshot();
    if let Some(value) = api_key {
        let trimmed = value.trim().to_string();
        cfg.api_key = if trimmed.is_empty() { None } else { Some(trimmed) };
    }
    if let Some(value) = enable_logging {
        cfg.enable_logging = value;
    }
    if let Some(value) = max_logs {
        cfg.max_logs = value.max(1);
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
    conn.execute("DELETE FROM request_logs", []).map_err(|e| e.to_string())?;
    Ok(serde_json::json!({ "success": true }))
}

#[tauri::command]
fn get_proxy_logs_count_filtered(filter: Option<String>, errors_only: Option<bool>) -> Result<usize, String> {
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
        format!("SELECT COUNT(*) FROM request_logs WHERE {}", clauses.join(" AND "))
    };
    let count: i64 = if filter.is_empty() {
        conn.query_row(&sql, [], |row| row.get(0)).map_err(|e| e.to_string())?
    } else {
        let pattern = format!("%{}%", filter);
        conn.query_row(&sql, params![pattern], |row| row.get(0)).map_err(|e| e.to_string())?
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
            (format!("{sql_base} ORDER BY id DESC LIMIT ?1 OFFSET ?2"), vec![limit.into(), offset.into()])
        }
    } else {
        let pattern = format!("%{}%", filter).into();
        if errors_only {
            (format!("{sql_base} WHERE (status < 200 OR status >= 400) AND {filter_clause} ORDER BY id DESC LIMIT ?2 OFFSET ?3"), vec![pattern, limit.into(), offset.into()])
        } else {
            (format!("{sql_base} WHERE {filter_clause} ORDER BY id DESC LIMIT ?2 OFFSET ?3"), vec![pattern, limit.into(), offset.into()])
        }
    };
    let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
    let logs_iter = stmt.query_map(rusqlite::params_from_iter(params_vec), |row| {
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
    }).map_err(|e| e.to_string())?;
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
        "SELECT id, timestamp, method, path, status, duration_ms, proxy_account_id, account_id, error, model, request_headers, response_headers, request_body, response_body, input_tokens, output_tokens FROM request_logs WHERE id = ?1",
    ).map_err(|e| e.to_string())?;
    let log = stmt.query_row(params![log_id], |row| {
        Ok(ProxyLogDetail {
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
            request_headers: row.get(10)?,
            response_headers: row.get(11)?,
            request_body: row.get(12)?,
            response_body: row.get(13)?,
            input_tokens: row.get(14)?,
            output_tokens: row.get(15)?,
        })
    }).map_err(|e| e.to_string())?;
    Ok(log)
}

// ─── Tauri commands: account usage ───────────────────────────────────────────

#[derive(Serialize, Deserialize, Clone)]
struct AccountUsage {
    account_id: String,
    // primary window (短窗口，约5小时)
    used_percent: Option<f64>,
    window_minutes: Option<u64>,
    resets_at: Option<i64>,       // unix timestamp seconds
    // secondary window (长窗口，约7天)
    secondary_used_percent: Option<f64>,
    secondary_window_minutes: Option<u64>,
    secondary_resets_at: Option<i64>,
    // meta
    availability: String, // "available" | "unavailable" | "primary_window_available_only" | "unknown"
    captured_at: i64,     // unix timestamp seconds when this data was fetched
}

/// Fetch rate-limit / usage snapshot for a managed account from chatgpt.com.
#[tauri::command]
async fn get_account_usage(id: String) -> Result<AccountUsage, String> {
    let auth_path = accounts_dir().join(&id).join("auth.json");
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

    let used_percent           = pw.and_then(|v| v.get("used_percent")).and_then(|v| v.as_f64());
    let window_secs            = pw.and_then(|v| v.get("limit_window_seconds")).and_then(|v| v.as_u64());
    let window_minutes         = window_secs.map(|s| (s + 59) / 60);
    let resets_at              = pw.and_then(|v| v.get("reset_at")).and_then(|v| v.as_i64());

    let secondary_used_percent = sw.and_then(|v| v.get("used_percent")).and_then(|v| v.as_f64());
    let secondary_secs         = sw.and_then(|v| v.get("limit_window_seconds")).and_then(|v| v.as_u64());
    let secondary_window_minutes = secondary_secs.map(|s| (s + 59) / 60);
    let secondary_resets_at    = sw.and_then(|v| v.get("reset_at")).and_then(|v| v.as_i64());

    let availability = match (used_percent, secondary_used_percent) {
        (None, _)                                       => "unknown",
        (Some(p), _) if p >= 100.0                      => "unavailable",
        (Some(_), None)                                 => "primary_window_available_only",
        (Some(_), Some(s)) if s >= 100.0                => "unavailable",
        _                                               => "available",
    }.to_string();

    let captured_at = chrono::Utc::now().timestamp();

    Ok(AccountUsage {
        account_id: id,
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

// ─── App entry ───────────────────────────────────────────────────────────────

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![
            list_accounts,
            get_current_account,
            switch_account,
            delete_account,
            update_label,
            import_current,
            get_config,
            launch_codex_login,
            oauth_login,
            refresh_account_token,
            get_account_usage,
            start_api_proxy,
            stop_api_proxy,
            reload_proxy_accounts,
            get_proxy_status,
            get_proxy_config,
            update_proxy_config,
            generate_proxy_api_key,
            clear_proxy_logs,
            get_proxy_logs_count_filtered,
            get_proxy_logs_filtered,
            get_proxy_log_detail,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
