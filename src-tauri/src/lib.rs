use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
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

#[derive(Serialize, Deserialize)]
struct ProxyStatus {
    running: bool,
    port: Option<u16>,
    active_email: Option<String>,
}

// Global proxy shutdown sender
static PROXY_SHUTDOWN: Mutex<Option<oneshot::Sender<()>>> = Mutex::new(None);
static PROXY_PORT: Mutex<Option<u16>> = Mutex::new(None);

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

// OAuth parameters for OpenAI / Auth0
const AUTH0_DOMAIN: &str = "auth0.openai.com";
const CLIENT_ID: &str = "TdJIcbe16WoTHlnIWebsggJCKDGMHMHq";
const SCOPE: &str = "openid email profile offline_access";
const AUDIENCE: &str = "https://api.openai.com/v1";

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

fn find_free_port() -> Option<u16> {
    TcpListener::bind("127.0.0.1:0")
        .ok()
        .and_then(|l| l.local_addr().ok())
        .map(|a| a.port())
}

fn build_auth_url(redirect_uri: &str, code_challenge: &str, state: &str) -> String {
    let domain = AUTH0_DOMAIN;
    format!(
        "https://{domain}/authorize\
         ?response_type=code\
         &client_id={CLIENT_ID}\
         &redirect_uri={redirect_uri}\
         &scope={scope}\
         &audience={audience}\
         &code_challenge={code_challenge}\
         &code_challenge_method=S256\
         &state={state}",
        redirect_uri = percent_encoding::utf8_percent_encode(
            redirect_uri,
            percent_encoding::NON_ALPHANUMERIC
        ),
        scope = percent_encoding::utf8_percent_encode(SCOPE, percent_encoding::NON_ALPHANUMERIC),
        audience =
            percent_encoding::utf8_percent_encode(AUDIENCE, percent_encoding::NON_ALPHANUMERIC),
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

/// Start in-app OAuth login flow. Opens browser, waits for callback,
/// exchanges code, saves auth.json, returns the new account.
#[tauri::command]
async fn oauth_login(label: Option<String>) -> Result<Value, String> {
    let port = find_free_port().ok_or("Could not find free port")?;
    let redirect_uri = format!("http://127.0.0.1:{port}/callback");
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

    // Local HTTP server to catch callback
    let listener =
        tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
            .await
            .map_err(|e| e.to_string())?;

    // We only need one request; use a channel to get the query string
    let (tx, rx) = tokio::sync::oneshot::channel::<String>();
    let tx = Arc::new(Mutex::new(Some(tx)));

    let server = {
        let tx = tx.clone();
        let state_check = state.clone();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = vec![0u8; 4096];
                let n = stream.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);
                // Parse GET /callback?code=...&state=...
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
        })
    };

    // Wait for callback (max 3 minutes)
    let qs = tokio::time::timeout(std::time::Duration::from_secs(180), rx)
        .await
        .map_err(|_| "Login timed out (3 min). Please try again.")?
        .map_err(|_| "Login cancelled")?;

    server.abort();

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

/// Start a local HTTP server that proxies requests to api.openai.com
/// using the currently active account's access token.
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

    // Read active account's access token
    let auth_path = auth_file();
    if !auth_path.exists() {
        return Err("No active account. Please login first.".into());
    }
    let content = fs::read_to_string(&auth_path).map_err(|e| e.to_string())?;
    let auth_data: Value = serde_json::from_str(&content).map_err(|e| e.to_string())?;

    let empty = Value::Object(Default::default());
    let tokens = auth_data.get("tokens").unwrap_or(&empty);
    let access_token = tokens
        .get("access_token")
        .and_then(|v| v.as_str())
        .ok_or("No access token found")?
        .to_string();
    let active_email = {
        let account = parse_auth_data(&auth_data, "proxy");
        account.email.clone()
    };

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    {
        let mut lock = PROXY_SHUTDOWN.lock().unwrap();
        *lock = Some(shutdown_tx);
    }
    {
        let mut lock = PROXY_PORT.lock().unwrap();
        *lock = Some(proxy_port);
    }

    // Build axum proxy app
    use axum::{
        body::Body,
        extract::State,
        http::{Method, Request, StatusCode},
        response::Response,
        routing::any,
        Router,
    };

    #[derive(Clone)]
    struct ProxyState {
        token: String,
        client: reqwest::Client,
    }

    async fn proxy_handler(
        State(state): State<ProxyState>,
        req: Request<Body>,
    ) -> Result<Response<Body>, StatusCode> {
        let path = req.uri().path_and_query().map(|p| p.as_str()).unwrap_or("/");
        let target = format!("https://api.openai.com{path}");

        let method = reqwest::Method::from_bytes(req.method().as_str().as_bytes())
            .unwrap_or(reqwest::Method::GET);

        let body_bytes = axum::body::to_bytes(req.into_body(), 10 * 1024 * 1024)
            .await
            .map_err(|_| StatusCode::BAD_REQUEST)?;

        let mut upstream = state
            .client
            .request(method, &target)
            .header("Authorization", format!("Bearer {}", state.token))
            .header("Content-Type", "application/json")
            .body(body_bytes);

        let upstream_resp = upstream
            .send()
            .await
            .map_err(|_| StatusCode::BAD_GATEWAY)?;

        let status = axum::http::StatusCode::from_u16(upstream_resp.status().as_u16())
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

        let mut builder = Response::builder().status(status);
        for (k, v) in upstream_resp.headers() {
            if let Ok(name) = axum::http::HeaderName::from_bytes(k.as_str().as_bytes()) {
                if let Ok(val) = axum::http::HeaderValue::from_bytes(v.as_bytes()) {
                    builder = builder.header(name, val);
                }
            }
        }
        // CORS
        builder = builder.header("Access-Control-Allow-Origin", "*");

        let body_bytes = upstream_resp.bytes().await.map_err(|_| StatusCode::BAD_GATEWAY)?;
        Ok(builder
            .body(Body::from(body_bytes))
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?)
    }

    let proxy_state = ProxyState {
        token: access_token,
        client: reqwest::Client::new(),
    };
    let app = Router::new()
        .route("/{*path}", any(proxy_handler))
        .route("/", any(proxy_handler))
        .with_state(proxy_state);

    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{proxy_port}"))
        .await
        .map_err(|e| format!("Cannot bind port {proxy_port}: {e}"))?;

    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .ok();

        let mut lock = PROXY_PORT.lock().unwrap();
        *lock = None;
    });

    Ok(serde_json::json!({
        "success": true,
        "port": proxy_port,
        "active_email": active_email,
        "base_url": format!("http://127.0.0.1:{proxy_port}")
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

#[tauri::command]
fn get_proxy_status() -> Result<ProxyStatus, String> {
    let port = *PROXY_PORT.lock().unwrap();
    let running = port.is_some();

    let active_email = if running {
        auth_file()
            .exists()
            .then(|| {
                fs::read_to_string(auth_file())
                    .ok()
                    .and_then(|c| serde_json::from_str::<Value>(&c).ok())
                    .map(|v| parse_auth_data(&v, "proxy").email)
            })
            .flatten()
    } else {
        None
    };

    Ok(ProxyStatus {
        running,
        port,
        active_email,
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
            oauth_login,
            refresh_account_token,
            start_api_proxy,
            stop_api_proxy,
            get_proxy_status,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
