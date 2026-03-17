import { invoke } from '@tauri-apps/api/core'
import type { CodexAccount } from '../types/account'

export interface AccountUsage {
  account_id: string
  // primary window (短窗口，约5小时)
  used_percent: number | null
  window_minutes: number | null
  resets_at: number | null
  // secondary window (长窗口，约7天)
  secondary_used_percent: number | null
  secondary_window_minutes: number | null
  secondary_resets_at: number | null
  // meta
  availability: 'available' | 'unavailable' | 'primary_window_available_only' | 'unknown'
  captured_at: number  // unix seconds
}

export interface ProxyStatus {
  running: boolean
  port: number | null
  account_count: number
  active: number
  cooldown: number
  blocked: number
}

export interface OpenAICompatModelMapping {
  alias: string
  provider_model: string
}

export interface OpenAICompatConfig {
  id: string
  provider_name: string
  base_url: string
  api_key: string
  default_model: string | null
  model_mappings: OpenAICompatModelMapping[]
  created_at: number
  updated_at: number
}

export interface OpenAICompatProxyStatus {
  running: boolean
  port: number | null
  config_id: string | null
  provider_name: string | null
}

export interface ProxyConfig {
  api_key: string | null
  enable_logging: boolean
  max_logs: number
  disable_on_usage_limit: boolean
  model_override: string | null
  reasoning_effort_override: string | null
  upstream_mode: string
  custom_openai_base_url: string | null
  custom_openai_api_key: string | null
}

export interface ProxyRequestLog {
  id: number
  timestamp: string
  method: string
  path: string
  status: number
  duration_ms: number
  proxy_account_id: string
  account_id: string | null
  error: string | null
  model: string | null
}

export interface ProxyLogDetail extends ProxyRequestLog {
  request_url: string | null
  request_headers: string | null
  response_headers: string | null
  request_body: string | null
  response_body: string | null
  input_tokens: number | null
  output_tokens: number | null
}

export interface ProxyTokenStatsItem {
  name: string
  requests: number
  input_tokens: number
  output_tokens: number
  total_tokens: number
}

export interface ProxyTokenStats {
  window_hours: number
  total_requests: number
  success_requests: number
  error_requests: number
  input_tokens: number
  output_tokens: number
  total_tokens: number
  avg_duration_ms: number
  top_models: ProxyTokenStatsItem[]
  top_accounts: ProxyTokenStatsItem[]
}

export interface AnthropicKeyEntry {
  id: string
  label: string | null
  key: string
  added_at: number
}

export const accountService = {
  list: () => invoke<CodexAccount[]>('list_accounts'),
  current: () => invoke<CodexAccount | null>('get_current_account'),
  switch: (id: string) => invoke<boolean>('switch_account', { id }),
  delete: (id: string) => invoke<boolean>('delete_account', { id }),
  updateLabel: (id: string, label: string) => invoke<boolean>('update_label', { id, label }),
  updateProxyEnabled: (id: string, enabled: boolean) => invoke<boolean>('update_proxy_enabled', { id, enabled }),
  importCurrent: (label?: string) =>
    invoke<{ success: boolean; id: string; email: string }>('import_current', { label: label ?? null }),
  login: () => invoke<{ success: boolean; message: string }>('launch_codex_login'),
  getConfig: () => invoke<{ raw: string }>('get_config'),

  oauthLogin: (label?: string) =>
    invoke<{ success: boolean; email: string; plan: string; id: string }>('oauth_login', { label: label ?? null }),
  getOAuthUrl: () =>
    invoke<{ auth_url: string }>('get_oauth_url'),
  completeOAuthManual: (callbackUrl: string, label?: string) =>
    invoke<{ success: boolean; email: string; plan: string; id: string }>('complete_oauth_manual', { callbackUrl, label: label ?? null }),
  refreshToken: (id: string) =>
    invoke<{ success: boolean; email: string; expires_at: number }>('refresh_account_token', { id }),
  getUsage: (id: string) => invoke<AccountUsage>('get_account_usage', { id }),

  // Proxy
  startProxy: (port?: number) =>
    invoke<{ success: boolean; port: number; account_count: number; base_url: string }>('start_api_proxy', { port: port ?? 8520 }),
  stopProxy: () => invoke<{ success: boolean; message: string }>('stop_api_proxy'),
  reloadProxy: () => invoke<{ success: boolean; account_count: number }>('reload_proxy_accounts'),
  getProxyStatus: () => invoke<ProxyStatus>('get_proxy_status'),
  getProxyConfig: () => invoke<ProxyConfig>('get_proxy_config'),
  updateProxyConfig: (payload: {
    api_key?: string | null
    enable_logging?: boolean
    max_logs?: number
    disable_on_usage_limit?: boolean
    model_override?: string | null
    reasoning_effort_override?: string | null
    upstream_mode?: string | null
    custom_openai_base_url?: string | null
    custom_openai_api_key?: string | null
  }) =>
    invoke<ProxyConfig>('update_proxy_config', payload),
  generateProxyApiKey: () => invoke<string>('generate_proxy_api_key'),
  clearProxyLogs: () => invoke<{ success: boolean }>('clear_proxy_logs'),
  getProxyLogsCount: (payload?: { filter?: string; errors_only?: boolean }) =>
    invoke<number>('get_proxy_logs_count_filtered', payload ?? {}),
  getProxyLogs: (payload?: { filter?: string; errors_only?: boolean; limit?: number; offset?: number }) =>
    invoke<ProxyRequestLog[]>('get_proxy_logs_filtered', payload ?? {}),
  getProxyLogDetail: (logId: number) => invoke<ProxyLogDetail>('get_proxy_log_detail', { logId }),
  getProxyTokenStats: (hours?: number) =>
    invoke<ProxyTokenStats>('get_proxy_token_stats', { hours: hours ?? 24 }),
  listCodexModels: () => invoke<string[]>('list_codex_models'),

  // OpenAI Compat Proxy
  listOpenAICompatConfigs: () => invoke<OpenAICompatConfig[]>('list_openai_compat_configs'),
  createOpenAICompatConfig: (payload: {
    provider_name: string
    base_url: string
    api_key: string
    default_model?: string | null
    model_mappings?: OpenAICompatModelMapping[]
  }) => invoke<OpenAICompatConfig>('create_openai_compat_config', {
    providerName: payload.provider_name,
    baseUrl: payload.base_url,
    apiKey: payload.api_key,
    defaultModel: payload.default_model ?? null,
    modelMappings: payload.model_mappings ?? [],
  }),
  updateOpenAICompatConfig: (payload: {
    id: string
    provider_name: string
    base_url: string
    api_key: string
    default_model?: string | null
    model_mappings?: OpenAICompatModelMapping[]
  }) => invoke<OpenAICompatConfig>('update_openai_compat_config', {
    id: payload.id,
    providerName: payload.provider_name,
    baseUrl: payload.base_url,
    apiKey: payload.api_key,
    defaultModel: payload.default_model ?? null,
    modelMappings: payload.model_mappings ?? [],
  }),
  deleteOpenAICompatConfig: (id: string) => invoke<boolean>('delete_openai_compat_config', { id }),
  listOpenAICompatProviderModels: (configId: string) =>
    invoke<string[]>('list_openai_compat_provider_models', { configId }),
  startOpenAICompatProxy: (configId: string, port?: number) =>
    invoke<{ success: boolean; port: number; base_url: string; config_id: string; provider_name: string }>('start_openai_compat_proxy', {
      configId,
      port: port ?? 8521,
    }),
  stopOpenAICompatProxy: () => invoke<{ success: boolean }>('stop_openai_compat_proxy'),
  getOpenAICompatProxyStatus: () => invoke<OpenAICompatProxyStatus>('get_openai_compat_proxy_status'),

  // Anthropic Proxy
  listAnthropicKeys: () => invoke<AnthropicKeyEntry[]>('list_anthropic_keys'),
  addAnthropicKey: (label: string | undefined, key: string) =>
    invoke<AnthropicKeyEntry>('add_anthropic_key', { label: label ?? null, key }),
  deleteAnthropicKey: (id: string) => invoke<void>('delete_anthropic_key', { id }),
  updateAnthropicKeyLabel: (id: string, label: string | undefined) =>
    invoke<void>('update_anthropic_key_label', { id, label: label ?? null }),
  startAnthropicProxy: (port?: number) =>
    invoke<{ success: boolean; port: number }>('start_anthropic_proxy', { port: port ?? null }),
  stopAnthropicProxy: () => invoke<{ success: boolean }>('stop_anthropic_proxy'),
  getAnthropicProxyStatus: () =>
    invoke<{ running: boolean; port: number | null }>('get_anthropic_proxy_status'),
}
