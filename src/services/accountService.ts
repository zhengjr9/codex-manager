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

export interface ProxyConfig {
  api_key: string | null
  enable_logging: boolean
  max_logs: number
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
  request_headers: string | null
  response_headers: string | null
  request_body: string | null
  response_body: string | null
  input_tokens: number | null
  output_tokens: number | null
}

export const accountService = {
  list: () => invoke<CodexAccount[]>('list_accounts'),
  current: () => invoke<CodexAccount | null>('get_current_account'),
  switch: (id: string) => invoke<boolean>('switch_account', { id }),
  delete: (id: string) => invoke<boolean>('delete_account', { id }),
  updateLabel: (id: string, label: string) => invoke<boolean>('update_label', { id, label }),
  importCurrent: (label?: string) =>
    invoke<{ success: boolean; id: string; email: string }>('import_current', { label: label ?? null }),
  login: () => invoke<{ success: boolean; message: string }>('launch_codex_login'),
  getConfig: () => invoke<{ raw: string }>('get_config'),

  oauthLogin: (label?: string) =>
    invoke<{ success: boolean; email: string; plan: string; id: string }>('oauth_login', { label: label ?? null }),
  refreshToken: (id: string) =>
    invoke<{ success: boolean; email: string; expires_at: number }>('refresh_account_token', { id }),
  getUsage: (id: string) => invoke<AccountUsage>('get_account_usage', { id }),

  // Proxy
  startProxy: (port?: number) =>
    invoke<{ success: boolean; port: number; account_count: number; base_url: string }>('start_api_proxy', { port: port ?? 8080 }),
  stopProxy: () => invoke<{ success: boolean; message: string }>('stop_api_proxy'),
  reloadProxy: () => invoke<{ success: boolean; account_count: number }>('reload_proxy_accounts'),
  getProxyStatus: () => invoke<ProxyStatus>('get_proxy_status'),
  getProxyConfig: () => invoke<ProxyConfig>('get_proxy_config'),
  updateProxyConfig: (payload: { api_key?: string | null; enable_logging?: boolean; max_logs?: number }) =>
    invoke<ProxyConfig>('update_proxy_config', payload),
  generateProxyApiKey: () => invoke<string>('generate_proxy_api_key'),
  clearProxyLogs: () => invoke<{ success: boolean }>('clear_proxy_logs'),
  getProxyLogsCount: (payload?: { filter?: string; errors_only?: boolean }) =>
    invoke<number>('get_proxy_logs_count_filtered', payload ?? {}),
  getProxyLogs: (payload?: { filter?: string; errors_only?: boolean; limit?: number; offset?: number }) =>
    invoke<ProxyRequestLog[]>('get_proxy_logs_filtered', payload ?? {}),
  getProxyLogDetail: (logId: number) => invoke<ProxyLogDetail>('get_proxy_log_detail', { log_id: logId }),
}
