import { invoke } from '@tauri-apps/api/core'
import type { CodexAccount } from '../types/account'

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

  // New features
  oauthLogin: (label?: string) =>
    invoke<{ success: boolean; email: string; plan: string; id: string }>('oauth_login', { label: label ?? null }),
  refreshToken: (id: string) =>
    invoke<{ success: boolean; email: string; expires_at: number }>('refresh_account_token', { id }),

  // Proxy server
  startProxy: (port?: number) => invoke<{ success: boolean; port: number; active_email: string; base_url: string }>('start_api_proxy', { port: port ?? 8080 }),
  stopProxy: () => invoke<{ success: boolean; message: string }>('stop_api_proxy'),
  getProxyStatus: () => invoke<{ running: boolean; port: number | null; active_email: string | null }>('get_proxy_status'),
}
