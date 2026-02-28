import type { CodexAccount } from '../types/account'
import { invoke } from '@tauri-apps/api/core'

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
}
