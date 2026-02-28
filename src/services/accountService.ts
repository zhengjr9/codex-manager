import type { CodexAccount } from '../types/account'

const BASE = '/api'

async function req<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, options)
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }))
    throw new Error(err.error || res.statusText)
  }
  return res.json()
}

export const accountService = {
  list: () => req<CodexAccount[]>('/accounts'),
  current: () => req<CodexAccount | null>('/accounts/current'),
  switch: (id: string) => req<{ success: boolean }>('/accounts/switch', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ id }),
  }),
  delete: (id: string) => req<{ success: boolean }>(`/accounts/${id}`, { method: 'DELETE' }),
  updateLabel: (id: string, label: string) => req<{ success: boolean }>(`/accounts/${id}/label`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ label }),
  }),
  importCurrent: (label?: string) => req<{ success: boolean; id: string; email: string }>('/accounts/import-current', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ label }),
  }),
  login: () => req<{ success: boolean; message: string }>('/login', { method: 'POST' }),
  getConfig: () => req<{ raw: string }>('/config'),
}
