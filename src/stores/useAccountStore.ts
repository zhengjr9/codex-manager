import { create } from 'zustand'
import type { CodexAccount } from '../types/account'
import { accountService, type AccountUsage, type ProxyStatus } from '../services/accountService'

interface AccountState {
  accounts: CodexAccount[]
  currentAccount: CodexAccount | null
  loading: boolean
  error: string | null
  proxyStatus: ProxyStatus
  usageMap: Record<string, AccountUsage>
  usageLoading: Record<string, boolean>

  fetchAccounts: () => Promise<void>
  fetchCurrent: () => Promise<void>
  switchAccount: (id: string) => Promise<void>
  deleteAccount: (id: string) => Promise<void>
  updateLabel: (id: string, label: string) => Promise<void>
  importCurrent: (label?: string) => Promise<void>
  refresh: () => Promise<void>

  oauthLogin: (label?: string) => Promise<void>
  refreshAccountToken: (id: string) => Promise<void>
  fetchUsage: (id: string) => Promise<void>
  fetchProxyStatus: () => Promise<void>
  startProxy: (port?: number) => Promise<void>
  stopProxy: () => Promise<void>
  reloadProxy: () => Promise<void>
}

export const useAccountStore = create<AccountState>((set, get) => ({
  accounts: [],
  currentAccount: null,
  loading: false,
  error: null,
  proxyStatus: { running: false, port: null, account_count: 0, active: 0, cooldown: 0, blocked: 0 },
  usageMap: {},
  usageLoading: {},

  fetchAccounts: async () => {
    set({ loading: true, error: null })
    try {
      const accounts = await accountService.list()
      set({ accounts, loading: false })
    } catch (e) {
      set({ error: String(e), loading: false })
    }
  },

  fetchCurrent: async () => {
    try {
      const currentAccount = await accountService.current()
      set({ currentAccount })
    } catch {
      set({ currentAccount: null })
    }
  },

  switchAccount: async (id) => {
    await accountService.switch(id)
    await get().fetchCurrent()
    await get().fetchProxyStatus()
  },

  deleteAccount: async (id) => {
    await accountService.delete(id)
    const { accounts, currentAccount, usageMap } = get()
    const newUsageMap = { ...usageMap }
    delete newUsageMap[id]
    set({ accounts: accounts.filter(a => a.id !== id), usageMap: newUsageMap })
    if (currentAccount?.id === id) set({ currentAccount: null })
    await get().fetchProxyStatus()
  },

  updateLabel: async (id, label) => {
    await accountService.updateLabel(id, label)
    set(state => ({
      accounts: state.accounts.map(a => a.id === id ? { ...a, label: label || undefined } : a),
    }))
  },

  importCurrent: async (label) => {
    await accountService.importCurrent(label)
    await get().fetchAccounts()
    await get().fetchCurrent()
    await get().fetchProxyStatus()
  },

  refresh: async () => {
    await Promise.all([get().fetchAccounts(), get().fetchCurrent(), get().fetchProxyStatus()])
  },

  oauthLogin: async (label) => {
    await accountService.oauthLogin(label)
    await get().fetchAccounts()
    await get().fetchCurrent()
    await get().fetchProxyStatus()
  },

  refreshAccountToken: async (id) => {
    await accountService.refreshToken(id)
    await get().fetchAccounts()
    await get().fetchCurrent()
  },

  fetchUsage: async (id) => {
    set(state => ({ usageLoading: { ...state.usageLoading, [id]: true } }))
    try {
      const usage = await accountService.getUsage(id)
      set(state => ({
        usageMap: { ...state.usageMap, [id]: usage },
        usageLoading: { ...state.usageLoading, [id]: false },
      }))
    } catch {
      set(state => ({ usageLoading: { ...state.usageLoading, [id]: false } }))
    }
  },

  fetchProxyStatus: async () => {
    try {
      const proxyStatus = await accountService.getProxyStatus()
      set({ proxyStatus })
    } catch (e) {
      console.error(e)
    }
  },

  startProxy: async (port) => {
    await accountService.startProxy(port)
    await get().fetchProxyStatus()
  },

  stopProxy: async () => {
    await accountService.stopProxy()
    await get().fetchProxyStatus()
  },

  reloadProxy: async () => {
    await accountService.reloadProxy()
    await get().fetchProxyStatus()
  }
}))
