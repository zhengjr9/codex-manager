# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`codex-manager` is a native macOS desktop app for managing multiple OpenAI Codex CLI accounts locally. It lets users store, switch between, and label multiple Codex/ChatGPT accounts. The UI is in Chinese (Simplified).

Built with **Tauri v2** — React frontend + Rust backend. No Express server; all filesystem I/O is handled by Tauri commands in `src-tauri/src/lib.rs`.

## Commands

```bash
npm run client        # Vite dev server only (port 5174)
npm run build         # tsc -b type check + vite build
npm run preview       # Preview production build
npm run tauri:dev     # Start Tauri desktop app in dev mode (runs Vite + Rust)
npm run tauri:build   # Build production .app + .dmg
```

No lint or test scripts are configured.

## Architecture

**Tauri v2: React/Vite frontend + Rust backend (no network server)**

The frontend calls Tauri commands via `invoke()` from `@tauri-apps/api/core`. The Rust backend reads/writes directly to the filesystem under `~/.codex/`:

- `~/.codex/auth.json` — active account credentials
- `~/.codex/accounts/<id>/auth.json` — per-account stored credentials
- `~/.codex/accounts_meta.json` — user labels and timestamps
- `~/.codex/config.toml` — Codex CLI config (read-only)

**Account switching** is a file copy: the chosen account's `auth.json` is copied to `~/.codex/auth.json`.

**JWT decoding** is done in Rust using `base64` crate — base64url-decodes the JWT payload to extract `email`, `plan`, `user_id`, and expiry from stored tokens.

**Login flow** spawns the external `codex` CLI binary via `std::process::Command`, so `codex` must be installed and on `$PATH`.

## Data Flow

```
accountService.ts  →  invoke('command_name')  →  src-tauri/src/lib.rs  →  ~/.codex/ filesystem
useAccountStore.ts (Zustand)  →  accountService  →  AccountsPage.tsx  →  components
```

- `src/services/accountService.ts` — typed `invoke()` wrappers for all Tauri commands
- `src/stores/useAccountStore.ts` — Zustand store; holds `accounts[]`, `currentAccount`, `loading`, `error`; exposes async actions
- `src/pages/AccountsPage.tsx` — main page consuming the store; renders stats cards + Ant Design table with per-row actions
- `src/components/accounts/` — `AddAccountDialog`, `AccountLabelEditor`, `PlanBadge`

## Tauri Commands (src-tauri/src/lib.rs)

| Command | Purpose |
|---------|---------|
| `list_accounts` | List all managed accounts from `~/.codex/accounts/` |
| `get_current_account` | Read and parse `~/.codex/auth.json` |
| `switch_account(id)` | Copy `accounts/{id}/auth.json` → `auth.json` |
| `delete_account(id)` | Remove account dir + update meta |
| `update_label(id, label)` | Update `accounts_meta.json` |
| `import_current(label)` | Copy `auth.json` → `accounts/{id}/auth.json` |
| `launch_codex_login` | Spawn `codex login` subprocess |
| `get_config` | Read `~/.codex/config.toml` |

## Key Type

```typescript
// src/types/account.ts
interface CodexAccount {
  id: string
  email: string
  plan: 'free' | 'plus' | 'pro' | 'ultra' | string
  user_id: string
  expires_at: number        // ms timestamp from JWT exp
  last_refresh: string | null
  has_refresh_token: boolean
  openai_api_key: string | null
  label?: string            // user-defined nickname
  added_at: number          // ms timestamp when imported
}
```

## Stack

TypeScript (strict) · React 19 · Tauri 2 · Rust · Ant Design 5 · Zustand 5 · React Router 7 (hash mode) · Tailwind CSS 3 · Vite 7

## GitHub Actions

`.github/workflows/release.yml` — triggers on `v*` tags, builds on `macos-14` (Apple Silicon), produces `aarch64-apple-darwin` .dmg, uploads to GitHub Release.

```bash
git tag v1.0.0 && git push origin v1.0.0  # trigger a release build
```
