# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`codex-manager` is a full-stack TypeScript web application for managing multiple OpenAI Codex CLI accounts locally. It lets users store, switch between, and label multiple Codex/ChatGPT accounts. The UI is in Chinese (Simplified).

## Commands

```bash
npm run dev        # Start both server and client concurrently (primary dev command)
npm run client     # Vite dev server only (port 5174)
npm run server     # Express API server only via tsx watch (port 3741)
npm run build      # tsc -b type check + vite build
npm run preview    # Preview production build
```

No lint or test scripts are configured.

## Architecture

**Frontend SPA (React/Vite on :5174) + local REST API (Express on :3741)**

Vite proxies all `/api/*` requests to the Express server. The server reads/writes directly to the filesystem under `~/.codex/`:

- `~/.codex/auth.json` — active account credentials
- `~/.codex/accounts/<id>/auth.json` — per-account stored credentials
- `~/.codex/accounts_meta.json` — user labels and timestamps
- `~/.codex/config.toml` — Codex CLI config (read-only)

**Account switching** is a file copy: the chosen account's `auth.json` is copied to `~/.codex/auth.json`, which the Codex CLI reads at runtime.

**JWT decoding** is done server-side without any external library — `decodeJwt()` base64-decodes the JWT payload to extract `email`, `plan`, `user_id`, and expiry from stored tokens.

**Login flow** spawns the external `codex` CLI binary (`spawn('codex', ['login'], ...)`), so `codex` must be installed and on `$PATH`.

## Data Flow

```
accountService.ts  →  fetch('/api/*')  →  Express server  →  ~/.codex/ filesystem
useAccountStore.ts (Zustand)  →  accountService  →  AccountsPage.tsx  →  components
```

- `src/services/accountService.ts` — typed HTTP client for all API calls
- `src/stores/useAccountStore.ts` — Zustand store; holds `accounts[]`, `currentAccount`, `loading`, `error`; exposes async actions
- `src/pages/AccountsPage.tsx` — main page consuming the store; renders stats cards + Ant Design table with per-row actions
- `src/components/accounts/` — `AddAccountDialog` (two-step: codex login or import current), `AccountLabelEditor` (inline modal edit), `PlanBadge` (plan tier display)

## Server API Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/accounts` | List all managed accounts |
| GET | `/api/accounts/current` | Get active account |
| POST | `/api/accounts/switch` | Switch active account (file copy) |
| DELETE | `/api/accounts/:id` | Remove account |
| PATCH | `/api/accounts/:id/label` | Update label in metadata |
| POST | `/api/accounts/import-current` | Import current `auth.json` as new account |
| POST | `/api/login` | Spawn `codex login` subprocess |
| GET | `/api/config` | Read `~/.codex/config.toml` |

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

TypeScript (strict) · React 19 · Express 4 · Ant Design 5 · Zustand 5 · React Router 7 · Tailwind CSS 3 · Vite 7 · `tsx` for server watch mode
