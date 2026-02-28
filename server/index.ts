import express from 'express'
import cors from 'cors'
import fs from 'fs'
import path from 'path'
import os from 'os'
import { execSync, spawn } from 'child_process'

const app = express()
const PORT = 3741
const CODEX_DIR = path.join(os.homedir(), '.codex')
const ACCOUNTS_DIR = path.join(CODEX_DIR, 'accounts')
const AUTH_FILE = path.join(CODEX_DIR, 'auth.json')
const META_FILE = path.join(CODEX_DIR, 'accounts_meta.json')

app.use(cors())
app.use(express.json())

// Ensure accounts directory exists
if (!fs.existsSync(ACCOUNTS_DIR)) {
  fs.mkdirSync(ACCOUNTS_DIR, { recursive: true })
}

function decodeJwt(token: string): Record<string, unknown> {
  try {
    const payload = token.split('.')[1]
    return JSON.parse(Buffer.from(payload, 'base64url').toString('utf-8'))
  } catch {
    return {}
  }
}

function readMeta(): Record<string, { label?: string; added_at: number }> {
  if (!fs.existsSync(META_FILE)) return {}
  try {
    return JSON.parse(fs.readFileSync(META_FILE, 'utf-8'))
  } catch {
    return {}
  }
}

function writeMeta(meta: Record<string, { label?: string; added_at: number }>) {
  fs.writeFileSync(META_FILE, JSON.stringify(meta, null, 2))
}

function parseAuthFile(authData: Record<string, unknown>, accountId: string) {
  const tokens = authData.tokens as Record<string, unknown> | undefined
  const idToken = (tokens?.id_token || authData.id_token || '') as string
  const accessToken = (tokens?.access_token || authData.access_token || '') as string
  const refreshToken = (tokens?.refresh_token || authData.refresh_token || '') as string
  const storedAccountId = (tokens?.account_id || authData.account_id || accountId) as string

  const idPayload = decodeJwt(idToken)
  const atPayload = decodeJwt(accessToken)

  const openaiClaims = (idPayload['https://api.openai.com/auth'] || atPayload['https://api.openai.com/auth'] || {}) as Record<string, unknown>
  const profileClaims = (atPayload['https://api.openai.com/profile'] || {}) as Record<string, unknown>

  const email = (idPayload.email || profileClaims.email || '') as string
  const planType = (openaiClaims.chatgpt_plan_type || 'free') as string
  const userId = (openaiClaims.chatgpt_user_id || idPayload.sub || '') as string
  const exp = (atPayload.exp || idPayload.exp || 0) as number
  const lastRefresh = authData.last_refresh as string | undefined

  return {
    id: storedAccountId,
    email,
    plan: planType,
    user_id: userId,
    expires_at: exp * 1000,
    last_refresh: lastRefresh || null,
    has_refresh_token: refreshToken.length > 0,
    openai_api_key: (authData.OPENAI_API_KEY || null) as string | null,
  }
}

// GET /api/accounts - list all managed accounts
app.get('/api/accounts', (_req, res) => {
  const meta = readMeta()
  const accounts = []

  // Read from accounts/ subdirectories
  if (fs.existsSync(ACCOUNTS_DIR)) {
    const dirs = fs.readdirSync(ACCOUNTS_DIR)
    for (const dir of dirs) {
      const authPath = path.join(ACCOUNTS_DIR, dir, 'auth.json')
      if (!fs.existsSync(authPath)) continue
      try {
        const authData = JSON.parse(fs.readFileSync(authPath, 'utf-8'))
        const parsed = parseAuthFile(authData, dir)
        accounts.push({
          ...parsed,
          id: dir,
          label: meta[dir]?.label,
          added_at: meta[dir]?.added_at || 0,
        })
      } catch {
        // skip malformed
      }
    }
  }

  res.json(accounts)
})

// GET /api/accounts/current - get currently active account
app.get('/api/accounts/current', (_req, res) => {
  if (!fs.existsSync(AUTH_FILE)) {
    return res.json(null)
  }
  try {
    const authData = JSON.parse(fs.readFileSync(AUTH_FILE, 'utf-8'))
    const parsed = parseAuthFile(authData, 'current')

    // Find which managed account this matches
    const meta = readMeta()
    if (fs.existsSync(ACCOUNTS_DIR)) {
      const dirs = fs.readdirSync(ACCOUNTS_DIR)
      for (const dir of dirs) {
        const authPath = path.join(ACCOUNTS_DIR, dir, 'auth.json')
        if (!fs.existsSync(authPath)) continue
        try {
          const candidate = JSON.parse(fs.readFileSync(authPath, 'utf-8'))
          const tokens = candidate.tokens || {}
          const currentTokens = authData.tokens || {}
          if (tokens.account_id === currentTokens.account_id || tokens.refresh_token === currentTokens.refresh_token) {
            return res.json({ ...parsed, id: dir, label: meta[dir]?.label, added_at: meta[dir]?.added_at || 0 })
          }
        } catch {}
      }
    }

    return res.json({ ...parsed })
  } catch {
    return res.json(null)
  }
})

// POST /api/accounts/switch - switch to an account
app.post('/api/accounts/switch', (req, res) => {
  const { id } = req.body as { id: string }
  const authPath = path.join(ACCOUNTS_DIR, id, 'auth.json')
  if (!fs.existsSync(authPath)) {
    return res.status(404).json({ error: 'Account not found' })
  }
  fs.copyFileSync(authPath, AUTH_FILE)
  res.json({ success: true })
})

// DELETE /api/accounts/:id - delete an account
app.delete('/api/accounts/:id', (req, res) => {
  const { id } = req.params
  const accountDir = path.join(ACCOUNTS_DIR, id)
  if (!fs.existsSync(accountDir)) {
    return res.status(404).json({ error: 'Account not found' })
  }
  fs.rmSync(accountDir, { recursive: true })
  const meta = readMeta()
  delete meta[id]
  writeMeta(meta)
  res.json({ success: true })
})

// PATCH /api/accounts/:id/label - update label
app.patch('/api/accounts/:id/label', (req, res) => {
  const { id } = req.params
  const { label } = req.body as { label: string }
  const meta = readMeta()
  if (!meta[id]) meta[id] = { added_at: Date.now() }
  meta[id].label = label || undefined
  writeMeta(meta)
  res.json({ success: true })
})

// POST /api/accounts/import-current - import current ~/.codex/auth.json as new account
app.post('/api/accounts/import-current', (req, res) => {
  const { label } = req.body as { label?: string }
  if (!fs.existsSync(AUTH_FILE)) {
    return res.status(400).json({ error: 'No auth.json found. Run `codex login` first.' })
  }
  try {
    const authData = JSON.parse(fs.readFileSync(AUTH_FILE, 'utf-8'))
    const parsed = parseAuthFile(authData, 'tmp')
    const tokens = authData.tokens || {}
    const accountId = tokens.account_id || parsed.user_id || `acc_${Date.now()}`
    const safeId = accountId.replace(/[^a-zA-Z0-9_-]/g, '_')

    const destDir = path.join(ACCOUNTS_DIR, safeId)
    fs.mkdirSync(destDir, { recursive: true })
    fs.copyFileSync(AUTH_FILE, path.join(destDir, 'auth.json'))

    const meta = readMeta()
    meta[safeId] = { added_at: Date.now(), label: label || undefined }
    writeMeta(meta)

    res.json({ success: true, id: safeId, email: parsed.email })
  } catch (e) {
    res.status(500).json({ error: String(e) })
  }
})

// POST /api/login - trigger codex login and wait, then import
app.post('/api/login', (_req, res) => {
  // Start codex login in a subprocess - it will update ~/.codex/auth.json
  // We spawn it and let the client poll for completion
  try {
    const proc = spawn('codex', ['login'], {
      detached: true,
      stdio: 'ignore',
    })
    proc.unref()
    res.json({ success: true, message: 'codex login started. Complete login in your terminal, then click "Import Current Account".' })
  } catch (e) {
    res.status(500).json({ error: String(e) })
  }
})

// GET /api/config - read codex config
app.get('/api/config', (_req, res) => {
  const configPath = path.join(CODEX_DIR, 'config.toml')
  if (!fs.existsSync(configPath)) return res.json({ raw: '' })
  res.json({ raw: fs.readFileSync(configPath, 'utf-8') })
})

app.listen(PORT, () => {
  console.log(`Codex Manager server running on http://localhost:${PORT}`)
})
