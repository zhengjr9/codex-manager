export interface CodexAccount {
  id: string
  email: string
  plan: 'free' | 'plus' | 'pro' | 'ultra' | string
  user_id: string
  expires_at: number
  last_refresh: string | null
  has_refresh_token: boolean
  openai_api_key: string | null
  label?: string
  added_at: number
}
