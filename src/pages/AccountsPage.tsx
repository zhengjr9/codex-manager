import { useEffect, useState } from 'react'
import { listen } from '@tauri-apps/api/event'
import {
  Table, Button, Popconfirm, Tag, Tooltip, message, Spin, Alert,
  Space, Typography, Card, Statistic, Row, Col, Progress, Switch, InputNumber, Input, Select
} from 'antd'
import {
  PlusOutlined, ReloadOutlined, CheckCircleFilled,
  DeleteOutlined, SwapOutlined, UserOutlined, ClockCircleOutlined,
  WarningOutlined, ApiOutlined, SyncOutlined
} from '@ant-design/icons'
import { useAccountStore } from '../stores/useAccountStore'
import type { CodexAccount } from '../types/account'
import {
  accountService,
  type AccountUsage,
  type ProxyConfig,
  type ProxyRequestLog,
  type ProxyLogDetail,
  type ProxyTokenStats,
} from '../services/accountService'
import PlanBadge from '../components/accounts/PlanBadge'
import AccountLabelEditor from '../components/accounts/AccountLabelEditor'
import AddAccountDialog from '../components/accounts/AddAccountDialog'
import AnthropicProxyCard from '../components/accounts/AnthropicProxyCard'

const { Text } = Typography

function formatExpiry(ts: number): { text: string; expired: boolean } {
  if (!ts) return { text: '未知', expired: false }
  const diff = ts - Date.now()
  const expired = diff < 0
  if (expired) return { text: '已过期', expired: true }
  const days = Math.floor(diff / 86400000)
  if (days > 0) return { text: `${days} 天后过期`, expired: false }
  const hours = Math.floor(diff / 3600000)
  return { text: `${hours} 小时后过期`, expired: false }
}

function formatWindowLabel(minutes: number | null, fallback: string): string {
  if (minutes == null) return fallback
  if (minutes < 60) return `${minutes}分钟`
  const h = Math.round(minutes / 60)
  if (h < 24) return `${h}小时`
  return `${Math.round(h / 24)}天`
}

function formatResetAt(resetAt: number | null): string {
  if (!resetAt) return ''
  const resetMs = resetAt * 1000
  if (resetMs <= Date.now()) return '即将刷新'

  // 转换为北京时间 (UTC+8)
  const bjOffset = 8 * 3600 * 1000
  const nowBjDay = Math.floor((Date.now() + bjOffset) / 86400000)
  const resetBjDay = Math.floor((resetMs + bjOffset) / 86400000)

  const bjHours = Math.floor((resetMs + bjOffset) / 3600000) % 24
  const bjMinutes = Math.floor((resetMs + bjOffset) / 60000) % 60
  const timeStr = `${String(bjHours).padStart(2, '0')}:${String(bjMinutes).padStart(2, '0')}`

  if (resetBjDay === nowBjDay) return `${timeStr} 刷新`
  if (resetBjDay === nowBjDay + 1) return `明天 ${timeStr} 刷新`
  return `${resetBjDay - nowBjDay}天后 ${timeStr} 刷新`
}

function formatCapturedAt(ts: number): string {
  const diff = Date.now() - ts * 1000
  const mins = Math.floor(diff / 60000)
  if (mins < 1) return '刚刚'
  if (mins < 60) return `${mins}分钟前`
  return `${Math.floor(mins / 60)}小时前`
}

function prettyJson(raw?: string | null): string {
  if (!raw) return '--'
  const text = raw.trim()
  if (!text) return '--'
  try {
    const parsed = JSON.parse(text) as unknown
    if (Array.isArray(parsed) && parsed.every((item) => Array.isArray(item) && item.length === 2)) {
      const headersObj: Record<string, string> = {}
      parsed.forEach((item) => {
        const [k, v] = item as [unknown, unknown]
        if (typeof k === 'string') headersObj[k] = String(v ?? '')
      })
      return JSON.stringify(headersObj, null, 2)
    }
    return JSON.stringify(parsed, null, 2)
  } catch {
    return raw
  }
}

function escapeHtml(text: string): string {
  return text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
}

function highlightJson(jsonText: string): string {
  const escaped = escapeHtml(jsonText)
  return escaped.replace(
    /("(?:\\u[\da-fA-F]{4}|\\[^u]|[^\\"])*")(\s*:)?|\b(true|false)\b|\bnull\b|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?/g,
    (match, quoted, isKey, boolValue) => {
      if (quoted) {
        if (isKey) {
          return `<span style="color:#1d4ed8;font-weight:600;">${quoted}</span>${isKey}`
        }
        return `<span style="color:#15803d;">${quoted}</span>`
      }
      if (boolValue) {
        return `<span style="color:#b45309;font-weight:600;">${match}</span>`
      }
      if (match === 'null') {
        return `<span style="color:#6b7280;font-style:italic;">null</span>`
      }
      return `<span style="color:#7c3aed;">${match}</span>`
    },
  )
}

function JsonViewer({ value, small }: { value?: string | null; small?: boolean }) {
  const content = prettyJson(value)
  const highlighted = highlightJson(content)
  return (
    <pre
      className={`text-xs bg-gray-50 p-2 rounded border border-gray-200 overflow-auto whitespace-pre-wrap break-all font-mono ${small ? 'max-h-24' : 'max-h-48'}`}
      dangerouslySetInnerHTML={{ __html: highlighted }}
    />
  )
}

function UsageCell({ usage, loading }: { usage: AccountUsage | undefined; loading: boolean }) {
  if (loading) return <Spin size="small" />
  if (!usage) return <Text type="secondary" style={{ fontSize: 12, color: '#d9d9d9' }}>--</Text>

  const {
    used_percent, window_minutes, resets_at,
    secondary_used_percent, secondary_window_minutes, secondary_resets_at,
    availability, captured_at,
  } = usage

  const hasPrimary = used_percent != null
  const hasSecondary = secondary_used_percent != null || secondary_window_minutes != null

  const primaryRemain = used_percent != null ? Math.max(0, 100 - used_percent) : null
  const secondaryRemain = secondary_used_percent != null ? Math.max(0, 100 - secondary_used_percent) : null

  const availColor =
    availability === 'available' ? '#52c41a' :
    availability === 'unavailable' ? '#ff4d4f' : '#faad14'

  const availText =
    availability === 'available' ? '可用' :
    availability === 'unavailable' ? '已耗尽' :
    availability === 'primary_window_available_only' ? '部分可用' : '未知'

  function ProgressRow({
    label, remain, resets, windowMin, secondary,
  }: {
    label: string
    remain: number | null
    resets: number | null
    windowMin: number | null
    secondary?: boolean
  }) {
    const pct = remain ?? 0
    const strokeColor = pct <= 0 ? '#ff4d4f' : pct <= 20 ? '#faad14' : secondary ? '#6366f1' : '#52c41a'
    const resetText = formatResetAt(resets)
    const winLabel = formatWindowLabel(windowMin, secondary ? '7天' : '5小时')
    return (
      <Tooltip title={`${winLabel}窗口${resetText ? ' · ' + resetText : ''}`}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
          <Text style={{ fontSize: 11, color: '#8c8c8c', width: 30, flexShrink: 0 }}>{label}</Text>
          <Progress
            percent={Math.round(pct)}
            size="small"
            strokeColor={strokeColor}
            trailColor="#f0f0f0"
            style={{ flex: 1, margin: 0, minWidth: 60 }}
            showInfo={false}
          />
          <Text style={{ fontSize: 11, width: 34, textAlign: 'right', color: strokeColor, flexShrink: 0 }}>
            {remain != null ? `${Math.round(remain)}%` : '--'}
          </Text>
        </div>
      </Tooltip>
    )
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 3, minWidth: 170 }}>
      {hasPrimary && (
        <ProgressRow
          label={formatWindowLabel(window_minutes, '5h')}
          remain={primaryRemain}
          resets={resets_at}
          windowMin={window_minutes}
        />
      )}
      {hasSecondary && (
        <ProgressRow
          label={formatWindowLabel(secondary_window_minutes, '7天')}
          remain={secondaryRemain}
          resets={secondary_resets_at}
          windowMin={secondary_window_minutes}
          secondary
        />
      )}
      <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginTop: 2 }}>
        <Tag
          style={{
            fontSize: 10, padding: '0 5px', lineHeight: '16px',
            border: 'none', background: availColor + '18', color: availColor, margin: 0,
          }}
        >
          {availText}
        </Tag>
        {resets_at && (
          <Text style={{ fontSize: 10, color: '#bfbfbf' }}>{formatResetAt(resets_at)}</Text>
        )}
      </div>
      <Text style={{ fontSize: 10, color: '#d9d9d9' }}>
        更新于 {formatCapturedAt(captured_at)}
      </Text>
    </div>
  )
}

export default function AccountsPage() {
  const {
    accounts, currentAccount, loading, error, proxyStatus,
    usageMap, usageLoading,
    refresh, switchAccount, deleteAccount, refreshAccountToken,
    fetchUsage, startProxy, stopProxy, reloadProxy, updateProxyEnabled
  } = useAccountStore()

  useEffect(() => {
    let unlisten: (() => void) | null = null
    listen('accounts_updated', async () => {
      await refresh()
    }).then((dispose) => {
      unlisten = dispose
    })
    return () => {
      if (unlisten) unlisten()
    }
  }, [refresh])

  const [addOpen, setAddOpen] = useState(false)
  const [switching, setSwitching] = useState<string | null>(null)
  const [deleting, setDeleting] = useState<string | null>(null)
  const [refreshingToken, setRefreshingToken] = useState<string | null>(null)
  const [proxyPortInput, setProxyPortInput] = useState<number>(8520)
  const [proxyLoading, setProxyLoading] = useState(false)
  const [proxyConfig, setProxyConfig] = useState<ProxyConfig | null>(null)
  const [apiKeyInput, setApiKeyInput] = useState('')
  const [configSaving, setConfigSaving] = useState(false)
  const [codexModels, setCodexModels] = useState<string[]>([])
  const [codexModelsLoading, setCodexModelsLoading] = useState(false)
  const [logs, setLogs] = useState<ProxyRequestLog[]>([])
  const [logsLoading, setLogsLoading] = useState(false)
  const [logsFilter, setLogsFilter] = useState('')
  const [logsErrorsOnly, setLogsErrorsOnly] = useState(false)
  const [logsTotal, setLogsTotal] = useState(0)
  const [logsPage, setLogsPage] = useState(1)
  const [logsPageSize, setLogsPageSize] = useState(20)
  const [logDetailOpen, setLogDetailOpen] = useState(false)
  const [logDetail, setLogDetail] = useState<ProxyLogDetail | null>(null)
  const [logDetailLoading, setLogDetailLoading] = useState(false)
  const [logsCollapsed, setLogsCollapsed] = useState(true)
  const [expandedRowData, setExpandedRowData] = useState<Record<number, ProxyLogDetail>>({})
  const [expandedRowLoading, setExpandedRowLoading] = useState<Set<number>>(new Set())
  const [expandedRowError, setExpandedRowError] = useState<Record<number, string>>({})
  const [tokenStats, setTokenStats] = useState<ProxyTokenStats | null>(null)
  const [tokenStatsLoading, setTokenStatsLoading] = useState(false)
  const [tokenStatsHours, setTokenStatsHours] = useState(24)

  // 初始加载账号
  useEffect(() => {
    refresh()
  }, [])

  // 账号列表就绪后，自动并发拉取所有未加载的额度
  useEffect(() => {
    if (accounts.length === 0) return
    accounts.forEach(a => {
      if (!usageMap[a.id] && !usageLoading[a.id]) {
        fetchUsage(a.id)
      }
    })
  }, [accounts.map(a => a.id).join(',')])

  useEffect(() => {
    let active = true
    accountService.getProxyConfig()
      .then(cfg => {
        if (!active) return
        const normalized = {
          ...cfg,
          upstream_mode: 'codex',
          custom_openai_base_url: null,
          custom_openai_api_key: null,
        }
        setProxyConfig(normalized)
        setApiKeyInput(cfg.api_key ?? '')
      })
      .catch(() => {})
    return () => { active = false }
  }, [])

  async function refreshCodexModels(showError = true) {
    setCodexModelsLoading(true)
    try {
      const models = await accountService.listCodexModels()
      setCodexModels(models)
    } catch (e) {
      setCodexModels([])
      if (showError) {
        message.error(String(e))
      }
    } finally {
      setCodexModelsLoading(false)
    }
  }

  useEffect(() => {
    if (!proxyConfig) return
    refreshCodexModels(false)
  }, [proxyConfig?.model_override])

  async function refreshProxyLogs(page = logsPage, pageSize = logsPageSize) {
    setLogsLoading(true)
    try {
      const [count, items] = await Promise.all([
        accountService.getProxyLogsCount({ filter: logsFilter, errors_only: logsErrorsOnly }),
        accountService.getProxyLogs({
          filter: logsFilter,
          errors_only: logsErrorsOnly,
          limit: pageSize,
          offset: (page - 1) * pageSize,
        }),
      ])
      setLogsTotal(count)
      setLogs(items)
    } catch {
      setLogs([])
    } finally {
      setLogsLoading(false)
    }
  }

  useEffect(() => {
    refreshProxyLogs()
  }, [logsFilter, logsErrorsOnly, logsPage, logsPageSize])

  async function refreshProxyTokenStats(hours = tokenStatsHours) {
    setTokenStatsLoading(true)
    try {
      const stats = await accountService.getProxyTokenStats(hours)
      setTokenStats(stats)
    } catch {
      setTokenStats(null)
    } finally {
      setTokenStatsLoading(false)
    }
  }

  useEffect(() => {
    refreshProxyTokenStats(tokenStatsHours)
  }, [tokenStatsHours])

  async function handleSwitch(id: string) {
    setSwitching(id)
    try {
      await switchAccount(id)
      message.success('已切换账号')
    } catch (e) {
      message.error(String(e))
    } finally {
      setSwitching(null)
    }
  }

  async function handleDelete(id: string) {
    setDeleting(id)
    try {
      await deleteAccount(id)
      message.success('账号已删除')
    } catch (e) {
      message.error(String(e))
    } finally {
      setDeleting(null)
    }
  }

  async function handleTokenRefresh(id: string) {
    setRefreshingToken(id)
    try {
      await refreshAccountToken(id)
      await fetchUsage(id)
      message.success('Token 刷新成功')
    } catch (e) {
      message.error('Token 刷新失败: ' + String(e))
    } finally {
      setRefreshingToken(null)
    }
  }

  async function handleSaveProxyConfig() {
    if (!proxyConfig) return
    setConfigSaving(true)
    try {
      const cfg = await accountService.updateProxyConfig({
        api_key: apiKeyInput,
        enable_logging: proxyConfig.enable_logging,
        max_logs: proxyConfig.max_logs,
        disable_on_usage_limit: proxyConfig.disable_on_usage_limit,
        model_override: proxyConfig.model_override,
        reasoning_effort_override: proxyConfig.reasoning_effort_override,
        upstream_mode: 'codex',
        codex_proxy_url: proxyConfig.codex_proxy_url,
        custom_openai_base_url: null,
        custom_openai_api_key: null,
      })
      setProxyConfig(cfg)
      setApiKeyInput(cfg.api_key ?? '')
      await refreshCodexModels()
      message.success('代理配置已保存')
    } catch (e) {
      message.error(String(e))
    } finally {
      setConfigSaving(false)
    }
  }

  async function handleGenerateApiKey() {
    try {
      const key = await accountService.generateProxyApiKey()
      setApiKeyInput(key)
    } catch (e) {
      message.error(String(e))
    }
  }

  async function handleClearLogs() {
    try {
      await accountService.clearProxyLogs()
      setLogs([])
      setLogsTotal(0)
      await refreshProxyTokenStats()
      message.success('日志已清空')
    } catch (e) {
      message.error(String(e))
    }
  }

  async function openLogDetail(id: number) {
    setLogDetailOpen(true)
    setLogDetailLoading(true)
    try {
      const detail = await accountService.getProxyLogDetail(id)
      setLogDetail(detail)
    } catch (e) {
      message.error(String(e))
    } finally {
      setLogDetailLoading(false)
    }
  }

  async function loadExpandedRow(id: number) {
    if (expandedRowData[id] && !expandedRowError[id]) return
    setExpandedRowLoading(prev => new Set(prev).add(id))
    setExpandedRowError(prev => {
      const next = { ...prev }
      delete next[id]
      return next
    })
    try {
      const detail = await accountService.getProxyLogDetail(id)
      setExpandedRowData(prev => ({ ...prev, [id]: detail }))
    } catch (e) {
      setExpandedRowError(prev => ({ ...prev, [id]: String(e) }))
      message.error(String(e))
    } finally {
      setExpandedRowLoading(prev => {
        const next = new Set(prev)
        next.delete(id)
        return next
      })
    }
  }

  async function copyText(label: string, text?: string | null) {
    if (!text) return
    try {
      await navigator.clipboard.writeText(text)
      message.success(`${label} 已复制`)
    } catch (e) {
      message.error(String(e))
    }
  }

  function formatTokenCount(v?: number | null) {
    if (!v) return '0'
    if (Math.abs(v) >= 1_000_000) return `${(v / 1_000_000).toFixed(2)}M`
    if (Math.abs(v) >= 1_000) return `${(v / 1_000).toFixed(1)}k`
    return String(v)
  }

  async function handleRefreshAll() {
    await refresh()
    accounts.forEach(a => fetchUsage(a.id))
  }

  async function toggleProxy(checked: boolean) {
    setProxyLoading(true)
    try {
      if (checked) {
        if (!currentAccount) {
          throw new Error('没有活跃账号。请先切换至某个账号，或通过 OAuth 登录。')
        }
        await startProxy(proxyPortInput)
        message.success(`API 代理已启动 (端口 ${proxyPortInput})`)
      } else {
        await stopProxy()
        message.info('API 代理已停止')
      }
    } catch (e) {
      message.error(String(e))
    } finally {
      setProxyLoading(false)
    }
  }

  const isCurrent = (account: CodexAccount) =>
    currentAccount?.id === account.id ||
    (currentAccount?.user_id && currentAccount.user_id === account.user_id)

  const stats = {
    total: accounts.length,
    free: accounts.filter(a => a.plan === 'free').length,
    pro: accounts.filter(a => ['pro', 'ultra', 'plus'].includes(a.plan)).length,
    expired: accounts.filter(a => a.expires_at && a.expires_at < Date.now()).length,
  }

  const columns = [
    {
      title: '状态',
      width: 50,
      render: (_: unknown, record: CodexAccount) =>
        isCurrent(record) ? (
          <Tooltip title="当前系统环境活跃账号">
            <CheckCircleFilled style={{ color: '#52c41a', fontSize: 18 }} />
          </Tooltip>
        ) : null,
    },
    {
      title: '账号',
      key: 'email',
      render: (_: unknown, record: CodexAccount) => (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <Text strong style={{ fontSize: 13 }}>{record.label || record.email}</Text>
            <AccountLabelEditor account={record} />
          </div>
          {record.label && (
            <Text type="secondary" style={{ fontSize: 11 }}>{record.email}</Text>
          )}
        </div>
      ),
    },
    {
      title: '代理',
      width: 90,
      render: (_: unknown, record: CodexAccount) => (
        <Switch
          size="small"
          checked={record.proxy_enabled ?? true}
          onChange={async (checked) => {
            try {
              await updateProxyEnabled(record.id, checked)
              message.success(checked ? '已加入代理池' : '已移出代理池')
            } catch (e) {
              message.error(String(e))
            }
          }}
        />
      ),
    },
    {
      title: '套餐',
      width: 80,
      render: (_: unknown, record: CodexAccount) => <PlanBadge plan={record.plan} />,
    },
    {
      title: 'Token',
      width: 120,
      render: (_: unknown, record: CodexAccount) => {
        const { text, expired } = formatExpiry(record.expires_at)
        return (
          <Space size={4}>
            {expired
              ? <WarningOutlined style={{ color: '#ff4d4f' }} />
              : <ClockCircleOutlined style={{ color: '#8c8c8c' }} />}
            <Text type={expired ? 'danger' : 'secondary'} style={{ fontSize: 12 }}>{text}</Text>
          </Space>
        )
      },
    },
    {
      title: '额度（剩余）',
      width: 220,
      render: (_: unknown, record: CodexAccount) => (
        <UsageCell
          usage={usageMap[record.id]}
          loading={!!usageLoading[record.id]}
        />
      ),
    },
    {
      title: '操作',
      width: 170,
      render: (_: unknown, record: CodexAccount) => (
        <Space>
          {!isCurrent(record) && (
            <Tooltip title="将此账号的 Token 写入 ~/.codex/auth.json">
              <Button
                size="small"
                type="primary"
                icon={<SwapOutlined />}
                loading={switching === record.id}
                onClick={() => handleSwitch(record.id)}
              >
                设为当前
              </Button>
            </Tooltip>
          )}

          {record.has_refresh_token && (
            <Tooltip title="刷新 Token 并更新额度">
              <Button
                size="small"
                icon={<SyncOutlined />}
                loading={refreshingToken === record.id}
                onClick={() => handleTokenRefresh(record.id)}
              />
            </Tooltip>
          )}

          <Popconfirm
            title="确认删除该账号？"
            description="此操作不可撤销。"
            onConfirm={() => handleDelete(record.id)}
            okText="删除"
            cancelText="取消"
            okButtonProps={{ danger: true }}
          >
            <Button size="small" danger icon={<DeleteOutlined />} loading={deleting === record.id} />
          </Popconfirm>
        </Space>
      ),
    },
  ]

  const logColumns = [
    {
      title: '时间',
      dataIndex: 'timestamp',
      width: 170,
      render: (value: string) => (
        <Text className="text-xs text-gray-500">{new Date(value).toLocaleString()}</Text>
      ),
    },
    {
      title: '方法',
      dataIndex: 'method',
      width: 80,
      render: (value: string) => <Text className="font-mono text-xs">{value}</Text>,
    },
    {
      title: '路径',
      dataIndex: 'path',
      render: (value: string) => <Text className="text-xs">{value}</Text>,
    },
    {
      title: '状态',
      dataIndex: 'status',
      width: 90,
      render: (value: number) => {
        const color = value >= 200 && value < 300 ? 'green' : value >= 400 ? 'red' : 'orange'
        return <Tag color={color}>{value}</Tag>
      },
    },
    {
      title: '耗时',
      dataIndex: 'duration_ms',
      width: 90,
      render: (value: number) => <Text className="text-xs">{value} ms</Text>,
    },
    {
      title: '模型',
      dataIndex: 'model',
      width: 140,
      render: (value: string | null) => (
        <Text className="text-xs">{value ?? '--'}</Text>
      ),
    },
    {
      title: '账号',
      dataIndex: 'proxy_account_id',
      width: 140,
      render: (value: string, record: ProxyRequestLog) => (
        <Text className="text-xs font-mono">{record.account_id ?? value}</Text>
      ),
    },
  ]

  return (
    <div className="space-y-6 max-w-5xl mx-auto pb-12">
      {/* Stats */}
      <Row gutter={16}>
        <Col span={6}>
          <Card size="small">
            <Statistic title="总账号数" value={stats.total} prefix={<UserOutlined />} />
          </Card>
        </Col>
        <Col span={6}>
          <Card size="small">
            <Statistic title="付费账号" value={stats.pro} valueStyle={{ color: '#6366f1' }} />
          </Card>
        </Col>
        <Col span={6}>
          <Card size="small">
            <Statistic title="免费账号" value={stats.free} />
          </Card>
        </Col>
        <Col span={6}>
          <Card size="small">
            <Statistic
              title="Token 过期"
              value={stats.expired}
              valueStyle={{ color: stats.expired > 0 ? '#ff4d4f' : undefined }}
            />
          </Card>
        </Col>
      </Row>

      {/* Main table */}
      <Card
        title={
          <div className="flex items-center gap-2">
            <span>账号管理池</span>
            {currentAccount && (
              <Tag color="cyan" className="ml-2 font-mono text-xs shadow-sm" style={{ padding: '0 8px', borderRadius: '4px' }}>
                当前: {currentAccount.label || currentAccount.email} ({currentAccount.plan.toUpperCase()})
              </Tag>
            )}
          </div>
        }
        extra={
          <Space>
            <Button icon={<ReloadOutlined />} onClick={handleRefreshAll} loading={loading}>
              刷新
            </Button>
            <Button type="primary" icon={<PlusOutlined />} onClick={() => setAddOpen(true)}>
              添加账号
            </Button>
          </Space>
        }
      >
        {error && (
          <Alert
            type="error"
            message={error}
            className="mb-4"
            showIcon
            action={<Button size="small" onClick={refresh}>重试</Button>}
          />
        )}

        {accounts.length === 0 && !loading && !error && (
          <div className="text-center py-12 text-gray-400">
            <UserOutlined style={{ fontSize: 48, marginBottom: 12, display: 'block' }} />
            <p className="font-medium">您的管理器中还未绑定任何账号</p>
            <p className="text-sm mt-1">点击右上角的「添加账号」使用浏览器进行 OAuth 一键授权登录</p>
          </div>
        )}

        {(accounts.length > 0 || loading) && (
          <Spin spinning={loading}>
            <Table
              dataSource={accounts}
              columns={columns}
              rowKey="id"
              pagination={{ pageSize: 20, hideOnSinglePage: true }}
              size="middle"
              rowClassName={(record) =>
                isCurrent(record) ? 'bg-indigo-50/40 border-l-2 border-indigo-400' : ''
              }
            />
          </Spin>
        )}
      </Card>

      {/* Proxy Dashboard */}
      <Card
        size="small"
        className={proxyStatus.running ? 'border-green-300 bg-green-50 shadow-sm' : 'border-gray-200 shadow-sm'}
        title={
          <Space>
            <ApiOutlined className={proxyStatus.running ? 'text-green-600' : 'text-gray-400'} />
            <span className="font-semibold text-gray-800">本地 API 反向代理</span>
          </Space>
        }
        extra={
          <Space>
            <Text className="text-xs text-gray-500 mr-2">端口:</Text>
            <InputNumber
              size="small"
              min={1024} max={65535}
              value={proxyPortInput}
              onChange={(val) => setProxyPortInput(val || 8520)}
              disabled={proxyStatus.running}
            />
            {proxyStatus.running && (
              <Tooltip title="热重载：从磁盘重新读取所有账号 Token，无需重启代理">
                <Button
                  size="small"
                  icon={<ReloadOutlined />}
                  onClick={async () => {
                    try {
                      await reloadProxy()
                      message.success('账号池已热重载')
                    } catch (e) {
                      message.error(String(e))
                    }
                  }}
                />
              </Tooltip>
            )}
            <Switch
              checked={proxyStatus.running}
              onChange={toggleProxy}
              loading={proxyLoading}
              style={{ backgroundColor: proxyStatus.running ? '#10b981' : undefined }}
              checkedChildren="运行中"
              unCheckedChildren="已停止"
            />
          </Space>
        }
      >
        <div className="flex items-center justify-between text-sm text-gray-600">
          <div>
            代理拦截本地请求，Round-Robin 轮询账号池转发至 <code>api.openai.com</code>。401 自动刷新 Token，429 冷却 60s 后自动恢复。
          </div>
          {proxyStatus.running && proxyStatus.port && (
            <div className="flex gap-2 flex-wrap justify-end">
              <Tag color="green" bordered={false} className="px-3 py-1 font-mono">
                http://127.0.0.1:{proxyStatus.port}
              </Tag>
              <Tag color="blue" bordered={false} className="px-2 py-1 text-xs">
                ✅ 活跃 {proxyStatus.active}
              </Tag>
              {proxyStatus.cooldown > 0 && (
                <Tag color="orange" bordered={false} className="px-2 py-1 text-xs">
                  ⏳ 冷却 {proxyStatus.cooldown}
                </Tag>
              )}
              {proxyStatus.blocked > 0 && (
                <Tag color="red" bordered={false} className="px-2 py-1 text-xs">
                  🚫 已封 {proxyStatus.blocked}
                </Tag>
              )}
            </div>
          )}
        </div>
      </Card>

      <AnthropicProxyCard />

      <Card
        size="small"
        title="代理设置"
        className="border-gray-200 shadow-sm"
      >
        {proxyConfig ? (
          <div className="space-y-3">
            <div className="flex flex-col gap-2 md:flex-row md:items-center">
              <Input.Password
                placeholder="设置 API Key（可留空）"
                value={apiKeyInput}
                onChange={(e) => setApiKeyInput(e.target.value)}
                className="md:flex-1"
              />
              <Input
                placeholder="Codex HTTP/HTTPS 代理（留空直连）"
                value={proxyConfig.codex_proxy_url ?? ''}
                onChange={(e) => setProxyConfig({ ...proxyConfig, codex_proxy_url: e.target.value || null })}
                className="md:flex-1"
              />
              <Space>
                <Button onClick={handleGenerateApiKey}>生成</Button>
                <Button type="primary" loading={configSaving} onClick={handleSaveProxyConfig}>
                  保存
                </Button>
              </Space>
            </div>
            <div className="flex flex-wrap items-center gap-4 text-sm text-gray-600">
              <span>启用流量日志</span>
              <Switch
                checked={proxyConfig.enable_logging}
                onChange={(val) => setProxyConfig({ ...proxyConfig, enable_logging: val })}
              />
              <span>最大日志</span>
              <InputNumber
                min={100}
                max={20000}
                value={proxyConfig.max_logs}
                onChange={(val) => setProxyConfig({ ...proxyConfig, max_logs: val || 1000 })}
              />
              <span>额度用尽自动禁用账号</span>
              <Switch
                checked={proxyConfig.disable_on_usage_limit}
                onChange={(val) => setProxyConfig({ ...proxyConfig, disable_on_usage_limit: val })}
              />
              <span>模型覆盖</span>
              <Select
                allowClear
                showSearch
                placeholder={codexModelsLoading ? '加载中...' : '按请求'}
                value={proxyConfig.model_override ?? undefined}
                onChange={(val) => setProxyConfig({ ...proxyConfig, model_override: val ?? null })}
                className="w-56"
                options={codexModels.map(m => ({ label: m, value: m }))}
                notFoundContent={codexModelsLoading ? <Spin size="small" /> : '暂无模型'}
              />
              <Button size="small" icon={<ReloadOutlined />} loading={codexModelsLoading} onClick={() => refreshCodexModels()}>
                刷新模型
              </Button>
              <span>思考强度</span>
              <Select
                allowClear
                placeholder="自动"
                value={proxyConfig.reasoning_effort_override ?? undefined}
                onChange={(val) => setProxyConfig({ ...proxyConfig, reasoning_effort_override: val ?? null })}
                className="w-32"
                options={[
                  { label: 'none', value: 'none' },
                  { label: 'low', value: 'low' },
                  { label: 'medium', value: 'medium' },
                  { label: 'high', value: 'high' },
                  { label: 'xhigh', value: 'xhigh' },
                ]}
              />
            </div>
            <Text type="secondary" className="text-xs">
              客户端需在 Authorization Bearer 或 x-api-key 中携带 API Key（留空则不校验）。
            </Text>
            <Text type="secondary" className="text-xs">
              Codex upstream 默认通过 <code>http://127.0.0.1:36666</code> 同时代理 HTTP 与 HTTPS 请求。
            </Text>
          </div>
        ) : (
          <Spin size="small" />
        )}
      </Card>

      <Card
        size="small"
        title="流量日志"
        className="border-gray-200 shadow-sm"
        extra={
          <Space>
            <Button size="small" icon={<ReloadOutlined />} onClick={() => refreshProxyLogs()}>
              刷新
            </Button>
            <Button
              size="small"
              icon={<ReloadOutlined />}
              onClick={() => refreshProxyTokenStats()}
              loading={tokenStatsLoading}
            >
              刷新统计
            </Button>
            <Popconfirm title="确认清空日志？" onConfirm={handleClearLogs}>
              <Button size="small" danger>清空</Button>
            </Popconfirm>
            <Button
              size="small"
              type="text"
              onClick={() => setLogsCollapsed(v => !v)}
            >
              {logsCollapsed ? '展开' : '收起'}
            </Button>
          </Space>
        }
      >
        {!logsCollapsed && (
          <div>
            <Card
              size="small"
              className="mb-3 border-gray-200"
              title="Token 统计"
              extra={(
                <Select
                  size="small"
                  value={tokenStatsHours}
                  onChange={setTokenStatsHours}
                  className="w-28"
                  options={[
                    { label: '近 1 小时', value: 1 },
                    { label: '近 24 小时', value: 24 },
                    { label: '近 7 天', value: 24 * 7 },
                  ]}
                />
              )}
            >
              <Spin spinning={tokenStatsLoading}>
                <Row gutter={12}>
                  <Col span={4}><Statistic title="请求" value={tokenStats?.total_requests ?? 0} /></Col>
                  <Col span={4}><Statistic title="成功" value={tokenStats?.success_requests ?? 0} /></Col>
                  <Col span={4}><Statistic title="错误" value={tokenStats?.error_requests ?? 0} /></Col>
                  <Col span={4}><Statistic title="输入 Token" value={formatTokenCount(tokenStats?.input_tokens)} /></Col>
                  <Col span={4}><Statistic title="输出 Token" value={formatTokenCount(tokenStats?.output_tokens)} /></Col>
                  <Col span={4}><Statistic title="总 Token" value={formatTokenCount(tokenStats?.total_tokens)} /></Col>
                </Row>
                <div className="mt-3 grid grid-cols-1 md:grid-cols-2 gap-3">
                  <div>
                    <Text strong className="text-xs">Top 模型</Text>
                    <div className="mt-1 flex flex-wrap gap-2">
                      {(tokenStats?.top_models ?? []).length > 0
                        ? (tokenStats?.top_models ?? []).map((item) => (
                          <Tag key={`model-${item.name}`} color="blue">
                            {item.name} · {formatTokenCount(item.total_tokens)}
                          </Tag>
                        ))
                        : <Text type="secondary" className="text-xs">暂无</Text>}
                    </div>
                  </div>
                  <div>
                    <Text strong className="text-xs">Top 账号</Text>
                    <div className="mt-1 flex flex-wrap gap-2">
                      {(tokenStats?.top_accounts ?? []).length > 0
                        ? (tokenStats?.top_accounts ?? []).map((item) => (
                          <Tag key={`account-${item.name}`} color="geekblue">
                            {item.name} · {formatTokenCount(item.total_tokens)}
                          </Tag>
                        ))
                        : <Text type="secondary" className="text-xs">暂无</Text>}
                    </div>
                  </div>
                </div>
              </Spin>
            </Card>
            <div className="flex flex-col gap-2 md:flex-row md:items-center md:justify-between mb-3">
              <Input
                placeholder="搜索路径/状态/账号"
                value={logsFilter}
                onChange={(e) => {
                  setLogsFilter(e.target.value)
                  setLogsPage(1)
                }}
                className="md:w-72"
              />
              <div className="flex items-center gap-3 text-sm text-gray-600">
                <span>仅错误</span>
                <Switch
                  checked={logsErrorsOnly}
                  onChange={(val) => {
                    setLogsErrorsOnly(val)
                    setLogsPage(1)
                  }}
                />
                <Text type="secondary" className="text-xs">
                  共 {logsTotal} 条
                </Text>
              </div>
            </div>
            <Table
          rowKey="id"
          size="small"
          loading={logsLoading}
          columns={logColumns}
          dataSource={logs}
          expandable={{
            onExpand: (expanded, record) => {
              if (expanded) {
                loadExpandedRow(record.id)
              }
            },
            expandedRowRender: (record) => {
              const detail = expandedRowData[record.id]
              if (expandedRowLoading.has(record.id)) return <Spin size="small" />
              if (!detail) {
                const err = expandedRowError[record.id]
                return (
                  <div className="flex items-center gap-2">
                    <Text type="secondary" className="text-xs">
                      {err ? `加载失败: ${err}` : '加载失败'}
                    </Text>
                    <Button size="small" onClick={() => loadExpandedRow(record.id)}>重试</Button>
                  </div>
                )
              }
              return (
                <div className="space-y-3 py-2 px-1">
                  <div className="space-y-1">
                    <div className="flex items-center justify-between">
                      <Text strong className="text-xs">请求地址</Text>
                      <Button size="small" onClick={() => copyText('请求地址', detail.request_url)}>复制</Button>
                    </div>
                    <JsonViewer value={detail.request_url} small />
                  </div>
                  <div className="grid grid-cols-2 gap-3">
                    <div className="space-y-1">
                      <div className="flex items-center justify-between">
                        <Text strong className="text-xs">请求头</Text>
                        <Button size="small" onClick={() => copyText('请求头', detail.request_headers)}>复制</Button>
                      </div>
                      <JsonViewer value={detail.request_headers} />
                    </div>
                    <div className="space-y-1">
                      <div className="flex items-center justify-between">
                        <Text strong className="text-xs">响应头</Text>
                        <Button size="small" onClick={() => copyText('响应头', detail.response_headers)}>复制</Button>
                      </div>
                      <JsonViewer value={detail.response_headers} />
                    </div>
                  </div>
                  <div className="grid grid-cols-2 gap-3">
                    <div className="space-y-1">
                      <div className="flex items-center justify-between">
                        <Text strong className="text-xs">请求体</Text>
                        <Button size="small" onClick={() => copyText('请求体', detail.request_body)}>复制</Button>
                      </div>
                      <JsonViewer value={detail.request_body} />
                    </div>
                    <div className="space-y-1">
                      <div className="flex items-center justify-between">
                        <Text strong className="text-xs">响应体</Text>
                        <Button size="small" onClick={() => copyText('响应体', detail.response_body)}>复制</Button>
                      </div>
                      <JsonViewer value={detail.response_body} />
                    </div>
                  </div>
                  {detail.error && (
                    <Text type="danger" className="text-xs">错误: {detail.error}</Text>
                  )}
                  <Text type="secondary" className="text-xs">
                    Tokens: 输入 {detail.input_tokens ?? '--'} · 输出 {detail.output_tokens ?? '--'}
                  </Text>
                </div>
              )
            },
          }}
          pagination={{
            current: logsPage,
            pageSize: logsPageSize,
            total: logsTotal,
            showSizeChanger: true,
            onChange: (page, pageSize) => {
              setLogsPage(page)
              setLogsPageSize(pageSize)
            },
          }}
            />
          </div>
        )}
      </Card>

      <AddAccountDialog open={addOpen} onClose={() => setAddOpen(false)} />
    </div>
  )
}
