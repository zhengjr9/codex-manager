import { useEffect, useState } from 'react'
import {
  Alert,
  Button,
  Card,
  Col,
  Input,
  InputNumber,
  Popconfirm,
  Row,
  Select,
  Space,
  Statistic,
  Switch,
  Table,
  Tag,
  Typography,
  message,
} from 'antd'
import {
  CloudServerOutlined,
  DatabaseOutlined,
  DeleteOutlined,
  ReloadOutlined,
} from '@ant-design/icons'
import {
  accountService,
  type AICacheEntrySummary,
  type AICacheOverview,
  type AICacheTrendPoint,
  type ProxyConfig,
} from '../services/accountService'

const { Text } = Typography

function formatTokenCount(value?: number | null) {
  const safe = value ?? 0
  if (Math.abs(safe) >= 1_000_000) return `${(safe / 1_000_000).toFixed(2)}M`
  if (Math.abs(safe) >= 1_000) return `${(safe / 1_000).toFixed(1)}k`
  return String(safe)
}

export default function AICachePage() {
  const [config, setConfig] = useState<ProxyConfig | null>(null)
  const [saving, setSaving] = useState(false)
  const [loading, setLoading] = useState(false)
  const [windowHours, setWindowHours] = useState(24)
  const [overview, setOverview] = useState<AICacheOverview | null>(null)
  const [trend, setTrend] = useState<AICacheTrendPoint[]>([])
  const [entries, setEntries] = useState<AICacheEntrySummary[]>([])

  async function refreshConfig() {
    const next = await accountService.getProxyConfig()
    setConfig(next)
  }

  async function refreshDashboard(hours = windowHours) {
    setLoading(true)
    try {
      const [overviewData, trendData, entryData] = await Promise.all([
        accountService.getAICacheOverview(hours),
        accountService.getAICacheTrend(hours),
        accountService.listAICacheEntries({ limit: 50, offset: 0 }),
      ])
      setOverview(overviewData)
      setTrend(trendData)
      setEntries(entryData)
    } catch (e) {
      message.error(String(e))
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    refreshConfig().catch((e) => message.error(String(e)))
  }, [])

  useEffect(() => {
    refreshDashboard(windowHours).catch((e) => message.error(String(e)))
  }, [windowHours])

  async function saveConfig() {
    if (!config) return
    setSaving(true)
    try {
      const next = await accountService.updateProxyConfig({
        enable_exact_cache: config.enable_exact_cache,
        exact_cache_ttl_minutes: config.exact_cache_ttl_minutes,
        exact_cache_max_entries: config.exact_cache_max_entries,
        enable_semantic_cache: config.enable_semantic_cache,
        semantic_cache_threshold: config.semantic_cache_threshold,
        vector_provider_mode: config.vector_provider_mode,
        vector_api_base_url: config.vector_api_base_url,
        vector_api_key: config.vector_api_key,
        vector_model: config.vector_model,
      })
      setConfig(next)
      message.success('AI 缓存配置已保存')
    } catch (e) {
      message.error(String(e))
    } finally {
      setSaving(false)
    }
  }

  async function clearCache() {
    try {
      await accountService.clearAICache()
      await refreshDashboard()
      message.success('AI 缓存已清空')
    } catch (e) {
      message.error(String(e))
    }
  }

  return (
    <div className="space-y-6 max-w-6xl mx-auto pb-12">
      <Card
        title={
          <Space>
            <DatabaseOutlined />
            <span>AI 缓存</span>
          </Space>
        }
        extra={
          <Space>
            <Select
              value={windowHours}
              onChange={setWindowHours}
              style={{ width: 120 }}
              options={[
                { label: '近 1 小时', value: 1 },
                { label: '近 24 小时', value: 24 },
                { label: '近 7 天', value: 24 * 7 },
              ]}
            />
            <Button icon={<ReloadOutlined />} onClick={() => refreshDashboard()} loading={loading}>
              刷新
            </Button>
            <Popconfirm title="确认清空所有缓存条目？" onConfirm={clearCache}>
              <Button danger icon={<DeleteOutlined />}>清空缓存</Button>
            </Popconfirm>
          </Space>
        }
      >
        <Alert
          showIcon
          type="info"
          message="默认开箱即用的是 Exact Cache。语义缓存配置入口已经预留，当前推荐先在纯文本、低 temperature 场景下使用。带 tools / 文件编辑 / agent 场景默认会绕过本地缓存。"
        />
      </Card>

      <Card title="缓存设置" loading={!config}>
        {config && (
          <div className="space-y-4">
            <div className="flex flex-wrap items-center gap-4">
              <span>启用 Exact Cache</span>
              <Switch
                checked={config.enable_exact_cache}
                onChange={(value) => setConfig({ ...config, enable_exact_cache: value })}
              />
              <span>TTL（分钟）</span>
              <InputNumber
                min={1}
                max={1440}
                value={config.exact_cache_ttl_minutes}
                onChange={(value) => setConfig({ ...config, exact_cache_ttl_minutes: value ?? 60 })}
              />
              <span>最大条目数</span>
              <InputNumber
                min={100}
                max={20000}
                value={config.exact_cache_max_entries}
                onChange={(value) => setConfig({ ...config, exact_cache_max_entries: value ?? 2000 })}
              />
            </div>

            <div className="flex flex-wrap items-center gap-4">
              <span>启用语义缓存</span>
              <Switch
                checked={config.enable_semantic_cache}
                onChange={(value) => setConfig({ ...config, enable_semantic_cache: value })}
              />
              <span>相似度阈值</span>
              <InputNumber
                min={0.5}
                max={0.9999}
                step={0.01}
                value={config.semantic_cache_threshold}
                onChange={(value) => setConfig({ ...config, semantic_cache_threshold: value ?? 0.95 })}
              />
              <span>向量模型来源</span>
              <Select
                value={config.vector_provider_mode}
                onChange={(value) => setConfig({ ...config, vector_provider_mode: value })}
                style={{ width: 180 }}
                options={[
                  { label: 'local', value: 'local' },
                  { label: 'openai-compatible api', value: 'remote_api' },
                ]}
              />
            </div>

            <div className="grid gap-3 md:grid-cols-3">
              <Input
                prefix={<CloudServerOutlined />}
                placeholder="向量 API 地址"
                value={config.vector_api_base_url ?? ''}
                onChange={(e) => setConfig({ ...config, vector_api_base_url: e.target.value || null })}
              />
              <Input.Password
                placeholder="向量 API Key"
                value={config.vector_api_key ?? ''}
                onChange={(e) => setConfig({ ...config, vector_api_key: e.target.value || null })}
              />
              <Input
                placeholder="向量模型名"
                value={config.vector_model ?? ''}
                onChange={(e) => setConfig({ ...config, vector_model: e.target.value || null })}
              />
            </div>

            <div className="flex items-center justify-between">
              <Text type="secondary">
                远端向量 API 预期为 OpenAI 兼容 embedding 接口；本地模式下后续会优先走内置/本地 embedding。
              </Text>
              <Button type="primary" onClick={saveConfig} loading={saving}>
                保存配置
              </Button>
            </div>
          </div>
        )}
      </Card>

      <Row gutter={16}>
        <Col xs={24} md={8} lg={6}>
          <Card><Statistic title="总请求" value={overview?.total_requests ?? 0} /></Card>
        </Col>
        <Col xs={24} md={8} lg={6}>
          <Card><Statistic title="缓存命中率" value={((overview?.local_hit_rate ?? 0) * 100).toFixed(1)} suffix="%" /></Card>
        </Col>
        <Col xs={24} md={8} lg={6}>
          <Card><Statistic title="本地命中 Token" value={formatTokenCount(overview?.local_cached_input_tokens)} /></Card>
        </Col>
        <Col xs={24} md={8} lg={6}>
          <Card><Statistic title="Provider 缓存 Token" value={formatTokenCount(overview?.provider_cached_input_tokens)} /></Card>
        </Col>
      </Row>

      <Row gutter={16}>
        <Col xs={24} md={8} lg={6}>
          <Card><Statistic title="输入 Token" value={formatTokenCount(overview?.input_tokens)} /></Card>
        </Col>
        <Col xs={24} md={8} lg={6}>
          <Card><Statistic title="输出 Token" value={formatTokenCount(overview?.output_tokens)} /></Card>
        </Col>
        <Col xs={24} md={8} lg={6}>
          <Card><Statistic title="可缓存请求" value={overview?.cache_eligible_requests ?? 0} /></Card>
        </Col>
        <Col xs={24} md={8} lg={6}>
          <Card><Statistic title="绕过请求" value={overview?.bypassed_requests ?? 0} /></Card>
        </Col>
      </Row>

      <Card title="趋势" loading={loading}>
        <Table
          rowKey="bucket"
          pagination={false}
          size="small"
          dataSource={trend}
          columns={[
            { title: '时间桶', dataIndex: 'bucket', width: 180 },
            { title: '总请求', dataIndex: 'total_requests', width: 90 },
            { title: '可缓存', dataIndex: 'cache_eligible_requests', width: 90 },
            { title: '本地命中', dataIndex: 'local_hits', width: 90 },
            {
              title: '命中率',
              key: 'hit_rate',
              width: 90,
              render: (_: unknown, record: AICacheTrendPoint) => {
                const rate = record.cache_eligible_requests > 0
                  ? (record.local_hits / record.cache_eligible_requests) * 100
                  : 0
                return `${rate.toFixed(1)}%`
              },
            },
            {
              title: '本地缓存 Token',
              dataIndex: 'local_cached_input_tokens',
              render: (value: number) => formatTokenCount(value),
            },
            {
              title: 'Provider 缓存 Token',
              dataIndex: 'provider_cached_input_tokens',
              render: (value: number) => formatTokenCount(value),
            },
          ]}
        />
      </Card>

      <Card title="缓存条目" loading={loading}>
        <Table
          rowKey="id"
          size="small"
          dataSource={entries}
          pagination={{ pageSize: 10 }}
          columns={[
            {
              title: '类型',
              dataIndex: 'cache_type',
              width: 90,
              render: (value: string) => <Tag color={value === 'exact' ? 'blue' : 'purple'}>{value}</Tag>,
            },
            { title: '路径', dataIndex: 'path', width: 170 },
            {
              title: '模型',
              dataIndex: 'model',
              width: 150,
              render: (value: string | null) => value ?? '--',
            },
            { title: '命中次数', dataIndex: 'hit_count', width: 90 },
            {
              title: 'Token 节省',
              key: 'saved',
              width: 120,
              render: (_: unknown, record: AICacheEntrySummary) =>
                formatTokenCount(record.local_cached_input_tokens + record.provider_cached_input_tokens),
            },
            {
              title: '过期时间',
              dataIndex: 'expires_at',
              width: 180,
              render: (value: string) => new Date(value).toLocaleString(),
            },
            {
              title: '响应预览',
              dataIndex: 'response_preview',
              render: (value: string | null) => (
                <Text className="text-xs" ellipsis={{ tooltip: value ?? '' }}>
                  {value ?? '--'}
                </Text>
              ),
            },
          ]}
        />
      </Card>
    </div>
  )
}
