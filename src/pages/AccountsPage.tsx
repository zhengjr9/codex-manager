import { useEffect, useState } from 'react'
import {
  Table, Button, Popconfirm, Tag, Tooltip, message, Spin, Alert,
  Space, Typography, Card, Statistic, Row, Col, Divider, Switch, InputNumber
} from 'antd'
import {
  PlusOutlined, ReloadOutlined, CheckCircleFilled,
  DeleteOutlined, SwapOutlined, UserOutlined, ClockCircleOutlined,
  WarningOutlined, ApiOutlined, SyncOutlined
} from '@ant-design/icons'
import { useAccountStore } from '../stores/useAccountStore'
import type { CodexAccount } from '../types/account'
import PlanBadge from '../components/accounts/PlanBadge'
import AccountLabelEditor from '../components/accounts/AccountLabelEditor'
import AddAccountDialog from '../components/accounts/AddAccountDialog'

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

export default function AccountsPage() {
  const {
    accounts, currentAccount, loading, error, proxyStatus,
    fetchAccounts, fetchCurrent, fetchProxyStatus,
    switchAccount, deleteAccount, refresh, refreshAccountToken,
    startProxy, stopProxy
  } = useAccountStore()

  const [addOpen, setAddOpen] = useState(false)
  const [switching, setSwitching] = useState<string | null>(null)
  const [deleting, setDeleting] = useState<string | null>(null)
  const [refreshingToken, setRefreshingToken] = useState<string | null>(null)

  const [proxyPortInput, setProxyPortInput] = useState<number>(8080)
  const [proxyLoading, setProxyLoading] = useState(false)

  useEffect(() => {
    refresh()
  }, [])

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
      message.success('Token 刷新成功')
    } catch (e) {
      message.error('Token 刷新失败: ' + String(e))
    } finally {
      setRefreshingToken(null)
    }
  }

  async function toggleProxy(checked: boolean) {
    setProxyLoading(true)
    try {
      if (checked) {
        if (!currentAccount) throw new Error("没有活跃账号。请先切换至某个账号，或通过 OAuth 登录。")
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
      width: 60,
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
        <div className="flex flex-col gap-0.5">
          <div className="flex items-center gap-2">
            <Text strong className="text-sm">{record.label || record.email}</Text>
            <AccountLabelEditor account={record} />
          </div>
          {record.label && (
            <Text type="secondary" className="text-xs">{record.email}</Text>
          )}
        </div>
      ),
    },
    {
      title: '套餐',
      width: 90,
      render: (_: unknown, record: CodexAccount) => <PlanBadge plan={record.plan} />,
    },
    {
      title: 'Token 状态',
      width: 140,
      render: (_: unknown, record: CodexAccount) => {
        const { text, expired } = formatExpiry(record.expires_at)
        return (
          <Space size={4}>
            {expired
              ? <WarningOutlined style={{ color: '#ff4d4f' }} />
              : <ClockCircleOutlined style={{ color: '#8c8c8c' }} />}
            <Text type={expired ? 'danger' : 'secondary'} className="text-xs">{text}</Text>
          </Space>
        )
      },
    },
    {
      title: '操作',
      width: 200,
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
             <Tooltip title="强制通过 Refresh Token 更新访问令牌">
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

  return (
    <div className="space-y-6 max-w-5xl mx-auto pb-12">
      {/* Proxy Dashboard */}
      <Card
        size="small"
        className={proxyStatus.running ? "border-green-300 bg-green-50 shadow-sm" : "border-gray-200 shadow-sm"}
        title={
          <Space>
            <ApiOutlined className={proxyStatus.running ? "text-green-600" : "text-gray-400"} />
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
               onChange={(val) => setProxyPortInput(val || 8080)}
               disabled={proxyStatus.running}
             />
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
             代理将您的本地请求拦截，并依次轮询（Round-Robin）账号池中所有健康的账号，将请求转发至 <code>api.openai.com</code>。
           </div>
           {proxyStatus.running && proxyStatus.port && (
             <div className="flex gap-4">
                 <Tag color="green" bordered={false} className="px-3 py-1">
                    Base URL: http://127.0.0.1:{proxyStatus.port}/v1
                 </Tag>
                 <Tag color="purple" bordered={false} className="px-3 py-1 font-mono text-xs">
                    正在轮询调用 {proxyStatus.account_count} 个健康账号
                 </Tag>
             </div>
           )}
        </div>
      </Card>

      {/* Row Stats */}
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
            <Statistic title="Token 过期" value={stats.expired} valueStyle={{ color: stats.expired > 0 ? '#ff4d4f' : undefined }} />
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
                当前环境系统配置: {currentAccount.label || currentAccount.email}
                ({currentAccount.plan.toUpperCase()})
              </Tag>
            )}
          </div>
        }
        extra={
          <Space>
            <Button icon={<ReloadOutlined />} onClick={refresh} loading={loading}>
              刷新
            </Button>
            <Button type="primary" icon={<PlusOutlined />} onClick={() => setAddOpen(true)}>
              添加外部账号
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
            <p className="text-sm mt-1">点击右上角的「添加外部账号」使用浏览器进行 OAuth 一键授权登录</p>
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

      <AddAccountDialog open={addOpen} onClose={() => setAddOpen(false)} />
    </div>
  )
}