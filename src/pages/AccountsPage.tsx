import { useEffect, useState } from 'react'
import {
  Table, Button, Popconfirm, Tag, Tooltip, message, Spin, Alert,
  Space, Typography, Card, Statistic, Row, Col
} from 'antd'
import {
  PlusOutlined, ReloadOutlined, CheckCircleFilled,
  DeleteOutlined, SwapOutlined, UserOutlined, ClockCircleOutlined,
  WarningOutlined
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
  const { accounts, currentAccount, loading, error, fetchAccounts, fetchCurrent, switchAccount, deleteAccount, refresh } = useAccountStore()
  const [addOpen, setAddOpen] = useState(false)
  const [switching, setSwitching] = useState<string | null>(null)
  const [deleting, setDeleting] = useState<string | null>(null)

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
          <Tooltip title="当前活跃账号">
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
      title: '最后刷新',
      width: 140,
      render: (_: unknown, record: CodexAccount) => {
        if (!record.last_refresh) return <Text type="secondary" className="text-xs">-</Text>
        const d = new Date(record.last_refresh)
        return <Text type="secondary" className="text-xs">{d.toLocaleDateString('zh-CN')} {d.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' })}</Text>
      },
    },
    {
      title: '操作',
      width: 160,
      render: (_: unknown, record: CodexAccount) => (
        <Space>
          {!isCurrent(record) && (
            <Button
              size="small"
              type="primary"
              icon={<SwapOutlined />}
              loading={switching === record.id}
              onClick={() => handleSwitch(record.id)}
            >
              切换
            </Button>
          )}
          {isCurrent(record) && (
            <Tag color="green" icon={<CheckCircleFilled />}>使用中</Tag>
          )}
          <Popconfirm
            title="确认删除该账号？"
            description="此操作不可撤销，账号数据将从管理器中移除。"
            onConfirm={() => handleDelete(record.id)}
            okText="删除"
            cancelText="取消"
            okButtonProps={{ danger: true }}
          >
            <Button
              size="small"
              danger
              icon={<DeleteOutlined />}
              loading={deleting === record.id}
            />
          </Popconfirm>
        </Space>
      ),
    },
  ]

  return (
    <div className="space-y-4 max-w-5xl mx-auto">
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
            <Statistic title="Token 过期" value={stats.expired} valueStyle={{ color: stats.expired > 0 ? '#ff4d4f' : undefined }} />
          </Card>
        </Col>
      </Row>

      {/* Main table */}
      <Card
        title={
          <div className="flex items-center gap-2">
            <span>账号列表</span>
            {currentAccount && (
              <Tag color="blue">
                当前: {currentAccount.label || currentAccount.email}
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
            <p className="font-medium">还没有管理任何账号</p>
            <p className="text-sm mt-1">先运行 <code>codex login</code>，然后点击「添加账号」导入</p>
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
                isCurrent(record) ? 'bg-green-50' : ''
              }
            />
          </Spin>
        )}
      </Card>

      <AddAccountDialog open={addOpen} onClose={() => setAddOpen(false)} />
    </div>
  )
}
