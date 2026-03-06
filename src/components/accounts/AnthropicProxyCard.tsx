import { useState, useEffect } from 'react'
import {
  Card, Button, Input, Space, Tag, Table, Popconfirm, message, Switch, InputNumber, Tooltip
} from 'antd'
import { PlusOutlined, DeleteOutlined, ApiOutlined, ReloadOutlined, EyeInvisibleOutlined, EyeOutlined } from '@ant-design/icons'
import { accountService, type AnthropicKeyEntry } from '../../services/accountService'
import type { ColumnsType } from 'antd/es/table'

export default function AnthropicProxyCard() {
  const [keys, setKeys] = useState<AnthropicKeyEntry[]>([])
  const [loading, setLoading] = useState(false)
  const [proxyRunning, setProxyRunning] = useState(false)
  const [proxyPort, setProxyPort] = useState<number | null>(null)
  const [portInput, setPortInput] = useState(8081)
  const [proxyLoading, setProxyLoading] = useState(false)
  const [addKey, setAddKey] = useState('')
  const [addLabel, setAddLabel] = useState('')
  const [adding, setAdding] = useState(false)
  const [showKey, setShowKey] = useState<Record<string, boolean>>({})

  async function loadKeys() {
    setLoading(true)
    try {
      const k = await accountService.listAnthropicKeys()
      setKeys(k)
    } catch (e) {
      message.error(String(e))
    } finally {
      setLoading(false)
    }
  }

  async function loadStatus() {
    try {
      const s = await accountService.getAnthropicProxyStatus()
      setProxyRunning(s.running)
      setProxyPort(s.port)
      if (s.port) setPortInput(s.port)
    } catch {}
  }

  useEffect(() => {
    loadKeys()
    loadStatus()
  }, [])

  async function handleAdd() {
    if (!addKey.trim()) { message.error('请输入 API Key 或 OAuth Token'); return }
    setAdding(true)
    try {
      await accountService.addAnthropicKey(addLabel.trim() || undefined, addKey.trim())
      setAddKey('')
      setAddLabel('')
      await loadKeys()
      message.success('Key 已添加')
    } catch (e) {
      message.error(String(e))
    } finally {
      setAdding(false)
    }
  }

  async function handleDelete(id: string) {
    try {
      await accountService.deleteAnthropicKey(id)
      await loadKeys()
      message.success('已删除')
    } catch (e) {
      message.error(String(e))
    }
  }

  async function toggleProxy(checked: boolean) {
    setProxyLoading(true)
    try {
      if (!checked) {
        await accountService.stopAnthropicProxy()
        await loadStatus()
        message.success('Anthropic 代理已停止')
      } else {
        const res = await accountService.startAnthropicProxy(portInput)
        await loadStatus()
        message.success(`Anthropic 代理已启动，端口 ${res.port}`)
      }
    } catch (e) {
      message.error(String(e))
    } finally {
      setProxyLoading(false)
    }
  }

  function maskKey(key: string, id: string) {
    if (showKey[id]) return key
    if (key.startsWith('sk-ant-')) return `sk-ant-...${key.slice(-6)}`
    return `${key.slice(0, 8)}...${key.slice(-6)}`
  }

  const columns: ColumnsType<AnthropicKeyEntry> = [
    {
      title: '备注',
      dataIndex: 'label',
      width: 130,
      render: (v: string | null) => v
        ? <span className="text-sm">{v}</span>
        : <span className="text-gray-400 text-xs">无备注</span>,
    },
    {
      title: '类型',
      width: 90,
      render: (_: unknown, record: AnthropicKeyEntry) => (
        <Tag color={record.key.startsWith('sk-ant-') ? 'blue' : 'purple'} className="text-xs">
          {record.key.startsWith('sk-ant-') ? 'API Key' : 'OAuth'}
        </Tag>
      ),
    },
    {
      title: 'Key',
      render: (_: unknown, record: AnthropicKeyEntry) => (
        <Space>
          <span className="font-mono text-xs">{maskKey(record.key, record.id)}</span>
          <Button
            size="small" type="text"
            icon={showKey[record.id] ? <EyeInvisibleOutlined /> : <EyeOutlined />}
            onClick={() => setShowKey(prev => ({ ...prev, [record.id]: !prev[record.id] }))}
          />
        </Space>
      ),
    },
    {
      title: '操作',
      width: 70,
      render: (_: unknown, record: AnthropicKeyEntry) => (
        <Popconfirm title="确认删除？" onConfirm={() => handleDelete(record.id)}>
          <Button size="small" danger icon={<DeleteOutlined />} />
        </Popconfirm>
      ),
    },
  ]

  return (
    <Card
      size="small"
      className={proxyRunning ? 'border-purple-300 bg-purple-50 shadow-sm' : 'border-gray-200 shadow-sm'}
      title={
        <Space>
          <ApiOutlined className={proxyRunning ? 'text-purple-600' : 'text-gray-400'} />
          <span className="font-semibold text-gray-800">Anthropic API 反向代理</span>
        </Space>
      }
      extra={
        <Space>
          <InputNumber
            size="small" min={1024} max={65535}
            value={portInput}
            onChange={v => setPortInput(v ?? 8081)}
            disabled={proxyRunning}
          />
          <Switch
            checked={proxyRunning}
            onChange={toggleProxy}
            loading={proxyLoading}
            style={{ backgroundColor: proxyRunning ? '#9333ea' : undefined }}
            checkedChildren="运行中"
            unCheckedChildren="已停止"
          />
        </Space>
      }
    >
      <div className="text-sm text-gray-600 mb-3">
        支持 Anthropic API Key (<code>sk-ant-</code>) 和 Claude Code OAuth Token，转发至 <code>api.anthropic.com</code>。429 自动切换账号重试。
        {proxyRunning && proxyPort && (
          <div className="mt-2 flex gap-2 flex-wrap">
            <Tag color="purple" bordered={false} className="px-3 py-1 font-mono">
              http://127.0.0.1:{proxyPort}
            </Tag>
            <Tag color="blue" bordered={false} className="text-xs px-2 py-1">
              {keys.length} 个 Key
            </Tag>
          </div>
        )}
      </div>

      <div className="flex gap-2 mb-3 flex-wrap">
        <Input
          placeholder="备注（可选）"
          value={addLabel}
          onChange={e => setAddLabel(e.target.value)}
          style={{ width: 130 }}
          size="small"
        />
        <Input.Password
          placeholder="sk-ant-... 或 OAuth access_token"
          value={addKey}
          onChange={e => setAddKey(e.target.value)}
          onPressEnter={handleAdd}
          style={{ flex: 1, minWidth: 200 }}
          size="small"
        />
        <Button size="small" type="primary" icon={<PlusOutlined />} loading={adding} onClick={handleAdd}>
          添加
        </Button>
        <Tooltip title="刷新列表">
          <Button size="small" icon={<ReloadOutlined />} onClick={loadKeys} loading={loading} />
        </Tooltip>
      </div>

      <Table
        rowKey="id"
        size="small"
        loading={loading}
        columns={columns}
        dataSource={keys}
        pagination={false}
        locale={{ emptyText: '暂无 Key，请先添加 Anthropic API Key 或 Claude OAuth Token' }}
      />
    </Card>
  )
}
