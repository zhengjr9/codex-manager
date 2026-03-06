import { useEffect, useState } from 'react'
import { Card, Space, Tag, message, Switch, InputNumber } from 'antd'
import { ApiOutlined } from '@ant-design/icons'
import { accountService } from '../../services/accountService'

export default function AnthropicProxyCard() {
  const [proxyRunning, setProxyRunning] = useState(false)
  const [proxyPort, setProxyPort] = useState<number | null>(null)
  const [portInput, setPortInput] = useState(8081)
  const [proxyLoading, setProxyLoading] = useState(false)

  async function loadStatus() {
    try {
      const s = await accountService.getAnthropicProxyStatus()
      setProxyRunning(s.running)
      setProxyPort(s.port)
      if (s.port) setPortInput(s.port)
    } catch {}
  }

  useEffect(() => {
    loadStatus()
  }, [])

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

  return (
    <Card
      size="small"
      className={proxyRunning ? 'border-purple-300 bg-purple-50 shadow-sm' : 'border-gray-200 shadow-sm'}
      title={
        <Space>
          <ApiOutlined className={proxyRunning ? 'text-purple-600' : 'text-gray-400'} />
          <span className="font-semibold text-gray-800">Anthropic 协议反向代理</span>
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
      <div className="text-sm text-gray-600">
        将 Anthropic Messages 协议请求转换为 OpenAI Chat Completions，并转发到本地 OpenAI 反向代理。
        该代理复用本地 OpenAI 代理的 API Key 与账号池，请先启动「本地 API 反向代理」。
        {proxyRunning && proxyPort && (
          <div className="mt-2 flex gap-2 flex-wrap">
            <Tag color="purple" bordered={false} className="px-3 py-1 font-mono">
              http://127.0.0.1:{proxyPort}
            </Tag>
          </div>
        )}
      </div>
    </Card>
  )
}
