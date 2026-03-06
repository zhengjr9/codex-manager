import { Card, Space, Tag, Typography } from 'antd'
import { ApiOutlined } from '@ant-design/icons'
import { useAccountStore } from '../../stores/useAccountStore'

export default function AnthropicProxyCard() {
  const proxyStatus = useAccountStore(s => s.proxyStatus)
  const port = proxyStatus.port
  const running = proxyStatus.running

  return (
    <Card
      size="small"
      className={running ? 'border-purple-300 bg-purple-50 shadow-sm' : 'border-gray-200 shadow-sm'}
      title={
        <Space>
          <ApiOutlined className={running ? 'text-purple-600' : 'text-gray-400'} />
          <span className="font-semibold text-gray-800">Anthropic 协议反向代理</span>
        </Space>
      }
    >
      <div className="text-sm text-gray-600 space-y-1">
        <div>
          Anthropic Messages 协议与 OpenAI 协议共用同一端口。请直接通过 <code>/v1/messages</code> 访问。
        </div>
        <div>
          复用「本地 API 反向代理」的 API Key 与账号池，未启动时请先在上方开启代理。
        </div>
        {running && port && (
          <div className="mt-2 flex gap-2 flex-wrap items-center">
            <Tag color="purple" bordered={false} className="px-3 py-1 font-mono">
              http://127.0.0.1:{port}
            </Tag>
            <Typography.Text type="secondary" className="text-xs">
              兼容 Anthropic Messages 协议
            </Typography.Text>
          </div>
        )}
      </div>
    </Card>
  )
}
