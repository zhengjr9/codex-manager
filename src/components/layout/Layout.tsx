import { Outlet } from 'react-router-dom'
import { Layout as AntLayout, Typography } from 'antd'
import { CodeOutlined } from '@ant-design/icons'

const { Header, Content } = AntLayout

export default function Layout() {
  return (
    <AntLayout className="min-h-screen">
      <Header className="flex items-center gap-3 px-6" style={{ background: '#1a1a2e' }}>
        <CodeOutlined style={{ color: '#6366f1', fontSize: 22 }} />
        <Typography.Title level={4} style={{ color: '#fff', margin: 0 }}>
          Codex Manager
        </Typography.Title>
        <span className="text-gray-400 text-sm ml-1">多账号管理</span>
      </Header>
      <Content className="p-6">
        <Outlet />
      </Content>
    </AntLayout>
  )
}
