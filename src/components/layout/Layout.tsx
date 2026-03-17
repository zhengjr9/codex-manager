import { Outlet, useLocation, useNavigate } from 'react-router-dom'
import { Layout as AntLayout, Menu, Typography } from 'antd'
import { ApiOutlined, CodeOutlined, DatabaseOutlined } from '@ant-design/icons'

const { Header, Content } = AntLayout

export default function Layout() {
  const location = useLocation()
  const navigate = useNavigate()

  return (
    <AntLayout className="min-h-screen">
      <Header className="flex items-center gap-6 px-6" style={{ background: '#1a1a2e' }}>
        <div className="flex items-center gap-3 shrink-0">
          <CodeOutlined style={{ color: '#6366f1', fontSize: 22 }} />
          <Typography.Title level={4} style={{ color: '#fff', margin: 0 }}>
            Codex Manager
          </Typography.Title>
        </div>
        <Menu
          mode="horizontal"
          selectedKeys={[
            location.pathname === '/openai-compat'
              ? '/openai-compat'
              : location.pathname === '/ai-cache'
                ? '/ai-cache'
                : '/',
          ]}
          onClick={({ key }) => navigate(key)}
          items={[
            { key: '/', icon: <CodeOutlined />, label: '账号与 Codex 代理' },
            { key: '/openai-compat', icon: <ApiOutlined />, label: 'AI代理' },
            { key: '/ai-cache', icon: <DatabaseOutlined />, label: 'AI缓存' },
          ]}
          style={{
            background: 'transparent',
            color: '#fff',
            minWidth: 420,
            flex: 1,
          }}
          theme="dark"
        />
      </Header>
      <Content className="p-6">
        <Outlet />
      </Content>
    </AntLayout>
  )
}
