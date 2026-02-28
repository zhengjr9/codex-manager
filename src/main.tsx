import React from 'react'
import ReactDOM from 'react-dom/client'
import { RouterProvider, createHashRouter } from 'react-router-dom'
import { ConfigProvider, theme } from 'antd'
import Layout from './components/layout/Layout'
import AccountsPage from './pages/AccountsPage'
import './index.css'

const router = createHashRouter([
  {
    path: '/',
    element: <Layout />,
    children: [
      { index: true, element: <AccountsPage /> },
    ],
  },
])

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <ConfigProvider theme={{ algorithm: theme.defaultAlgorithm }}>
      <RouterProvider router={router} />
    </ConfigProvider>
  </React.StrictMode>
)
