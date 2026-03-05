import { useState } from 'react'
import { Modal, Input, Form, message, Button, Typography } from 'antd'
import { CopyOutlined, LinkOutlined } from '@ant-design/icons'
import { useAccountStore } from '../../stores/useAccountStore'
import { accountService } from '../../services/accountService'

const { Text } = Typography

interface Props {
  open: boolean
  onClose: () => void
}

type Step = 'choice' | 'label' | 'oauth_label' | 'oauth_manual'

export default function AddAccountDialog({ open, onClose }: Props) {
  const [form] = Form.useForm()
  const [loading, setLoading] = useState(false)
  const [step, setStep] = useState<Step>('choice')
  const [authUrl, setAuthUrl] = useState('')
  const [callbackInput, setCallbackInput] = useState('')
  const importCurrent = useAccountStore(s => s.importCurrent)
  const oauthLogin = useAccountStore(s => s.oauthLogin)

  // -- Method 1: Manual Terminal Login --
  async function handleTerminalLogin() {
    setLoading(true)
    try {
      const res = await accountService.login()
      message.info(res.message, 6)
      setStep('label')
    } catch (e) {
      message.error(String(e))
    } finally {
      setLoading(false)
    }
  }

  async function handleImport() {
    setLoading(true)
    try {
      const values = form.getFieldsValue()
      await importCurrent(values.label)
      message.success('账号导入成功')
      handleCancel()
    } catch (e) {
      message.error(String(e))
    } finally {
      setLoading(false)
    }
  }

  // -- Method 2a: Auto OAuth (opens browser, waits for local callback) --
  async function handleOAuthLogin() {
    setLoading(true)
    try {
      const values = form.getFieldsValue()
      message.loading({ content: '请在浏览器中完成登录...', key: 'oauth' })
      await oauthLogin(values.label)
      message.success({ content: 'OAuth 登录并导入成功', key: 'oauth' })
      handleCancel()
    } catch (e) {
      message.error({ content: String(e), key: 'oauth' })
    } finally {
      setLoading(false)
    }
  }

  // -- Method 2b: Manual OAuth (copy URL → paste callback) --
  async function handleGetOAuthUrl() {
    setLoading(true)
    try {
      const res = await accountService.getOAuthUrl()
      setAuthUrl(res.auth_url)
      setStep('oauth_manual')
    } catch (e) {
      message.error(String(e))
    } finally {
      setLoading(false)
    }
  }

  async function handleCompleteManual() {
    if (!callbackInput.trim()) {
      message.error('请粘贴回调地址')
      return
    }
    setLoading(true)
    try {
      const values = form.getFieldsValue()
      await accountService.completeOAuthManual(callbackInput.trim(), values.label)
      message.success('OAuth 登录并导入成功')
      handleCancel()
    } catch (e) {
      message.error(String(e))
    } finally {
      setLoading(false)
    }
  }

  async function copyAuthUrl() {
    try {
      await navigator.clipboard.writeText(authUrl)
      message.success('登录地址已复制')
    } catch {
      message.error('复制失败，请手动复制')
    }
  }

  function handleCancel() {
    form.resetFields()
    setStep('choice')
    setLoading(false)
    setAuthUrl('')
    setCallbackInput('')
    onClose()
  }

  return (
    <Modal
      title="添加 Codex 账号"
      open={open}
      onCancel={handleCancel}
      footer={null}
      width={500}
    >
      {/* Step 1: Choose method */}
      {step === 'choice' && (
        <div className="space-y-4 py-2">
          <p className="text-gray-600 text-sm">选择添加方式：</p>
          <div className="grid grid-cols-1 gap-3 border-b pb-4 mb-2">
            <button
              onClick={() => setStep('oauth_label')}
              className="flex flex-col items-center gap-2 p-6 border-2 border-dashed border-indigo-400 rounded-lg hover:border-indigo-600 hover:bg-indigo-50 transition-colors cursor-pointer w-full text-indigo-700"
            >
              <span className="text-3xl">🚀</span>
              <span className="font-semibold">一键 OAuth 浏览器登录</span>
              <span className="text-sm text-indigo-500/80">推荐。自动打开浏览器，自动捕获 Token</span>
            </button>
          </div>

          <div className="grid grid-cols-3 gap-3">
            <button
              onClick={handleGetOAuthUrl}
              disabled={loading}
              className="flex flex-col items-center gap-2 p-4 border border-indigo-200 rounded-lg hover:border-indigo-400 hover:bg-indigo-50 transition-colors cursor-pointer disabled:opacity-50"
            >
              <span className="text-xl">🔗</span>
              <span className="font-medium text-sm text-center">复制链接登录</span>
              <span className="text-xs text-gray-500 text-center">手动打开，粘贴回调地址</span>
            </button>
            <button
              onClick={handleTerminalLogin}
              disabled={loading}
              className="flex flex-col items-center gap-2 p-4 border border-gray-200 rounded-lg hover:border-gray-400 transition-colors cursor-pointer disabled:opacity-50"
            >
              <span className="text-xl">💻</span>
              <span className="font-medium text-sm">终端登录</span>
              <span className="text-xs text-gray-500 text-center">运行 codex login</span>
            </button>
            <button
              onClick={() => setStep('label')}
              className="flex flex-col items-center gap-2 p-4 border border-gray-200 rounded-lg hover:border-gray-400 transition-colors cursor-pointer"
            >
              <span className="text-xl">📥</span>
              <span className="font-medium text-sm">导入缓存</span>
              <span className="text-xs text-gray-500 text-center">从 ~/.codex/ 导入</span>
            </button>
          </div>
        </div>
      )}

      {/* Step 2a: Auto OAuth - set label then open browser */}
      {step === 'oauth_label' && (
        <Form form={form} layout="vertical" className="py-2">
          <p className="text-gray-600 text-sm mb-4">
            即将打开系统浏览器前往 Auth0 登录，授权后将自动捕获 Tokens 并绑定。
          </p>
          <Form.Item name="label" label="账号备注（可选）">
            <Input placeholder="例如：Plus主号..." />
          </Form.Item>
          <div className="flex gap-2 justify-end">
            <button type="button" onClick={() => setStep('choice')} className="px-4 py-2 text-sm border border-gray-300 rounded-lg hover:bg-gray-50">返回</button>
            <button type="button" onClick={handleOAuthLogin} disabled={loading} className="px-4 py-2 text-sm bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50 font-medium">
              {loading ? '等待浏览器授权...' : '打开浏览器登录'}
            </button>
          </div>
        </Form>
      )}

      {/* Step 2b: Manual OAuth - copy URL, paste callback */}
      {step === 'oauth_manual' && (
        <Form form={form} layout="vertical" className="py-2 space-y-3">
          <div className="space-y-2">
            <Text strong className="text-sm">第 1 步：复制登录地址，在浏览器中打开并完成授权</Text>
            <div className="flex gap-2">
              <Input
                value={authUrl}
                readOnly
                className="font-mono text-xs"
              />
              <Button icon={<CopyOutlined />} onClick={copyAuthUrl}>复制</Button>
              <Button
                icon={<LinkOutlined />}
                onClick={() => window.open(authUrl, '_blank')}
              >
                打开
              </Button>
            </div>
          </div>

          <div className="space-y-2">
            <Text strong className="text-sm">第 2 步：授权完成后，将浏览器地址栏的回调地址粘贴到此处</Text>
            <Text type="secondary" className="text-xs block">
              回调地址格式：<code>http://localhost:1455/auth/callback?code=...&state=...</code>
            </Text>
            <Input.TextArea
              placeholder="粘贴回调地址..."
              value={callbackInput}
              onChange={e => setCallbackInput(e.target.value)}
              rows={3}
              className="font-mono text-xs"
            />
          </div>

          <Form.Item name="label" label="账号备注（可选）" className="mb-0">
            <Input placeholder="例如：Plus主号..." />
          </Form.Item>

          <div className="flex gap-2 justify-end pt-1">
            <button type="button" onClick={() => setStep('choice')} className="px-4 py-2 text-sm border border-gray-300 rounded-lg hover:bg-gray-50">返回</button>
            <button
              type="button"
              onClick={handleCompleteManual}
              disabled={loading || !callbackInput.trim()}
              className="px-4 py-2 text-sm bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50 font-medium"
            >
              {loading ? '验证中...' : '完成登录'}
            </button>
          </div>
        </Form>
      )}

      {/* Step 3: Import from ~/.codex/auth.json */}
      {step === 'label' && (
        <Form form={form} layout="vertical" className="py-2">
          <p className="text-gray-600 text-sm mb-4">
            将当前 <code>~/.codex/auth.json</code> 中的账号保存到管理器。
          </p>
          <Form.Item name="label" label="账号备注（可选）">
            <Input placeholder="例如：工作账号、个人账号..." />
          </Form.Item>
          <div className="flex gap-2 justify-end">
            <button type="button" onClick={() => setStep('choice')} className="px-4 py-2 text-sm border border-gray-300 rounded-lg hover:bg-gray-50">返回</button>
            <button type="button" onClick={handleImport} disabled={loading} className="px-4 py-2 text-sm bg-gray-800 text-white rounded-lg hover:bg-gray-900 disabled:opacity-50">
              {loading ? '导入中...' : '确认导入'}
            </button>
          </div>
        </Form>
      )}
    </Modal>
  )
}
