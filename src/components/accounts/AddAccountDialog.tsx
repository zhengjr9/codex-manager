import { useState } from 'react'
import { Modal, Input, Form, message, Tabs } from 'antd'
import { useAccountStore } from '../../stores/useAccountStore'
import { accountService } from '../../services/accountService'

interface Props {
  open: boolean
  onClose: () => void
}

export default function AddAccountDialog({ open, onClose }: Props) {
  const [form] = Form.useForm()
  const [loading, setLoading] = useState(false)
  const [step, setStep] = useState<'choice' | 'label' | 'oauth_label'>('choice')
  const importCurrent = useAccountStore(s => s.importCurrent)
  const oauthLogin = useAccountStore(s => s.oauthLogin)

  // -- Method 1: Manual Login (Terminal) --
  async function handleTerminalLogin() {
    setLoading(true)
    try {
      const res = await accountService.login()
      message.info(res.message, 6)
      setStep('label') // go to manual import step
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

  // -- Method 2: In-app OAuth PKCE (Browser) --
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

  function handleCancel() {
    form.resetFields()
    setStep('choice')
    onClose()
  }

  return (
    <Modal
      title="添加 Codex 账号"
      open={open}
      onCancel={handleCancel}
      footer={null}
      width={480}
    >
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
              <span className="text-sm text-indigo-500/80">推荐。自动管理 Refresh Token</span>
            </button>
          </div>

          <div className="grid grid-cols-2 gap-3">
            <button
              onClick={handleTerminalLogin}
              disabled={loading}
              className="flex flex-col items-center gap-2 p-4 border border-gray-200 rounded-lg hover:border-gray-400 transition-colors cursor-pointer disabled:opacity-50"
            >
              <span className="text-xl">💻</span>
              <span className="font-medium text-sm">运行 codex login</span>
              <span className="text-xs text-gray-500 text-center">终端手动登录</span>
            </button>
            <button
              onClick={() => setStep('label')}
              className="flex flex-col items-center gap-2 p-4 border border-gray-200 rounded-lg hover:border-gray-400 transition-colors cursor-pointer"
            >
              <span className="text-xl">📥</span>
              <span className="font-medium text-sm">导入本地缓存</span>
              <span className="text-xs text-gray-500 text-center">从 ~/.codex/ 导入</span>
            </button>
          </div>
        </div>
      )}

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
    </Modal>
  )
}