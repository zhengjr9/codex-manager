import { useState } from 'react'
import { Modal, Input, message } from 'antd'
import { EditOutlined } from '@ant-design/icons'
import { useAccountStore } from '../../stores/useAccountStore'
import type { CodexAccount } from '../../types/account'

interface Props {
  account: CodexAccount
}

export default function AccountLabelEditor({ account }: Props) {
  const [open, setOpen] = useState(false)
  const [value, setValue] = useState(account.label || '')
  const [loading, setLoading] = useState(false)
  const updateLabel = useAccountStore(s => s.updateLabel)

  async function handleSave() {
    setLoading(true)
    try {
      await updateLabel(account.id, value)
      message.success('备注已更新')
      setOpen(false)
    } catch (e) {
      message.error(String(e))
    } finally {
      setLoading(false)
    }
  }

  return (
    <>
      <button
        onClick={() => { setValue(account.label || ''); setOpen(true) }}
        className="text-gray-400 hover:text-indigo-500 transition-colors"
        title="编辑备注"
      >
        <EditOutlined />
      </button>
      <Modal
        title="编辑账号备注"
        open={open}
        onOk={handleSave}
        onCancel={() => setOpen(false)}
        confirmLoading={loading}
        okText="保存"
        cancelText="取消"
        width={360}
      >
        <Input
          value={value}
          onChange={e => setValue(e.target.value)}
          placeholder="输入备注名称..."
          onPressEnter={handleSave}
          className="mt-2"
        />
      </Modal>
    </>
  )
}
