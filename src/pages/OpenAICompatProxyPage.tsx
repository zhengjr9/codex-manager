import { useEffect, useMemo, useRef, useState } from 'react'
import {
  Button,
  Card,
  Empty,
  Form,
  Input,
  InputNumber,
  Modal,
  Popconfirm,
  Select,
  Space,
  Switch,
  Table,
  Tag,
  Typography,
  message,
} from 'antd'
import {
  ApiOutlined,
  DeleteOutlined,
  EditOutlined,
  PlusOutlined,
  ReloadOutlined,
} from '@ant-design/icons'
import {
  accountService,
  type OpenAICompatConfig,
  type OpenAICompatModelMapping,
  type OpenAICompatProxyStatus,
} from '../services/accountService'

const { Text, Title } = Typography

interface CompatDraft {
  id?: string
  provider_name: string
  base_url: string
  api_key: string
  default_model: string
  model_mappings: OpenAICompatModelMapping[]
}

const emptyDraft = (): CompatDraft => ({
  provider_name: '',
  base_url: '',
  api_key: '',
  default_model: '',
  model_mappings: [{ alias: 'glm5', provider_model: 'glm-5' }],
})

export default function OpenAICompatProxyPage() {
  const [configs, setConfigs] = useState<OpenAICompatConfig[]>([])
  const [status, setStatus] = useState<OpenAICompatProxyStatus>({ running: false, port: 8081, config_id: null, provider_name: null })
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [port, setPort] = useState(8081)
  const [loading, setLoading] = useState(false)
  const [saving, setSaving] = useState(false)
  const [modalOpen, setModalOpen] = useState(false)
  const [draft, setDraft] = useState<CompatDraft>(emptyDraft())
  const [saveError, setSaveError] = useState<string | null>(null)
  const [providerModels, setProviderModels] = useState<string[]>([])
  const [providerModelsLoading, setProviderModelsLoading] = useState(false)
  const detailCardRef = useRef<HTMLDivElement | null>(null)
  const activeSelectedId = selectedId ?? status.config_id ?? null

  const selectedConfig = useMemo(
    () => configs.find(item => item.id === (activeSelectedId ?? '')) ?? null,
    [configs, activeSelectedId],
  )

  function handleSelectConfig(id: string) {
    setSelectedId(id)
    requestAnimationFrame(() => {
      detailCardRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' })
    })
  }

  async function refresh() {
    setLoading(true)
    try {
      const [configList, proxyStatus] = await Promise.all([
        accountService.listOpenAICompatConfigs(),
        accountService.getOpenAICompatProxyStatus(),
      ])
      setConfigs(configList)
      setStatus(proxyStatus)
      if (proxyStatus.port) {
        setPort(proxyStatus.port)
      }
      if (proxyStatus.config_id) {
        setSelectedId(proxyStatus.config_id)
      } else if (!selectedId && configList[0]) {
        setSelectedId(configList[0].id)
      }
    } catch (e) {
      message.error(String(e))
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    refresh()
  }, [])

  useEffect(() => {
    if (!selectedConfig) {
      setProviderModels([])
      return
    }
    setProviderModelsLoading(true)
    accountService.listOpenAICompatProviderModels(selectedConfig.id)
      .then(setProviderModels)
      .catch(() => setProviderModels([]))
      .finally(() => setProviderModelsLoading(false))
  }, [selectedConfig?.id])

  function openCreateModal() {
    setDraft(emptyDraft())
    setSaveError(null)
    setModalOpen(true)
  }

  function openEditModal(config: OpenAICompatConfig) {
    setDraft({
      id: config.id,
      provider_name: config.provider_name,
      base_url: config.base_url,
      api_key: config.api_key,
      default_model: config.default_model ?? '',
      model_mappings: config.model_mappings.length > 0 ? config.model_mappings : [{ alias: '', provider_model: '' }],
    })
    setSaveError(null)
    setModalOpen(true)
  }

  async function saveDraft() {
    if (!draft.provider_name.trim()) {
      setSaveError('请填写 Provider 命名')
      message.error('请填写 Provider 命名')
      return
    }
    if (!draft.base_url.trim()) {
      setSaveError('请填写兼容地址')
      message.error('请填写兼容地址')
      return
    }
    if (!draft.api_key.trim()) {
      setSaveError('请填写兼容 API Key')
      message.error('请填写兼容 API Key')
      return
    }
    setSaveError(null)
    setSaving(true)
    try {
      const payload = {
        provider_name: draft.provider_name,
        base_url: draft.base_url,
        api_key: draft.api_key,
        default_model: draft.default_model || null,
        model_mappings: draft.model_mappings.filter(item => item.alias.trim() && item.provider_model.trim()),
      }
      const result = draft.id
        ? await accountService.updateOpenAICompatConfig({ id: draft.id, ...payload })
        : await accountService.createOpenAICompatConfig(payload)
      setModalOpen(false)
      setSelectedId(result.id)
      await refresh()
      setSaveError(null)
      message.success(draft.id ? '配置已更新' : '配置已创建')
    } catch (e) {
      console.error('save openai compat config failed', e)
      const errorText = e instanceof Error ? e.message : String(e)
      setSaveError(errorText)
      message.error(errorText)
    } finally {
      setSaving(false)
    }
  }

  async function toggleProxy(checked: boolean) {
    try {
      if (checked) {
        const configId = activeSelectedId ?? configs[0]?.id
        if (!configId) {
          throw new Error('请先创建一个 Provider 配置')
        }
        await accountService.startOpenAICompatProxy(configId, port)
        message.success(`OpenAI 兼容代理已启动 (端口 ${port})`)
      } else {
        await accountService.stopOpenAICompatProxy()
        message.info('OpenAI 兼容代理已停止')
      }
      await refresh()
    } catch (e) {
      message.error(String(e))
    }
  }

  async function deleteConfig(id: string) {
    try {
      await accountService.deleteOpenAICompatConfig(id)
      if (selectedId === id) {
        setSelectedId(null)
      }
      await refresh()
      message.success('配置已删除')
    } catch (e) {
      message.error(String(e))
    }
  }

  const columns = [
    {
      title: 'Provider',
      dataIndex: 'provider_name',
      render: (value: string, record: OpenAICompatConfig) => (
        <Space direction="vertical" size={2}>
          <Text strong>{value}</Text>
          <Text type="secondary" className="text-xs">{record.base_url}</Text>
        </Space>
      ),
    },
    {
      title: '默认模型',
      dataIndex: 'default_model',
      width: 140,
      render: (value: string | null) => value ? <Tag>{value}</Tag> : <Text type="secondary">--</Text>,
    },
    {
      title: '映射',
      width: 220,
      render: (_: unknown, record: OpenAICompatConfig) => (
        <Space wrap size={[4, 4]}>
          {record.model_mappings.length > 0
            ? record.model_mappings.slice(0, 3).map(item => (
              <Tag key={`${record.id}-${item.alias}`}>{item.alias} {'->'} {item.provider_model}</Tag>
            ))
            : <Text type="secondary">--</Text>}
        </Space>
      ),
    },
    {
      title: '操作',
      width: 210,
      render: (_: unknown, record: OpenAICompatConfig) => (
        <Space>
          <Button size="small" type={activeSelectedId === record.id ? 'primary' : 'default'} onClick={() => handleSelectConfig(record.id)}>
            查看
          </Button>
          <Button size="small" icon={<EditOutlined />} onClick={() => openEditModal(record)}>
            编辑
          </Button>
          <Popconfirm title="确认删除该配置？" onConfirm={() => deleteConfig(record.id)}>
            <Button size="small" danger icon={<DeleteOutlined />} />
          </Popconfirm>
        </Space>
      ),
    },
  ]

  return (
    <div className="space-y-6 max-w-6xl mx-auto pb-12">
      <Card
        size="small"
        className={status.running ? 'border-green-300 bg-green-50 shadow-sm' : 'border-gray-200 shadow-sm'}
        title={
          <Space>
            <ApiOutlined className={status.running ? 'text-green-600' : 'text-gray-400'} />
            <span className="font-semibold text-gray-800">OpenAI 兼容代理</span>
          </Space>
        }
        extra={
          <Space>
            <Select
              placeholder="选择 Provider"
              value={activeSelectedId ?? undefined}
              onChange={handleSelectConfig}
              className="w-64"
              options={configs.map(item => ({ label: item.provider_name, value: item.id }))}
              disabled={status.running}
            />
            <InputNumber
              min={1024}
              max={65535}
              value={port}
              onChange={(value) => setPort(value || 8081)}
              disabled={status.running}
            />
            <Switch
              checked={status.running}
              onChange={toggleProxy}
              checkedChildren="运行中"
              unCheckedChildren="已停止"
            />
          </Space>
        }
      >
        <div className="flex items-center justify-between gap-4">
          <div>
            <Text>独立端口默认 `8081`。此代理专门把 Codex 的 `Responses` 协议转换成 OpenAI 兼容上游的 `Chat Completions`。</Text>
            <div className="mt-2 text-xs text-gray-500">
              当前运行: {status.running ? `${status.provider_name ?? '--'} @ http://127.0.0.1:${status.port}` : '未启动'}
            </div>
          </div>
          <Space>
            <Button icon={<ReloadOutlined />} onClick={refresh} loading={loading}>刷新</Button>
            <Button type="primary" icon={<PlusOutlined />} onClick={openCreateModal}>新建配置</Button>
          </Space>
        </div>
      </Card>

      <Card
        size="small"
        title="Provider 配置"
        className="border-gray-200 shadow-sm"
      >
        <Table
          rowKey="id"
          loading={loading}
          columns={columns}
          dataSource={configs}
          onRow={(record) => ({
            onClick: () => handleSelectConfig(record.id),
          })}
          rowClassName={(record) => (activeSelectedId === record.id ? 'bg-blue-50' : '')}
          pagination={{ pageSize: 8, hideOnSinglePage: true }}
          locale={{ emptyText: <Empty description="暂无 Provider 配置" /> }}
        />
      </Card>

      <Card
        ref={detailCardRef}
        size="small"
        title="配置查看"
        className="border-gray-200 shadow-sm"
      >
        {selectedConfig ? (
          <div className="space-y-4">
            <div className="flex items-start justify-between gap-4">
              <div>
                <Title level={5} style={{ margin: 0 }}>{selectedConfig.provider_name}</Title>
                <Text type="secondary">{selectedConfig.base_url}</Text>
              </div>
              <Space>
                <Button icon={<ReloadOutlined />} loading={providerModelsLoading} onClick={() => selectedConfig && accountService.listOpenAICompatProviderModels(selectedConfig.id).then(setProviderModels).catch((e) => message.error(String(e)))}>
                  刷新模型
                </Button>
                <Button icon={<EditOutlined />} onClick={() => openEditModal(selectedConfig)}>
                  编辑当前配置
                </Button>
              </Space>
            </div>

            <div>
              <Text strong>默认模型</Text>
              <div className="mt-2">
                {selectedConfig.default_model ? <Tag>{selectedConfig.default_model}</Tag> : <Text type="secondary">未设置</Text>}
              </div>
            </div>

            <div>
              <Text strong>模型映射</Text>
              <div className="mt-2 flex flex-wrap gap-2">
                {selectedConfig.model_mappings.length > 0
                  ? selectedConfig.model_mappings.map(item => (
                    <Tag key={`${selectedConfig.id}-${item.alias}`}>{item.alias} {'->'} {item.provider_model}</Tag>
                  ))
                  : <Text type="secondary">未配置模型映射</Text>}
              </div>
            </div>

            <div>
              <Text strong>Provider 暴露模型</Text>
              <div className="mt-2 flex flex-wrap gap-2">
                {providerModels.length > 0
                  ? providerModels.map(item => <Tag color="blue" key={item}>{item}</Tag>)
                  : <Text type="secondary">{providerModelsLoading ? '加载中...' : '暂无数据'}</Text>}
              </div>
            </div>
          </div>
        ) : (
          <Empty description="请选择或创建一个 Provider 配置" />
        )}
      </Card>

      <Modal
        title={draft.id ? '编辑 Provider 配置' : '新建 Provider 配置'}
        open={modalOpen}
        onCancel={() => setModalOpen(false)}
        onOk={saveDraft}
        confirmLoading={saving}
        destroyOnClose
        width={760}
      >
        <Form layout="vertical">
          {saveError ? <Text type="danger">{saveError}</Text> : null}
          <Form.Item label="Provider 命名" required>
            <Input
              placeholder="例如 Midea GLM"
              value={draft.provider_name}
              onChange={(e) => setDraft({ ...draft, provider_name: e.target.value })}
            />
          </Form.Item>
          <Form.Item label="兼容地址" required>
            <Input
              placeholder="https://example.com/v1"
              value={draft.base_url}
              onChange={(e) => setDraft({ ...draft, base_url: e.target.value })}
            />
          </Form.Item>
          <Form.Item label="兼容 API Key" required>
            <Input.Password
              placeholder="sk-..."
              value={draft.api_key}
              onChange={(e) => setDraft({ ...draft, api_key: e.target.value })}
            />
          </Form.Item>
          <Form.Item label="默认模型">
            <Input
              placeholder="例如 glm5"
              value={draft.default_model}
              onChange={(e) => setDraft({ ...draft, default_model: e.target.value })}
            />
          </Form.Item>
          <Form.Item label="模型映射">
            <div className="space-y-2">
              {draft.model_mappings.map((mapping, index) => (
                <Space key={`mapping-${index}`} className="flex">
                  <Input
                    placeholder="对外模型名，例如 glm5"
                    value={mapping.alias}
                    onChange={(e) => {
                      const next = [...draft.model_mappings]
                      next[index] = { ...next[index], alias: e.target.value }
                      setDraft({ ...draft, model_mappings: next })
                    }}
                  />
                  <Input
                    placeholder="Provider 模型名，例如 glm-5"
                    value={mapping.provider_model}
                    onChange={(e) => {
                      const next = [...draft.model_mappings]
                      next[index] = { ...next[index], provider_model: e.target.value }
                      setDraft({ ...draft, model_mappings: next })
                    }}
                  />
                  <Button
                    danger
                    onClick={() => {
                      const next = draft.model_mappings.filter((_, i) => i !== index)
                      setDraft({ ...draft, model_mappings: next.length > 0 ? next : [{ alias: '', provider_model: '' }] })
                    }}
                  >
                    删除
                  </Button>
                </Space>
              ))}
              <Button
                icon={<PlusOutlined />}
                onClick={() => setDraft({ ...draft, model_mappings: [...draft.model_mappings, { alias: '', provider_model: '' }] })}
              >
                添加映射
              </Button>
            </div>
          </Form.Item>
        </Form>
      </Modal>
    </div>
  )
}
