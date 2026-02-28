import { Tag } from 'antd'

const PLAN_COLORS: Record<string, string> = {
  free: 'default',
  plus: 'blue',
  pro: 'purple',
  ultra: 'gold',
}

const PLAN_LABELS: Record<string, string> = {
  free: 'Free',
  plus: 'Plus',
  pro: 'Pro',
  ultra: 'Ultra',
}

interface Props {
  plan: string
}

export default function PlanBadge({ plan }: Props) {
  const key = plan.toLowerCase()
  return (
    <Tag color={PLAN_COLORS[key] || 'default'}>
      {PLAN_LABELS[key] || plan}
    </Tag>
  )
}
