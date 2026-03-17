# TODO List

## Claude Code `Error writing file` 问题跟踪

### 背景

项目当前同时支持：

- `8080` 主代理：Codex / OpenAI / Anthropic 混合转换链路
- `8521` AI 代理：面向 OpenAI 兼容上游，同时兼容 OpenAI 与 Anthropic 请求

在 Claude Code 使用文件编辑相关工具时，容易出现：

- `Error writing file`

这个问题不是单一端口问题，`8080` 和 `8521` 都会受影响，因为根因位于共享的协议转换层。

### 已确认根因

#### 1. 工具参数在多层协议之间反复字符串化

高风险链路：

- Claude `tool_use.input`
- Codex / Responses `function_call.arguments`
- OpenAI Chat Completions `tool_calls[].function.arguments`
- 再反向转回 Claude `tool_use.input`

文件编辑类工具参数通常包含：

- 多行文本
- patch 内容
- 引号和反斜杠
- 大段替换文本
- 路径和精确匹配片段

这类参数在多次字符串化后极易损坏。

#### 2. 历史实现会把非法参数静默降级成 `{}`

这会导致：

- `path`
- `content`
- `old_string`
- `new_string`
- `patch`

等关键字段丢失，最终在 Claude Code 侧表现为 `Error writing file`。

#### 3. 流式工具参数曾经按半截 JSON 直接转发

历史流式逻辑是：

- 收到一点 `arguments delta`
- 立即转成 Claude 的 `input_json_delta`
- 继续拼接

这对文件工具非常脆弱，因为 Claude 侧可能在参数尚未完整时就进入工具调用流程。

### 已完成修复

#### 第一层修复：去掉静默吞参

已修改共享转换层：

- 请求路径不再把非法 `arguments` 自动改成 `{}`
- 响应路径不再把解析失败的工具参数默认回退为空对象

当前策略：

- 尽量保留原始参数
- 非流式回转 Claude 时，如果工具参数 JSON 非法，则显式报错

目的：

- 不再“悄悄把文件工具参数吞掉”
- 让问题显式暴露，便于后续定位

#### 第二层修复：流式参数缓冲

已修改共享流式转换层：

- `Codex -> Claude` 流式工具参数：先缓冲，再一次性发送完整 `input_json_delta`
- `OpenAI Chat -> Claude` 流式工具参数：按 `tool_index` 累积，结束时一次性发出

目的：

- 避免半截 JSON 被 Claude Code 当成完整工具参数
- 降低文件工具在流式场景下的失败率

#### 第三层修复：工具参数 schema 深度规范化

已修改共享 schema 转换层：

- 不再只做顶层 `type/object/properties` 的浅处理
- 现在会递归规范化：
  - `type: ["string", "null"]` 这类联合类型
  - 嵌套 `properties`
  - `items`
  - `anyOf` / `oneOf` / `allOf`
  - 非法 `required`
  - 非对象 / 布尔 schema
- Claude -> Responses 和 Responses -> Chat 两条链路都统一走同一个 `normalize_tool_parameters`

目的：

- 修复 Claude Terminal 常见的 `Invalid tool parameters`
- 避免同一份工具 schema 在二次转换后再次变形

#### 第四层修复：参考 CLIProxyAPI 补齐工具参数 JSON 修复

已对齐 `CLIProxyAPI` 的关键思路：

- 在工具参数链路增加 `repair_tool_arguments_json`
- 对非标准 JSON 做单引号修复
- 统一要求最终下发给 Claude 的工具参数必须是 JSON object
- 流式 `input_json_delta` 在发出前也先做修复，而不是原样透传

覆盖位置：

- `function_call.arguments` 进入共享转换层时
- 非流式 `Responses/Codex -> Claude` 时
- 流式 `Codex -> Claude` 时
- 流式 `OpenAI Chat -> Claude` 时

目的：

- 进一步降低 Claude Terminal `Read/Edit file` 因参数 JSON 轻微损坏而失败的概率
- 与 `CLIProxyAPI` 的稳定处理方式保持一致

### 当前状态

已完成且通过编译：

- `cargo check -q`

相关已改文件包括：

- `src-tauri/src/lib.rs`

### 后续待做

#### P0：为流式工具参数增加“完整 JSON 校验”

当前流式缓冲只做到“等完整后再发”，但还没有做到：

- 在发给 Claude 前，强校验拼接后的 `arguments` 是否为合法 JSON

建议方案：

1. 对每个流式 tool call 缓冲完整字符串
2. 在 `output_item.done` 或流结束时执行 JSON 校验
3. 如果合法：
   - 正常发出 `input_json_delta`
4. 如果不合法：
   - 记录错误日志
   - 返回明确协议转换错误
   - 不再把损坏参数送给 Claude

这样可以进一步消灭“参数虽然完整，但其实已经坏了”的情况。

#### P0：增加专项请求日志

需要新增更强的调试日志，用于后续彻底验证：

- 原始 tool arguments
- 缓冲后的 tool arguments
- 转回 Claude 前的最终 tool input
- 出错时关联 request id / tool index / tool name

建议新增日志字段：

- `tool_name`
- `tool_index`
- `tool_call_id`
- `raw_arguments`
- `normalized_arguments`
- `tool_arguments_parse_error`

#### P1：为文件工具建立专项测试

建议补以下测试用例：

1. `write_file(path, content)`，内容为多行文本
2. `str_replace_editor(path, old_string, new_string)`，带引号和反斜杠
3. `apply_patch(input)`，内容为大 patch
4. 流式 `tool_calls.arguments` 分片拼接
5. 非法 JSON 参数必须显式失败，不能回退 `{}`

目标：

- 避免后续改动重新引入 `Error writing file`

#### P1：将 Anthropic 协议做成独立转换链

当前 `8521` 的 Anthropic 兼容仍然存在多层中转：

- Anthropic -> Codex Responses -> Chat Completions -> Provider

更稳妥的最终方案是：

- Anthropic `/v1/messages` 直接映射到 OpenAI Chat Completions
- 尽量减少中间协议层数
- 工具参数只在最后一步才转成 OpenAI 所需字符串

这样可以进一步降低文件工具损坏概率。

### 需要特别注意的原则

后续继续修复时，必须坚持：

1. 不要再把非法工具参数静默改成 `{}`
2. 不要把半截 JSON 直接发给 Claude
3. 优先做“显式失败 + 可观测日志”，不要做“模糊兼容”
4. 每次改动后都要重新验证 `8080` 和 `8521`

### 建议下次继续时的优先顺序

1. 流式工具参数完整 JSON 校验
2. 增加工具参数专项日志
3. 补文件工具测试
4. 再考虑把 Anthropic 协议改成独立直转链
