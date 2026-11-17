# 账号强制断开接口（v0.2.22-drainfix）

本分支在官方 v0.2.22 上保留来自 v0.2.21-drainfix 的两个补丁，以下强断接口的请求、返回及范围均保持不变。原有 `POST /api/v1/admin/accounts/:id/schedulable` 仍然只停止新请求和 WS 后续轮次，允许已经准入的工作完成。

## 调用

```sh
curl --request POST "$SUB2API_URL/api/v1/admin/accounts/$ACCOUNT_ID/disconnect" \
  --header "x-api-key: $SUB2API_ADMIN_KEY" \
  --header 'Content-Type: application/json' \
  --max-time 45 \
  --data '{"wait_timeout_ms":5000,"reason":"candy_periodic_test_failed"}'
```

认证沿用现有管理员认证：全权限管理员 API Key（`x-api-key`）或管理员 JWT（`Authorization: Bearer ...`）。普通用户 API Key 无此权限；本次没有为 `adm_sc_` 受限管理员密钥扩展授权目录。

- 账号 ID 必须为正整数，且账号平台为 OpenAI（OAuth/API Key）。不存在返回 404，其他平台返回 400。
- 请求体可省略，或使用单个 JSON 对象。`wait_timeout_ms` 默认 5000，范围 0–30000；0 表示发出取消后立即检查一次。
- `reason` 可省略，最多 256 UTF-8 字节，仅用于审计和日志，不参与自动判断。
- `wait_timeout_ms` 是发出取消后的清理等待预算，不包含前面的数据库更新，也不打断正在执行的状态读取。客户端应留出数据库/Redis 调用时间。

## 执行顺序

1. 在当前进程中暂时拦住这个账号的新登记，同时将数据库 `schedulable` 设为 `false`。
2. 向此账号已登记的 HTTP 请求、Responses WebSocket 会话发出专用取消信号；中断相应 HTTP 响应写入和下游 WS 连接。
3. 强制关闭此账号的上游 WS 连接池连接，同时取消后台预热建连，丢弃旧的预热状态；该账号正在进行的拨号不能重新进入旧一代连接池。
4. 等待转发函数实际退出、连接中断回调完成、连接池及进行中拨号清理，并读取真实的账号并发计数。只有本进程登记项为零、池中连接和进行中拨号为零、并发计数为零、数据库仍关闭调度，才返回 `drained: true`。

重复调用同一个账号是安全的。已关闭调度的账号可以直接调用；接口不会自动重新开启调度，不改账号凭证，不切账号，不直接删除 Redis 并发键，也不伪造计数为零。

普通客户端断线后的计费尾流仍沿用原来的有限等待规则；管理员强制取消能够穿透这层脱离客户端的上下文，立即停止本地上游读写。已收集的用量仍走原来的记录路径；强制中断可能无法取得上游最终 usage，也不保证上游供应商已经停止远端计算。

## 返回与 multi 的判定

清空后返回 HTTP 200，例如：

```json
{
  "code": 0,
  "message": "success",
  "data": {
    "account_id": 5579,
    "schedulable": false,
    "scope": "instance_openai_gateway",
    "cancellation_requested": {"http": 2, "websockets": 1},
    "remaining": {"http": 0, "websockets": 0},
    "closed_upstream_connections": 1,
    "remaining_upstream_connections": 0,
    "current_concurrency": 0,
    "drained": true,
    "timed_out": false
  }
}
```

`cancellation_requested` 是本次取消快照中的登记项数量，包含已经收到取消但尚未退出的项；不是成功关闭数量。`remaining` 是当前进程尚未完成清理的请求/会话数；HTTP 登记也包含已选中该账号、正在等待并发槽的请求。WS 会话整个存活期都会登记，不能因为该轮结束、Redis 并发已归零就漏掉空闲旧连接。

| HTTP 状态 | 含义 | multi 后续行为 |
| --- | --- | --- |
| 200 且 `data.drained == true` | 已确认此范围内清空 | 单实例部署可以继续切号 |
| 202 | 已发出取消，仍有工作或并发残留 | 保持关闭调度，稍后重复调用；不要切号 |
| 503 且带 `data` | 已取消，但无法验证清空，如 Redis 读取失败或账号被同时重新启用 | 保持关闭并检查原因；不要把失败读数当成零 |
| 400 / 404 / 500 等 | 参数、账号、数据库等错误 | 不认为清空成功 |

`remaining_upstream_connections` 包含当前进程池中连接和进行中的建连预留，避免后台预热尚未退出就确认清空；`closed_upstream_connections` 只统计本次从池内取出并关闭的连接。

202 的外层 `code` 也是 0，因此 **不能只看 `code == 0` 或“HTTP 是 2xx”**；必须确认 `data.drained == true`，并检查调用成功。

建议 multi 的正常额度阈值轮换仍走关闭调度、等待完成；只有明确异常且决定中止当前工作的场景才调用本接口。本次没有修改 multi 的分类或调用逻辑。

## 范围

- 当前 Sub2API 进程内的 OpenAI 网关 HTTP 转发，包括 Responses（含 compact 和路径别名）、Messages、Chat Completions、Embeddings、Images（含 multipart 编辑和已经执行的异步图片任务）、Alpha Search、上游 token 计数请求。异步任务的 HTTP 提交连接和任务执行连接分别管理，取消任务不会中断已经提交完成的 HTTP 连接。
- Responses WS 的首轮、正在进行的轮次、两轮之间的空闲连接，以及对应上游 WS 池。断开不会等待用户再发下一轮。
- 目标是该账号的请求/会话。HTTP/2 按流中止，避免切断共享 TCP 连接上的其他账号请求。HTTP 空闲 keep-alive TCP 连接本身不代表某个账号的在途请求。
- 不覆盖其他平台的网关、管理员账号测试/OAuth 刷新/用量探测、WebRTC 媒体会话或另一个 Sub2API 进程。未新增前端按钮、数据库迁移或跨实例广播。
- **多实例部署不能只请求负载均衡地址一次。** 必须对每个持有连接的实例执行断开并确认；共享 Redis 的并发数可以暴露其他实例的在途轮次，但不代表其他实例没有空闲 WS。因此返回明确标记 `scope: instance_openai_gateway`。
- multi 切换完成前不要有其他任务重新开启此账号调度。本接口在结束前复核调度状态，但不能阻止接口返回后的另一次显式启用。

## 本地验证命令

在 `backend` 目录运行（需要仓库指定的 Go 版本）：

```sh
go test -tags unit ./internal/pkg/httputil ./internal/service ./internal/handler ./internal/handler/admin \
  -run 'Test(AccountDisconnect|AccountRequestRegistry|Force|OpenAIWSConnPoolAbortAccount|CheckAccountScheduling|AccountSchedulingAdmission|OpenAIAccountAdmission_|OpenAIResponsesWebSocket_DisableDrains)' -count=1

go test -race -tags unit ./internal/pkg/httputil ./internal/service ./internal/handler ./internal/handler/admin \
  -run 'Test(AccountDisconnect|AccountRequestRegistry|Force|OpenAIWSConnPoolAbortAccount)' -count=1
```

本分支 2026-11-17 的实际移植验证结果见 [V0.2.22_PORT_VALIDATION.md](V0.2.22_PORT_VALIDATION.md)。[FORK_NOTES.md](FORK_NOTES.md) 中的 2026-11-14 构建记录和 `.3-local` 二进制属于旧 v0.2.21 分支，不能作为 v0.2.22 的发布产物；旧 `.2-local` 还缺少最后一轮取消补强。原始测试日志和构建文件仅保存在本地，不随源码发布。
