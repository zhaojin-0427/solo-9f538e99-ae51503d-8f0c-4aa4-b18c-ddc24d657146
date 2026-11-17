# 招标公告语料采集器（Tender Corpus Collector）

面向招标语料数据员的采集 API：提交多个官网公告 URL，后台按共享配额抓取，
提取 HTML 标题与正文，全部成功后提供完整 JSON 语料下载。**作业成功后还可发起
复核（review）**：按原始 URL 重新抓取每条公告，给出未变 / 改写 / 撤回结论，
帮助核实官网事后是否改写或撤回了公告。

技术栈：FastAPI + httpx（异步）+ SQLite（持久化），Docker Compose 一键启动
API 与两个可模拟**双向跳转、限流（429/503 + Retry-After）、慢响应**的本地站点。

## 快速开始

```bash
docker compose up --build
```

然后打开 <http://localhost:8000/docs>（Swagger UI），即可**采集、查询、下载、复核**：

1. `POST /jobs` 提交 1–20 个公告 URL（示例见下），返回 `job_id`；
2. `GET /jobs/{job_id}` 轮询进度（每条公告的状态/跳数/重试次数/错误）；
3. `GET /jobs/{job_id}/download` 在**全部成功**后下载完整 JSON 语料
   （有失败或未完成时返回 409，不提供半份结果）；
4. `GET /cooldowns` 查看当前生效的源冷却；`GET /jobs` 列出所有作业。

### 复核（重新核实公告是否被改写/撤回）

仅**完全成功**的作业可复核。复核按每条公告的**原始 URL** 重新抓取，与采集时
保存的标题/正文快照对比：

- `POST /jobs/{job_id}/reviews` 发起复核（请求体可省略；也可传
  `{"urls": [...]}` 只复核子集），返回 `review_id`；
- `GET /jobs/{job_id}/reviews` 列出该作业的全部复核批次；
- `GET /jobs/{job_id}/reviews/{review_id}` 查询复核进度与逐条结论；
- `GET /jobs/{job_id}/report` 读取**当前已发布**的复核报告（尚无报告时 404）。

结论只有四类：`unchanged`（未变）、`modified`（改写，含标题前后值与正文
unified diff）、`withdrawn`（撤回，**只有 404/410 算撤回**）、`error`
（限流耗尽、网络错误、跳转超限、解析失败等不确定情况——**不会改动旧报告**）。

复核的关键保证：

- 与采集**共用同一套**全局并发（2）、同源串行（1）、跳转（≤3 跳）、重试
  （≤2 次）与源冷却；复核触发的冷却同样延迟其他作业的同源请求；
- **整批复核全部到达终态后才在一个事务里原子发布**新报告；复核进行期间，
  原语料（download）与旧报告（report）照常可读，不存在半份报告；
- 同一作业可交错发起多次复核，每批带单调递增 `seq`；**先发起但更慢的旧复核
  不能覆盖后发布的新报告**，晚完成的旧批次标记为 `superseded`；
- 复核结果写入独立的 `review_items` 表，**永不修改**原 `announcements` 语料；
- 重启后自动恢复未完成的复核批次（未完成项从原始 URL 重新抓取），未发布的
  批次不会留下任何报告。

两个模拟站点：宿主机上 `http://localhost:8001`（site_a）、`http://localhost:8002`
（site_b）可浏览；**提交给 API 的 URL 请使用容器网络名**
`http://site_a:8000/...`、`http://site_b:8000/...`（API 容器内解析）。

### 可试的 URL（在 /docs 的 POST /jobs 里提交）

| URL | 行为 |
|---|---|
| `http://site_a:8000/article/1` | 普通公告（提取标题+正文） |
| `http://site_a:8000/to-peer/2` | 302 跳到 site_b 的公告（跨源 1 跳） |
| `http://site_a:8000/chain/3` | 双向跳转链，恰好 3 跳成功 |
| `http://site_a:8000/chain/4` | 第 4 跳超出上限 → 失败 |
| `http://site_a:8000/flaky/1?fails=2&ra=3` | 前 2 次 429 + `Retry-After: 3`，第 3 次成功 |
| `http://site_b:8000/busy` | 始终 503 + `Retry-After: 1` → 重试 2 次后失败 |
| `http://site_a:8000/slow/1?delay=3` | 慢响应（3 秒） |
| `http://site_a:8000/redirect-loop` | 双向死循环跳转 → 跳数上限失败 |
| `http://site_a:8000/mutable/1` | 可改写/撤回的公告：`POST /mutable/1/edit` 改写、`POST /mutable/1/withdraw?status=404|410` 撤回、`POST /mutable/1/blank` 变空白页、`POST /mutable/1/rate-limit` 变永久 429 |
| `http://site_a:8000/hold/1` | 复核专用：首次复核请求 429+Retry-After，该批次重试再延迟，用于交错复核测试 |

## 需求实现对照

| 要求 | 实现 |
|---|---|
| 提交至多 20 个 URL | `POST /jobs`，pydantic 校验 1–20 条、http/https、去重 |
| 提取标题和正文 | `api/app/extract.py`：stdlib `html.parser` 提取 `<title>` 与 `<body>` 文本（剔除 script/style） |
| 全局最多 2 个在途请求 | 全局 `asyncio.Semaphore(2)`，所有作业共享一个引擎 |
| 同源最多 1 个 | 每个源（scheme://host:port）一条串行"pump"协程 |
| 跳转按目标源排队 | 不自动跟随重定向；302 目标作为新任务投入**目标源**的队列 |
| 每条公告最多 3 跳 | `hops >= 3` 再遇 3xx 即失败（`too many redirects`） |
| 429/503 + 整数 Retry-After → 同源所有作业未发请求延后 | 设置源级冷却（内存 + SQLite），该源 pump 发送前等待 |
| 等待不占全局名额 | 冷却 `sleep` 发生在获取全局信号量**之前** |
| 其他源仍能采集 | 冷却是 per-origin 的，其他源的 pump 不受影响 |
| 每个 URL 最多重试 2 次 | `attempts` 计数（429/503/网络错误），跳转后的新 URL 重新计数 |
| 可查进度 | `GET /jobs/{id}`（逐条状态）、`GET /jobs`、`GET /cooldowns` |
| 重启保留冷却、继续未完成、成功不重取 | SQLite 持久化作业/公告/冷却；启动时恢复未完成的（pending/fetching），成功的不重取 |
| 全部成功才开放下载 | `GET /jobs/{id}/download`：非全成功返回 409 |
| 成功作业才能复核 | `POST /jobs/{id}/reviews`：有失败/未完成公告返回 409（无可信基线） |
| 按原始 URL 重新抓取 | 复核任务从 `announcements.url`（原始 URL）发起，重新走跳转链 |
| 共用配额/跳转/冷却 | 采集与复核任务进入同一引擎队列，共享全局信号量、同源 pump、冷却、跳转、重试 |
| 仅 404/410 算撤回 | 引擎 `WITHDRAWN_STATUSES = {404, 410}`；其他状态码不产生撤回结论 |
| 限流/网络/解析失败不改旧报告 | 这些情况记为复核项 `error`，不写撤回/改写结论，原语料与旧报告不变 |
| 整批完成才发布、无半份报告 | 全部复核项终态后，单事务 `publish_review_if_newest` 写报告；运行中 report 返回 404 |
| 复核期间旧数据照常可读 | 复核只写独立的 reviews/review_items 表，不触碰 announcements 与已发布报告 |
| 慢的旧复核不覆盖新报告 | 每作业复核 `seq` 单调递增；发布时若已有更新 seq 的报告，本批标记 `superseded` |
| 重启继续未完复核 | 启动时恢复 `running` 复核，未完成项重置后从原始 URL 重抓；未发布则无报告 |
| 复核报告含逐条结论与变化 | 报告 items 含 verdict、previous/current title、正文 unified diff、hops/attempts/error |
| /docs 可发起复核/查进度/读报告 | Swagger UI 暴露全部 review/report 端点；报告带 `Content-Disposition: attachment` |

## 目录结构

```
api/            采集 + 复核 API（FastAPI）
  app/main.py   端点与生命周期（启动时恢复冷却、未完成任务与未完成复核）
  app/engine.py 抓取引擎：全局/同源配额、冷却、跳转、重试；采集与复核共用
  app/db.py     SQLite 持久化（jobs / announcements / cooldowns /
                reviews / review_items / review_reports）
  app/diffing.py 复核对比：未变/改写判定、标题前后值与正文 unified diff
  app/extract.py HTML 标题正文提取
sites/          模拟招标站点（同一镜像以环境变量区分 site_a/site_b）
tests/          端到端自测脚本（本地进程模拟 compose 拓扑）
docker-compose.yml
```

## 自测

```bash
python3 tests/e2e_test.py                 # 全量：提取/跳转/限流/配额/下载门控/重启持久化
python3 tests/cross_job_cooldown_test.py  # 专项：跨作业共享冷却
python3 tests/review_e2e_test.py          # 复核：未变/改写/撤回/错误门控、整批原子发布、
                                          #       交错复核 seq 仲裁、重启恢复、共享冷却/跳转
```

## 已知限制

- "源"按 `scheme://host:port` 判定；同一服务器的不同端口/主机名视为不同源。
- 失败（重试耗尽）是终态，重启不会自动重试失败公告；需重新提交作业。
- 提取器面向常规 HTML 公告页，不执行 JavaScript。
- 复核的 `error`（限流耗尽/网络/解析失败）是该批复核的终态：本批报告中标记为
  不确定项，不会自动无限重试；需要时对该作业再发起一次新复核即可。
- 复核对比基于提取后的纯文本标题与正文；HTML 标签层面但不影响可见文本的变化
  （如属性调整）不会判为改写。
