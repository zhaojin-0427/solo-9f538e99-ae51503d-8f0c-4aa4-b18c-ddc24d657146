# 招标公告语料采集器（Tender Corpus Collector）

面向招标语料数据员的采集 API：提交多个官网公告 URL，后台按共享配额抓取，
提取 HTML 标题与正文，全部成功后提供完整 JSON 语料下载。

技术栈：FastAPI + httpx（异步）+ SQLite（持久化），Docker Compose 一键启动
API 与两个可模拟**双向跳转、限流（429/503 + Retry-After）、慢响应**的本地站点。

## 快速开始

```bash
docker compose up --build
```

然后打开 <http://localhost:8000/docs>（Swagger UI），即可**采集、查询、下载**：

1. `POST /jobs` 提交 1–20 个公告 URL（示例见下），返回 `job_id`；
2. `GET /jobs/{job_id}` 轮询进度（每条公告的状态/跳数/重试次数/错误）；
3. `GET /jobs/{job_id}/download` 在**全部成功**后下载完整 JSON 语料
   （有失败或未完成时返回 409，不提供半份结果）；
4. `GET /cooldowns` 查看当前生效的源冷却；`GET /jobs` 列出所有作业。

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
| /docs 可采集、查询、下载 | Swagger UI 暴露全部端点；下载带 `Content-Disposition: attachment` |

## 目录结构

```
api/            采集 API（FastAPI）
  app/main.py   端点与生命周期（启动时恢复冷却与未完成任务）
  app/engine.py 抓取引擎：全局/同源配额、冷却、跳转、重试
  app/db.py     SQLite 持久化（jobs / announcements / cooldowns）
  app/extract.py HTML 标题正文提取
sites/          模拟招标站点（同一镜像以环境变量区分 site_a/site_b）
tests/          端到端自测脚本（本地进程模拟 compose 拓扑）
docker-compose.yml
```

## 自测

```bash
python3 tests/e2e_test.py                 # 全量：提取/跳转/限流/配额/下载门控/重启持久化
python3 tests/cross_job_cooldown_test.py  # 专项：跨作业共享冷却
```

## 已知限制

- "源"按 `scheme://host:port` 判定；同一服务器的不同端口/主机名视为不同源。
- 失败（重试耗尽）是终态，重启不会自动重试失败公告；需重新提交作业。
- 提取器面向常规 HTML 公告页，不执行 JavaScript。
