# 临床实验室质量控制与结果拦截

只使用Python标准库和SQLite的模块化服务，默认端口`8339`。覆盖检测项目、质控品批次、质控规则、允许范围、仪器校准、连续偏差、趋势、失控、结果拦截、复测、调查、批次切换和历史更正。

## 模块结构

- `app.py`：参数解析、依赖组装和服务生命周期。
- `src/domain.py`：角色、领域异常和数据对象。
- `src/rules.py`：QC规则计算、状态机、校准与放行约束。
- `src/repository.py`：SQLite持久化、乐观锁、幂等和审计查询。
- `src/service.py`：用例编排、权限校验和版本控制。
- `src/http_api.py`：JSON接口和统一错误响应。
- `src/audit.py`：操作审计。
- `static/index.html`：最小演示页面。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8339
```

## 核心对象

`assay`为检测项目，`qc_lot`为质控品批次，`instrument`为仪器，`qc_run`为质控结果，`result_batch`为患者结果批次，`freeze_order`为隔离冻结单。

## 隔离与恢复流程

仪器故障（`fail`）或校准日期/证书变动（`calibrate`）会自动开出冻结单，手动上报走`POST /api/instruments/freeze`：

1. **立即失效**：该仪器当时仍为`accepted`的质控结果（`qc_run`）立即转为`invalidated`，不能再放行任何患者批次。
2. **拦截未出科批次**：该仪器尚在`waiting`的患者结果批次原子转为`intercepted`；已经`released`（已出科）的批次原样保留，记入`skipped`。
3. **同因去重**：同一仪器、同一`cause`+`cause_key`的重复上报只保留原冻结单（响应中`duplicate_report: true`）；仪器事件并入任何一张在途冻结单，不开第二张。
4. **冻结/放行并发**：放行在单个`BEGIN IMMEDIATE`事务内"校验+条件写入"，与冻结项互斥。后到一方按已冻结状态返回409，`details.state = "frozen"`并在`details.blockers`中列出冻结单、失效质控、被拦截批次等阻塞项；放行先提交时冻结项跳过该批次。
5. **失败保留与续做**：冻结单逐项处理、逐项落检查点；单项失败保留为`unfinished`（含错误信息），可对`POST /api/freeze_orders/<id>/resume`重试，已完成项不会重做；进程中断后用新进程对同库`resume`，只续做未完成项。
6. **校准恢复后重评**：仪器恢复`ready`且存在冻结之后的新`accepted`质控后，`POST /api/freeze_orders/<id>/recover`把受影响批次重新关联到新质控并放回`waiting`；没有新质控的批次继续阻塞、恢复可重试；已出科批次保持原状。

## 接口

- `GET /health`
- `GET /api/<kind>`，可用`?status=`过滤
- `GET /api/entities/<id>`
- `POST /api/<kind>`
- `POST /api/entities/<id>/actions`
- `POST /api/instruments/freeze`：上报仪器故障/校准事件（`instrument_id`、`reason`、`cause`、`cause_key`）
- `POST /api/freeze_orders/<id>/resume`：续做未完成隔离项
- `POST /api/freeze_orders/<id>/recover`：校准恢复后重评受影响批次（可带`fresh_qc_run_id`）
- `GET /api/audit`

身份通过`X-User-Id`和`X-Role`请求头传入。可选`Idempotency-Key`防止重复创建。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

质控规则为可运行的简化模型，包含1-3s、连续偏移和趋势检查，但不替代CLIA、ISO 15189、Westgard完整规则集或实验室信息系统接口。
