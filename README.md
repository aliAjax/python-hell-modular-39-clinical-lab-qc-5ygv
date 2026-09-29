# 临床实验室质量控制与结果拦截

只使用Python标准库和SQLite的模块化服务，默认端口`8339`。覆盖检测项目、质控品批次、质控规则、允许范围、仪器校准、连续偏差、趋势、失控、结果拦截、复测、调查、批次切换、历史更正，以及仪器故障或校准日期变动后的隔离与恢复。

## 模块结构

- `app.py`：参数解析、依赖组装和服务生命周期。
- `src/domain.py`：角色、领域异常和数据对象；`ConflictError`可携带结构化阻塞项。
- `src/rules.py`：QC规则计算、状态机、校准与放行约束、冻结单校验。
- `src/repository.py`：SQLite持久化、乐观锁、幂等、冻结单去重唯一索引和审计查询。
- `src/service.py`：用例编排、权限校验、版本控制和隔离/恢复saga。
- `src/http_api.py`：JSON接口和统一错误响应（含阻塞项详情）。
- `src/audit.py`：操作审计。
- `static/index.html`：最小演示页面。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8339
```

## 核心对象

`assay`为检测项目，`qc_lot`为质控品批次，`instrument`为仪器，`qc_run`为质控结果，`result_batch`为患者结果批次，`freeze_order`为冻结单。

## 隔离与恢复流程

仪器故障（`fail`）或校准日期变动（`calibrate`）后，系统自动为该仪器创建或复用一张未完成的冻结单（`freeze_order`），把旧质控结果作废、把尚未出科的患者结果批次拦截：

- 校准日期一变，该仪器原有的未作废质控结果立即置为`voided`，尚未出科的批次置为`intercepted`；已出科（`released`）批次保留原状。
- 同一仪器同一原因重复上报只保留一张冻结单（按`instrument_id`+`reason`去重，数据库唯一索引兜底）。
- 冻结处理按项持久化：每项为`pending`/`done`/`failed`。失败项保留错误并可通过`apply`重试；进程中断后再次`apply`只续做未完成项，已完成项不重复处理。
- 冻结期间放行批次返回`409`，响应`details`列出冻结单ID和阻塞项。
- 校准恢复后（`recover`或仪器`restore`），受影响批次回到`waiting`重新评估，已出科批次保留原状；旧质控结果保持作废，需用新质控结果重新放行。

冻结单接口：

- `POST /api/freeze_order`：上报冻结（`instrument_id`、`reason`），自动去重并处理。
- `POST /api/entities/<id>/actions`，`action`为`apply`：重试未完成项。
- `POST /api/entities/<id>/actions`，`action`为`recover`：校准恢复，受影响批次重新评估。

## 接口

- `GET /health`
- `GET /api/<kind>`，可用`?status=`过滤
- `GET /api/entities/<id>`
- `POST /api/<kind>`
- `POST /api/entities/<id>/actions`
- `GET /api/audit`

身份通过`X-User-Id`和`X-Role`请求头传入。可选`Idempotency-Key`防止重复创建。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

质控规则为可运行的简化模型，包含1-3s、连续偏移和趋势检查，但不替代CLIA、ISO 15189、Westgard完整规则集或实验室信息系统接口。
