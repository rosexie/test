# 同步任务元数据管理平台实施方案

## 1. 项目背景

当前实时同步链路为：

```text
Oracle -> OGG -> Kafka -> Spark Streaming -> Hive / HBase / Kafka / 其他目标
```

现状问题：

- 同步任务配置分散在手工配置、Airflow 参数、旧配置文件、Oracle 维表和 Spark 定制类中。
- 管理侧无法清楚知道同步了哪些数据、同步到哪里、谁负责、当前是否运行正常。
- 缺少统一的发布版本、回滚、补数、Kafka lag、错误和字段质量观测。

目标是建设一个“同步任务元数据管理平台”，将同步任务抽象成可管理、可发布、可观测、可追溯的数据资产。

## 2. 平台职责边界

平台负责：

- 同步任务定义
- 字段映射
- 目标端配置
- 处理策略
- 发布版本
- SyncMetadata snapshot 生成
- Airflow 触发
- 运行状态展示
- Kafka lag 展示
- 错误记录和字段质量
- 回滚和补数入口
- 现有任务导入

Airflow 负责：

- Spark Streaming 任务提交
- 任务重启、停止、补数 DAG 编排
- DAG Run 记录

Spark Streaming 负责：

- 消费 Kafka
- 解析数据
- 写入 Hive / HBase / Kafka 等目标
- 上报 batch、offset、错误、字段质量指标

## 3. MVP 总体链路

```text
创建任务元数据
  -> 配置字段映射和目标端
  -> validate
  -> generate snapshot
  -> create release
  -> trigger Airflow DAG
  -> spark-submit
  -> Spark 从 snapshot 启动
  -> 运行状态回写
  -> 页面展示 runtime / lag / error / quality
```

## 4. 核心数据模型

MVP 需要建立以下表：

- SYNC_TASK：同步任务主表
- SYNC_FIELD_MAPPING：字段映射
- SYNC_TARGET_CONFIG：目标端配置
- SYNC_STRATEGY：处理策略
- SYNC_RELEASE：发布版本和 snapshot
- SYNC_JOB_INSTANCE：运行实例
- SYNC_TASK_RUNTIME：任务运行状态汇总
- SYNC_MONITOR_EVENT：阶段事件流水
- SYNC_FIELD_QUALITY：字段质量统计
- SYNC_ERROR_RECORD：错误明细

### 4.1 SYNC_TASK

关键字段：

- taskId
- taskName
- sourceSystem
- sourceDb
- sourceSchema
- sourceTable
- sourceTopic
- targetType
- targetCluster
- targetDb
- targetTable
- consumerGroup
- parserType
- taskMode: standard/custom
- taskCategory: realtime/batch_complement/kafka_replay
- owner
- department
- slaLevel
- status
- currentVersion

### 4.2 SYNC_RELEASE

关键规则：

- draft 可以修改。
- published release 不可修改。
- 每次发布生成不可变 snapshot。
- 回滚只能切换 currentVersion，不能修改历史 snapshot。

关键字段：

- releaseId
- taskId
- version
- releaseStatus
- snapshotPath
- snapshotJson
- validationResult
- airflowDagId
- airflowRunId
- jarVersion
- gitCommitId
- createdBy / approvedBy / publishedBy

### 4.3 SYNC_TASK_RUNTIME

用于页面快速查询任务健康状态。

关键字段：

- taskId
- taskName
- version
- status
- yarnAppId
- sparkAppId
- lastBatchTime
- lastSuccessTime
- lastFailedTime
- lastInputRows
- lastOutputRows
- lastErrorRows
- kafkaCurrentOffset
- kafkaEndOffset
- kafkaLag
- dataDelaySeconds
- lastErrorMessage
- updatedTime

## 5. 后端 API

### 5.1 任务台账 API

```http
GET    /api/sync/tasks
GET    /api/sync/tasks/{taskId}
POST   /api/sync/tasks
PUT    /api/sync/tasks/{taskId}
DELETE /api/sync/tasks/{taskId}
```

查询条件：

- taskName
- sourceTopic
- sourceTable
- targetTable
- owner
- status
- taskMode
- targetType

### 5.2 字段、目标、策略 API

```http
GET /api/sync/tasks/{taskId}/fields
PUT /api/sync/tasks/{taskId}/fields

GET /api/sync/tasks/{taskId}/target-config
PUT /api/sync/tasks/{taskId}/target-config

GET /api/sync/tasks/{taskId}/strategy
PUT /api/sync/tasks/{taskId}/strategy
```

### 5.3 校验与 Snapshot API

```http
POST /api/sync/tasks/{taskId}/validate
POST /api/sync/tasks/{taskId}/snapshot
GET  /api/sync/tasks/{taskId}/releases
GET  /api/sync/releases/{releaseId}
```

### 5.4 发布、回滚、补数 API

```http
POST /api/sync/releases/{releaseId}/publish
POST /api/sync/tasks/{taskId}/restart
POST /api/sync/tasks/{taskId}/stop
POST /api/sync/tasks/{taskId}/rollback
POST /api/sync/tasks/{taskId}/complement
```

## 6. Snapshot 设计

发布时生成：

```text
hdfs:///sync-metadata/{taskName}/{version}/sync_metadata.conf
```

同时写入：

- SYNC_RELEASE.SNAPSHOT_JSON
- SYNC_RELEASE.SNAPSHOT_PATH

Snapshot 至少包含：

- taskName
- version
- source system/table/topic
- target type/db/table
- consumerGroup
- parserType
- taskMode
- strategy/customClass
- field mappings
- target config
- monitor config

## 7. 校验规则

任务级：

- taskName 非空且唯一。
- sourceTopic 非空。
- targetType 非空。
- targetTable 非空。
- consumerGroup 非空。
- taskMode 必须为 standard/custom。

字段级：

- sourceField 不重复。
- targetField 不重复。
- HBase 任务必须有 rowkey 字段或 rowkeyExpr。
- Hive 任务如果配置 partition，partition 字段必须存在。

策略级：

- custom 任务必须有 customClass。
- standard 任务不能强依赖 customClass。

发布级：

- published release 不允许覆盖。
- 同一个 task 下 version 不可重复。

## 8. Airflow 集成

第一阶段采用一个通用 DAG：

```text
airflow/dags/streaming_sync_submit.py
```

DAG 通过 dag_run.conf 接收：

- taskName
- version
- snapshotPath
- runType: realtime/restart/complement/replay
- jarPath
- mainClass
- sparkQueue
- executorMemory
- executorCores
- numExecutors
- extraSparkConf

DAG 任务建议：

- validate_conf
- build_spark_submit_command
- submit_spark_job
- extract_yarn_app_id
- callback_platform

关键要求：

- DAG 顶层不要查询业务数据库。
- 所有运行参数从 dag_run.conf 传入。
- 发布时创建 SYNC_JOB_INSTANCE。
- Airflow 调用失败时 release 不能标记为 published/running。

## 9. Spark Streaming 改造

新增启动参数：

```bash
spark-submit \
  --class com.foxconn.streaming.parser \
  streaming_parser.jar \
  --syncMetadataPath hdfs:///sync-metadata/R_WIP_LOG_T_TO_HIVE/20260430_001/sync_metadata.conf \
  --syncTaskName R_WIP_LOG_T_TO_HIVE \
  --syncVersion 20260430_001 \
  --runType realtime \
  --validateOnly false
```

启动逻辑：

1. 如果传入 syncMetadataPath：
   - 加载 snapshot。
   - 校验 taskName/version。
   - 转换为现有 Setting 或等价运行配置。
   - 进入原有 parser 逻辑。
2. 如果未传 syncMetadataPath：
   - 保持现有启动逻辑不变。
3. validateOnly=true：
   - 只执行配置校验。
   - 不创建 StreamingContext。

## 10. 运行监控

监控阶段：

- kafka_fetch
- parse_json
- target_write
- offset_commit

写入对象：

- SYNC_MONITOR_EVENT
- SYNC_TASK_RUNTIME
- SYNC_ERROR_RECORD
- SYNC_FIELD_QUALITY

关键要求：

- 每个 batch 至少写一条 monitor event。
- batch 成功后更新 runtime。
- batch 失败后写 error record。
- offset commit 成功/失败都要记录。
- 监控写入失败不能影响主同步任务，只能 warning 日志。
- 增加 enableSyncMonitor 开关，默认开启。

## 11. 前端 MVP

### 11.1 任务列表

字段：

- 任务名
- 源表
- 源 topic
- 目标类型
- 目标表
- 任务模式 standard/custom
- 负责人
- 当前版本
- 状态
- Kafka lag
- 数据延迟
- 最近成功时间
- 最近失败原因

### 11.2 任务详情

Tab：

- 基础信息
- 字段映射
- 目标配置
- 处理策略
- 发布历史
- 运行实例
- Kafka Offset
- 错误记录
- 字段质量

## 12. 发布、回滚、补数

### 12.1 发布流程

```text
draft task
  -> validate
  -> generate snapshot
  -> create release
  -> trigger Airflow
  -> create SYNC_JOB_INSTANCE
  -> update task.currentVersion/status
```

### 12.2 回滚流程

```text
select old release
  -> validate release exists and snapshot available
  -> update task.currentVersion
  -> trigger Airflow restart with old snapshot
  -> create new job instance
```

### 12.3 补数流程

支持：

- 按时间补数 startTime/endTime
- 按 Kafka offset 补数 startOffset/endOffset
- 按 partition 补数

要求：

- 补数任务必须创建独立 SYNC_JOB_INSTANCE。
- 补数 consumer group 必须和实时 consumer group 隔离。
- 支持 targetMode=normal/temp。

## 13. 现有任务导入

导入来源：

- 旧 setting.conf 或配置目录
- sync_metadata_example.conf
- MERGE_TOPIC_DETAIL
- MORE_TOPIC_DETAIL
- Airflow DAG conf
- 现有 parse/merge/custom class 信息

导入策略：

- 能识别的标准任务导入为 taskMode=standard。
- 识别不了复杂逻辑的任务导入为 taskMode=custom。
- custom 任务必须登记 customClass。
- 导入后 status=draft_imported。
- 导入任务不自动发布，必须人工确认后才能发布。

## 14. MVP Issue 顺序

建议按以下顺序实施：

1. #5 [MVP-1] 建立同步任务元数据表结构与领域模型
2. #6 [MVP-2] 实现任务台账与任务详情基础 API
3. #7 [MVP-3] 实现 SyncMetadata Snapshot 生成与发布前校验
4. #8 [MVP-4] 实现 Airflow 通用 DAG 与平台触发集成
5. #9 [MVP-5] 改造 Spark Streaming 支持从 SyncMetadata Snapshot 启动
6. #10 [MVP-6] 增加运行状态、Kafka Lag、错误与字段质量回写
7. #11 [MVP-7] 实现同步任务台账与任务详情前端页面
8. #12 [MVP-8] 实现发布、回滚、补数操作闭环
9. #13 [MVP-9] 实现现有同步任务导入工具
10. #16 [MVP-10] 补充端到端测试、部署脚本与运维文档

## 15. MVP 验收清单

第一版上线验收：

- 可以导入 10 个现有同步任务。
- 可以在页面看到任务台账。
- 可以查看某个任务的 sourceTopic、targetTable、consumerGroup、owner。
- 可以维护字段映射。
- 可以生成 snapshot。
- 可以 validate snapshot。
- 可以通过 Airflow 触发一个 Spark Streaming 任务。
- Spark Streaming 可以从 syncMetadataPath 启动。
- 可以看到任务 runtime 状态。
- 可以看到 Kafka lag 或最近 offset。
- 可以看到发布历史。
- 可以回滚到上一个 release。

## 16. 非 MVP 范围

第一版暂不做：

- 复杂低代码转换编辑器。
- 完整字段级血缘图谱。
- 自动 schema evolution。
- 复杂审批流。
- 多租户权限模型。
- 机器学习异常检测。

## 17. Codex 执行提示

每个 Issue 单独开 PR，避免一次性大改。

优先顺序：

1. 数据模型
2. API
3. Snapshot
4. Airflow
5. Spark 启动改造
6. Runtime 监控
7. 前端
8. 发布回滚补数
9. 导入
10. 文档和 E2E

所有 PR 必须保证：

- 不破坏现有 Spark Streaming 老启动方式。
- 新功能有单元测试或最小集成测试。
- 发布后的 snapshot 不可变。
- 监控写入失败不能影响主同步链路。
