# 同步任务元数据管理平台实施方案

## 1. 目标

建设一个同步任务元数据管理平台，用于统一管理 Oracle OGG -> Kafka -> Spark Streaming -> Hive/HBase/Kafka 等实时同步任务。

平台核心目标不是替代 Airflow，也不是替代 Spark Streaming，而是补齐任务资产、配置、发布、运行监控、回滚、补数和治理闭环。

## 2. 职责边界

### 元数据平台负责

- 同步任务定义
- 字段映射
- 目标端配置
- 标准任务与定制任务区分
- 发布版本管理
- SyncMetadata snapshot 生成
- 发布前校验
- Airflow 触发
- 运行状态展示
- Kafka lag、offset、错误、字段质量展示
- 回滚和补数入口

### Airflow 负责

- 任务编排
- spark-submit
- 重启、停止、补数触发
- DAG Run 记录
- 基础失败重试和告警

### Spark Streaming 负责

- 消费 Kafka
- 解析 OGG 消息
- 执行业务处理逻辑
- 写 Hive/HBase/Kafka 等目标端
- 回写 batch、offset、错误、字段质量和运行状态

## 3. 总体架构

```text
同步任务管理平台
  -> 元数据表
  -> Snapshot 生成
  -> Airflow API
  -> Runtime 查询

Airflow
  -> streaming_sync_submit DAG
  -> spark-submit

Spark Streaming
  -> 读取 syncMetadataPath
  -> 处理 Kafka 数据
  -> 写目标端
  -> 回写运行指标
```

## 4. 核心数据模型

MVP 阶段至少包含以下表：

- SYNC_TASK：同步任务主表
- SYNC_FIELD_MAPPING：字段映射
- SYNC_TARGET_CONFIG：目标端配置
- SYNC_STRATEGY：处理策略
- SYNC_RELEASE：发布版本与 snapshot
- SYNC_JOB_INSTANCE：运行实例
- SYNC_TASK_RUNTIME：任务当前运行状态
- SYNC_MONITOR_EVENT：阶段事件流水
- SYNC_FIELD_QUALITY：字段质量
- SYNC_ERROR_RECORD：错误记录

## 5. 任务类型

### standard 标准任务

适用于：

- 单 topic 输入
- 单目标输出
- 字段映射为主
- 简单 rowkey
- 简单 Hive partition
- 无复杂 join
- 无复杂业务状态处理

这类任务应支持页面化配置、自动校验、自动生成 snapshot、自动发布。

### custom 定制任务

适用于：

- 多 topic 合并
- 复杂 merge
- 需要查 HBase/Redis/DB 补充数据
- 复杂过滤和状态判断
- 回抛 Kafka
- 多目标写入

这类任务不强行低代码化。平台只管理元数据、customClass、jarVersion/gitCommitId、发布版本、运行状态和负责人。

## 6. Snapshot 原则

发布时生成不可变 SyncMetadata snapshot。

推荐路径：

```text
hdfs:///sync-metadata/{taskName}/{version}/sync_metadata.conf
```

关键规则：

- draft 可以修改。
- published release 不可修改。
- active/currentVersion 只指向某个 release。
- 回滚不是修改旧版本，而是把 currentVersion 切回历史 release。
- Spark Streaming 运行时只读取 snapshot，不直接读取 draft 表。

## 7. 发布流程

```text
创建/修改任务
  -> 保存 draft
  -> validate
  -> generate snapshot
  -> create release
  -> trigger Airflow DAG
  -> create SYNC_JOB_INSTANCE
  -> update task currentVersion/status
  -> Spark Streaming 启动
  -> runtime/monitor 回写
```

## 8. Airflow 集成

第一阶段使用一个通用 DAG：

```text
airflow/dags/streaming_sync_submit.py
```

DAG 通过 dag_run.conf 接收参数：

```json
{
  "taskName": "R_WIP_LOG_T_TO_HIVE",
  "version": "20260430_001",
  "snapshotPath": "hdfs:///sync-metadata/R_WIP_LOG_T_TO_HIVE/20260430_001/sync_metadata.conf",
  "runType": "realtime",
  "jarPath": "hdfs:///jars/streaming-parser.jar",
  "mainClass": "com.foxconn.streaming.parser",
  "sparkQueue": "default",
  "executorMemory": "4g",
  "executorCores": 2,
  "numExecutors": 4
}
```

DAG 顶层不要查询业务数据库。所有运行参数从 dag_run.conf 传入。

## 9. Spark Streaming 启动改造

新增启动参数：

```text
--syncMetadataPath
--syncTaskName
--syncVersion
--runType
--validateOnly
```

新启动方式：

```bash
spark-submit \
  --class com.foxconn.streaming.parser \
  streaming_parser.jar \
  --syncMetadataPath hdfs:///sync-metadata/R_WIP_LOG_T_TO_HIVE/20260430_001/sync_metadata.conf \
  --syncTaskName R_WIP_LOG_T_TO_HIVE \
  --syncVersion 20260430_001 \
  --runType realtime
```

兼容原则：

- 如果传入 syncMetadataPath，走 snapshot 模式。
- 如果没有传入 syncMetadataPath，保持老启动逻辑不变。
- validateOnly=true 时只校验配置，不创建 StreamingContext。

## 10. 运行监控

每个 batch 至少记录以下阶段：

- kafka_fetch
- parse_json
- target_write
- offset_commit

运行状态汇总写入 SYNC_TASK_RUNTIME，至少包括：

- status
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

监控写入失败不能影响主同步链路，只允许打印 warning 日志。

## 11. MVP Issues

当前仓库中已拆分以下实施任务：

- #5 [MVP-1] 建立同步任务元数据表结构与领域模型
- #6 [MVP-2] 实现任务台账与任务详情基础 API
- #7 [MVP-3] 实现 SyncMetadata Snapshot 生成与发布前校验
- #8 [MVP-4] 实现 Airflow 通用 DAG 与平台触发集成
- #9 [MVP-5] 改造 Spark Streaming 支持从 SyncMetadata Snapshot 启动
- #10 [MVP-6] 增加运行状态、Kafka Lag、错误与字段质量回写
- #11 [MVP-7] 实现同步任务台账与任务详情前端页面
- #12 [MVP-8] 实现发布、回滚、补数操作闭环
- #14 [MVP-9] 实现现有同步任务导入工具
- #15 [MVP-10] 补充总体设计、API、Airflow、Spark 启动与端到端验收文档

## 12. 建议执行顺序

第一批：

1. MVP-1 元数据模型
2. MVP-3 Snapshot 生成与校验
3. MVP-5 Spark Streaming snapshot 启动改造

第二批：

1. MVP-2 基础 API
2. MVP-4 Airflow 集成
3. MVP-6 Runtime 回写

第三批：

1. MVP-7 前端台账
2. MVP-8 发布/回滚/补数
3. MVP-9 旧任务导入
4. MVP-10 文档完善

## 13. MVP 验收清单

第一版上线只验收以下能力：

- 可以导入或创建同步任务。
- 可以查看任务台账。
- 可以查看 sourceTopic、targetTable、consumerGroup、owner。
- 可以维护字段映射。
- 可以生成 snapshot。
- 可以 validate snapshot。
- 可以通过 Airflow 触发 Spark Streaming。
- Spark Streaming 可以从 syncMetadataPath 启动。
- 可以查看 runtime 状态。
- 可以查看 Kafka lag 或最近 offset。
- 可以查看发布历史。
- 可以回滚到上一个 release。
- 可以创建补数任务。
