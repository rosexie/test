package com.example.metrics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.net.InetAddress
import java.time.Instant
import java.util.UUID
import scala.collection.mutable
import scala.util.control.NonFatal

final case class RunContext(
    runId: String,
    sparkAppId: String,
    sparkUiUrl: Option[String],
    yarnQueue: Option[String],
    codeVersion: String,
    driverHost: String,
    startedAtMs: Long,
    extra: Map[String, String] = Map.empty
)

object RunContext {
  def fromSpark(spark: SparkSession, codeVersion: String, runId: String = UUID.randomUUID().toString): RunContext = {
    val sc = spark.sparkContext
    val conf = sc.getConf
    RunContext(
      runId = runId,
      sparkAppId = sc.applicationId,
      sparkUiUrl = Option(sc.uiWebUrl).flatten,
      yarnQueue = conf.getOption("spark.yarn.queue"),
      codeVersion = codeVersion,
      driverHost = InetAddress.getLocalHost.getHostName,
      startedAtMs = System.currentTimeMillis(),
      extra = Map(
        "spark_master" -> sc.master,
        "spark_app_name" -> sc.appName
      )
    )
  }
}

final case class ModuleDef(
    name: String,
    moduleType: String,
    ioInfo: Map[String, String] = Map.empty,
    tags: Map[String, String] = Map.empty
)

final case class DfMetrics(
    rowCount: Long,
    countDurationMs: Long,
    persisted: Boolean,
    storageLevel: String
)

object DfMetrics {
  def collect(df: DataFrame, persistBeforeCount: Boolean = true): DfMetrics = {
    val shouldPersist = persistBeforeCount && !df.storageLevel.useMemory && !df.storageLevel.useDisk
    if (shouldPersist) {
      df.persist(StorageLevel.MEMORY_AND_DISK)
    }

    val start = System.currentTimeMillis()
    val rowCount = df.count()
    val duration = System.currentTimeMillis() - start

    if (shouldPersist) {
      df.unpersist(blocking = false)
    }

    DfMetrics(
      rowCount = rowCount,
      countDurationMs = duration,
      persisted = shouldPersist,
      storageLevel = if (shouldPersist) StorageLevel.MEMORY_AND_DISK.description else df.storageLevel.description
    )
  }
}

final case class ErrorInfo(
    message: String,
    stackTrace: String,
    errorClass: String
)

final case class ModuleInfo(
    moduleInstanceId: String,
    moduleName: String,
    moduleType: String,
    startMs: Long,
    endMs: Long,
    durationMs: Long,
    status: String
)

final case class ModuleEvent(
    eventAt: String,
    runContext: RunContext,
    moduleInfo: ModuleInfo,
    dataMetrics: Option[DfMetrics],
    ioInfo: Map[String, String],
    errorInfo: Option[ErrorInfo],
    tags: Map[String, String]
)

trait MetricsSink {
  def report(event: ModuleEvent): Unit
}

final class StdoutSink extends MetricsSink {
  override def report(event: ModuleEvent): Unit = {
    println(ModuleEventRenderer.render(event))
  }
}

object ModuleEventRenderer {
  def render(event: ModuleEvent): String = {
    val base = mutable.ArrayBuffer[String](
      s"event_at=${event.eventAt}",
      s"run_id=${event.runContext.runId}",
      s"spark_app_id=${event.runContext.sparkAppId}",
      s"spark_ui_url=${event.runContext.sparkUiUrl.getOrElse("-")}",
      s"yarn_queue=${event.runContext.yarnQueue.getOrElse("-")}",
      s"code_version=${event.runContext.codeVersion}",
      s"driver_host=${event.runContext.driverHost}",
      s"module_instance_id=${event.moduleInfo.moduleInstanceId}",
      s"module=${event.moduleInfo.moduleName}",
      s"module_type=${event.moduleInfo.moduleType}",
      s"status=${event.moduleInfo.status}",
      s"start_ms=${event.moduleInfo.startMs}",
      s"end_ms=${event.moduleInfo.endMs}",
      s"duration_ms=${event.moduleInfo.durationMs}"
    )

    event.dataMetrics.foreach { dm =>
      base += s"row_count=${dm.rowCount}"
      base += s"count_duration_ms=${dm.countDurationMs}"
      base += s"count_persisted=${dm.persisted}"
      base += s"count_storage_level=${dm.storageLevel}"
    }

    if (event.ioInfo.nonEmpty) {
      base += s"io_info=${event.ioInfo.mkString("{", ",", "}")}"
    }

    event.errorInfo.foreach { err =>
      base += s"error_class=${err.errorClass}"
      base += s"error_message=${err.message}"
      base += s"error_stacktrace=${err.stackTrace}"
    }

    if (event.tags.nonEmpty) {
      base += s"tags=${event.tags.mkString("{", ",", "}")}"
    }

    base.mkString(" | ")
  }
}

final case class ModuleCtx(
    spark: SparkSession,
    runContext: RunContext,
    moduleInstanceId: String,
    moduleName: String,
    tags: Map[String, String] = Map.empty
)

final class ModuleTracer(sink: MetricsSink) {

  def traceModule[T](moduleDef: ModuleDef)(fn: ModuleCtx => T)(implicit baseCtx: ModuleCtx): T = {
    val moduleInstanceId = s"${moduleDef.name}-${UUID.randomUUID()}"
    val ctx = baseCtx.copy(moduleInstanceId = moduleInstanceId, moduleName = moduleDef.name, tags = baseCtx.tags ++ moduleDef.tags)
    val sc = ctx.spark.sparkContext

    val start = System.currentTimeMillis()
    sc.setJobGroup(groupId = moduleInstanceId, description = s"module=${moduleDef.name}", interruptOnCancel = true)

    try {
      val result = fn(ctx)
      val end = System.currentTimeMillis()
      emit(
        ctx = ctx,
        moduleDef = moduleDef,
        start = start,
        end = end,
        status = "SUCCESS",
        dataMetrics = extractDataMetrics(result),
        errorInfo = None
      )
      result
    } catch {
      case NonFatal(e) =>
        val end = System.currentTimeMillis()
        emit(
          ctx = ctx,
          moduleDef = moduleDef,
          start = start,
          end = end,
          status = "FAILED",
          dataMetrics = None,
          errorInfo = Some(buildErrorInfo(e))
        )
        throw e
    } finally {
      sc.clearJobGroup()
    }
  }

  private def extractDataMetrics(result: Any): Option[DfMetrics] = result match {
    case df: DataFrame => Some(DfMetrics.collect(df))
    case _             => None
  }

  private def emit(
      ctx: ModuleCtx,
      moduleDef: ModuleDef,
      start: Long,
      end: Long,
      status: String,
      dataMetrics: Option[DfMetrics],
      errorInfo: Option[ErrorInfo]
  ): Unit = {
    val event = ModuleEvent(
      eventAt = Instant.ofEpochMilli(end).toString,
      runContext = ctx.runContext,
      moduleInfo = ModuleInfo(
        moduleInstanceId = ctx.moduleInstanceId,
        moduleName = moduleDef.name,
        moduleType = moduleDef.moduleType,
        startMs = start,
        endMs = end,
        durationMs = end - start,
        status = status
      ),
      dataMetrics = dataMetrics,
      ioInfo = moduleDef.ioInfo,
      errorInfo = errorInfo,
      tags = ctx.tags
    )

    try {
      sink.report(event)
    } catch {
      case NonFatal(reportEx) =>
        System.err.println(s"[WARN] metrics report failed but ignored: ${reportEx.getMessage}")
    }
  }

  private def buildErrorInfo(e: Throwable): ErrorInfo = {
    ErrorInfo(
      message = DataMasking.maskAndTruncate(Option(e.getMessage).getOrElse(e.toString), 1024),
      stackTrace = DataMasking.maskAndTruncate(e.getStackTrace.mkString("\n"), 4096),
      errorClass = e.getClass.getName
    )
  }
}

object DataMasking {
  private val SensitivePattern = "(?i)(password|token|secret)\\s*[=:]\\s*([^\\s,;]+)".r

  def maskAndTruncate(input: String, maxLen: Int): String = {
    val masked = SensitivePattern.replaceAllIn(input, m => s"${m.group(1)}=***")
    if (masked.length <= maxLen) masked else masked.take(maxLen) + "...(truncated)"
  }
}
