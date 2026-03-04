package com.example.metrics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

object PipelineModules {

  /**
    * 兼容 Yarn/Spark 以 `--class com.example.metrics.PipelineModules` 启动。
    * 统一转发到 `PipelineMain`，避免因为入口类配置差异导致 ClassNotFound 或无 main 方法问题。
    */
  def main(args: Array[String]): Unit = {
    PipelineMain.main(args)
  }

  def readData(spark: SparkSession, ctx: ModuleCtx): DataFrame = {
    import spark.implicits._
    Seq(
      (1, "alice", 18),
      (2, "bob", 25),
      (3, "cathy", 31)
    ).toDF("id", "name", "age")
  }

  def transformData(df: DataFrame, ctx: ModuleCtx): DataFrame = {
    df.filter(col("age") >= 21)
      .withColumn("is_adult", col("age") >= 18)
  }

  def writeDb(df: DataFrame, ctx: ModuleCtx): Long = {
    // 示例：真实生产里可替换为 JDBC/Delta/Kafka 等写入。
    val cnt = df.count()
    println(s"[WRITE_DB] module_instance_id=${ctx.moduleInstanceId}, rows=$cnt")
    cnt
  }
}

object PipelineMain {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("module-tracer-demo")
      .master("local[*]")
      .getOrCreate()

    val runContext = RunContext.fromSpark(spark, codeVersion = "v1.0.0")
    val tracer = new ModuleTracer(new StdoutSink)

    implicit val baseCtx: ModuleCtx = ModuleCtx(
      spark = spark,
      runContext = runContext,
      moduleInstanceId = "bootstrap",
      moduleName = "bootstrap",
      tags = Map("pipeline" -> "demo", "env" -> "local")
    )

    try {
      val sourceDf = tracer.traceModule(ModuleDef(
        name = "readData",
        moduleType = "reader",
        ioInfo = Map("source" -> "in-memory-seq"),
        tags = Map("stage" -> "extract")
      ))(ctx => PipelineModules.readData(spark, ctx))

      val transformedDf = tracer.traceModule(ModuleDef(
        name = "transformData",
        moduleType = "transformer",
        ioInfo = Map("logic" -> "age_filter_and_flag"),
        tags = Map("stage" -> "transform")
      ))(ctx => PipelineModules.transformData(sourceDf, ctx))

      tracer.traceModule(ModuleDef(
        name = "writeDb",
        moduleType = "writer",
        ioInfo = Map("sink" -> "stdout-simulated-db"),
        tags = Map("stage" -> "load")
      ))(ctx => PipelineModules.writeDb(transformedDf, ctx))
    } finally {
      spark.stop()
    }
  }
}
