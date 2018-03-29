package com.leobenkel.starspipeline

import com.leobenkel.sparkcommon.{SparkJob, SparkMain}
import com.leobenkel.starspipeline.model.{Star, StarFromCsv}
import com.leobenkel.utils.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.concurrent.ExecutionContext
import scala.reflect.io.File

case class StarsPipelineTask(spark: SparkSession)
  extends SparkJob[StarsPipelineConfig](spark) {

  override def run(config: StarsPipelineConfig): Unit = {
    import spark.implicits._
    //    log.info(s"The config says: ${config.test}")

    val pathToFile = File(System.getProperty("user.dir") + "/src/main/resources/asu.tsv").toURL.getPath
    log.info(s"Get file at $pathToFile")

    val data: RDD[Star] = spark
      .sqlContext
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .option("inferSchema", "true")
      .load(pathToFile)
      .as[StarFromCsv]
      .flatMap(_.toStar)
      .rdd
      .keyBy(_.uniqueId)
      .mapValues(star => Seq(star))
      .reduceByKey(_ ++ _)
      .mapValues { stars =>
        stars.maxBy(_.probabilityOfRightValues)
      }
      .values

    implicit val execContext: ExecutionContext =
      ExecutionContext.fromExecutor(ExecutionContext.global)
    val es = new ElasticSearchConnector()

    val unpackedData = data.collect()
    log.info(s"Read ${unpackedData.length} stars")

    es.removeIndex.createMyIndex

    es.lastResult.get.fold(
      failure => log.error(s"Failed: ${failure.error.reason}"),
      _ => unpackedData
        .grouped(500)
        .foreach { stars =>
          es.insertDocuments(stars)
          es.lastResult.get.fold(
            failure => log.error(s"Failed: ${failure.error.reason}"),
            _ => Unit
          )
        }
    )

    log.info("Closing ES connection")
    es.close()
  }
}

case class StarsPipelineConfig(override val args: Seq[String]) extends Config(args) {
  //  val test = opt[Int](required = true)
}

object StarsPipelineMain extends SparkMain[StarsPipelineTask, StarsPipelineConfig] {
  override def getName: String = "StarsPipeline"

  override protected def instantiateJob: SparkSession => StarsPipelineTask = {
    (spark: SparkSession) => StarsPipelineTask(spark)
  }

  override protected def instantiateConfig: Seq[String] => StarsPipelineConfig = {
    (args: Seq[String]) => StarsPipelineConfig(args)
  }
}
