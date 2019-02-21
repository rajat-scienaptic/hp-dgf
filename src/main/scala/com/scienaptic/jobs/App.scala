package com.scienaptic.jobs

import com.scienaptic.jobs.config.{AppConfiguration, ConfigurationFactory}
import com.scienaptic.jobs.core.{CommercialTransform, HPDataProcessor, RetailTransform}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]): Unit = {
    new App(args).start()
  }
}

case class ExecutionContext(val spark: SparkSession, val configuration: AppConfiguration)

class App(args: Array[String]) extends ConfigurationFactory[AppConfiguration](args) {

  private def start(): Unit = {
    println("Spark-job Started.")
    val sparkConf = new SparkConf()
    sparkConf.set("spark.sql.crossJoin.enabled", "true")
    val spark = SparkSession.builder
      .master(configuration.sparkConfig.master)
      .appName(configuration.sparkConfig.appName)
      .config(sparkConf)
      .getOrCreate
    val executionContext = ExecutionContext(spark, configuration)
    try {
      HPDataProcessor.execute(executionContext)
    } finally {
      spark.close()
    }
    println("Spark-job finished.")
  }
}