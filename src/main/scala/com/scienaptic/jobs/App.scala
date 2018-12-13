package com.scienaptic.jobs

import com.scienaptic.jobs.config.{AppConfiguration, ConfigurationFactory}
import com.scienaptic.jobs.core.{CommercialTransform, RetailTransform}
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
    val spark = SparkSession.builder
      .master(configuration.sparkConfig.master)
      .appName(configuration.sparkConfig.appName)
      .getOrCreate
    val executionContext = ExecutionContext(spark, configuration)
    try {
      //TODO: Based on cli options, call appropriate Transformation.
      RetailTransform.execute(executionContext)
    } finally {
      spark.close()
    }
    println("Spark-job finished.")
  }
}