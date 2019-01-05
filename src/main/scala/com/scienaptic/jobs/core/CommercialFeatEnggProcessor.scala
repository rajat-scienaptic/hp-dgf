package com.scienaptic.jobs.core

import com.scienaptic.jobs.ExecutionContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CommercialFeatEnggProcessor {
  val Cat_switch=1

  def execute(executionContext: ExecutionContext): Unit = {

    val sparkConf = new SparkConf().setAppName("Test")
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Retail-R")
      .config(sparkConf)
      .getOrCreate


    val retail = spark.read.option("header","true").csv("posqty_")

  }
}
