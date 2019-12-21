package com.scienaptic.jobs.core.npd.common

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, to_date, udf}

object CommonTransformations {

  /*
  replaces spaces in column headers with "_" and drops round brackets from column names
  */
  def withCleanHeaders(df :DataFrame)  = {

    df.toDF(df
      .schema
      .fieldNames
      .map(name => {
        "[ ,;{}.\\n\\t=]+".r.replaceAllIn(name, "_").toLowerCase
          .replace("(","").replace(")","")
      }): _*)
  }

  /*
  Updates converts time_periods to date to be utilised by other transformations
  */
  def timePeriodsToDate(df: DataFrame): DataFrame = {
    df.withColumn("tmp_date",to_date(col("time_periods"), "MMM yyyy"))
      .drop("time_periods").withColumnRenamed("tmp_date","time_periods")
  }


  /*def cleanDollars(df: DataFrame): DataFrame = {

    val cleanUpDollers = (str : String) => {
      str.replace("$","").replace(",","").toInt
    }

    def cleanDollersUDF = udf(cleanUpDollers)

    df.withColumn("tmp_dollars",
      cleanDollersUDF(col("dollars")))
      .drop("dollars")
      .withColumnRenamed("tmp_dollars","dollars")

    df
  }*/



}
