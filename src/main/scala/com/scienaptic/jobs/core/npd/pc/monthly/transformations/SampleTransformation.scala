package com.scienaptic.jobs.core.npd.pc.monthly.transformations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class SampleTransformation {

  def execute(df: DataFrame, debugMode : Boolean) : DataFrame = {
    df.withColumn("sampleLitColumn",lit("1"))
  }

}
