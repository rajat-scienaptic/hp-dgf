package com.scienaptic.jobs.core.npd.pc.monthly.transformations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object MonthlyUSTransformations {

  val DEBUG_TABLE_NAME = "debugTableName"

  def withASP(df: DataFrame): DataFrame = {

    val cleanDf=df.withColumn("AMS_Temp_Units",
      when(
        (col("Units")>0) && (col("Dollars")>0),
        col("Units")
      ).otherwise(lit(0).cast(IntegerType)))

    val withTempDollers =cleanDf.withColumn("AMS_Temp_Dollars",
      when((col("Units")>0) && (col("Dollars")>0),col("Dollars")).otherwise(lit(0).cast(IntegerType)))

    val withASPDf =withTempDollers.withColumn("AMS_ASP",
      when(col("AMS_Temp_Units")===0 ,lit(0).cast(IntegerType)).otherwise(col("AMS_Temp_Dollars")/col("AMS_Temp_Units")))

    //    if(debug){
    //      writeToDebugTable(df,DEBUG_TABLE_NAME)
    //    }

    withASPDf


  }


}
