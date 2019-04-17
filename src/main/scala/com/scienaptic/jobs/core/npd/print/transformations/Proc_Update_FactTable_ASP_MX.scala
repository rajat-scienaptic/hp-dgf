package com.scienaptic.jobs.core.npd.print.transformations

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.NPDUtility.exportToHive
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.IntegerType

class Proc_Update_FactTable_ASP_MX {

  def execute(executionContext: ExecutionContext): Unit = {
    val spark: SparkSession = executionContext.spark

    val sourceMap = executionContext.configuration.sources

    var facttable =spark.sql("select * from ams_datamart_print.facttable")

    facttable=facttable.withColumn("AMS_Temp_Units",
      when((col("Units")>0) && (col("Dollars")>0),col("Units")).otherwise(lit(0).cast(IntegerType)))


    facttable=facttable.withColumn("AMS_Temp_DollarsMX",
      when((col("Units")>0) && (col("Dollars")>0),col("Dollars")).otherwise(lit(0).cast(IntegerType)))

    facttable=facttable.withColumn("AMS_ASP_MX",
      when(col("AMS_Temp_Units")===0 ,lit(0).cast(IntegerType)).otherwise(col("AMS_Temp_DollarsMX")/col("AMS_Temp_Units")))

    //exportToHive(facttable,"",facttablename,"ams_datamart_print",executionContext)
    exportToHive(facttable,"","stgtable","ams_datamart_print",executionContext)

  }
}
