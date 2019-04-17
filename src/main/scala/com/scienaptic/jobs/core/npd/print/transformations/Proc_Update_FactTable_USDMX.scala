package com.scienaptic.jobs.core.npd.print.transformations

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.NPDUtility.exportToHive
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.IntegerType

class Proc_Update_FactTable_USDMX {
  def execute(executionContext: ExecutionContext): Unit = {
    val spark: SparkSession = executionContext.spark

    val sourceMap = executionContext.configuration.sources

    //val Update_FactTable_ASP = sourceMap(Update_FactTable_ASP_SOURCE)
    //val facttablename=Update_FactTable_ASP.tablename

    var facttable =spark.sql("select * from ams_datamart_print.facttable")


    facttable=facttable.withColumn("AMS_Dollar_US",
      when(col("AMS_Exchange_Rate")===0 ,lit(0).cast(IntegerType)).otherwise(col("Dollars")/col("AMS_Exchange_Rate_US")))
      .withColumn("AMS_Temp_Dollar_US",
        when(col("AMS_Exchange_Rate_US")===0 ,lit(0).cast(IntegerType)).otherwise(col("AMS_Temp_DollarsMX")/col("AMS_Exchange_Rate_US")))
      .withColumn("AMS_ASP_US_Dollar",
        when(col("AMS_Temp_Units")===0 ,lit(0).cast(IntegerType)).otherwise(col("AMS_Temp_Dollar_US")/col("AMS_Temp_Units")))

    //exportToHive(facttable,"",facttablename,"ams_datamart_print",executionContext)
    exportToHive(facttable,"","facttable_temp","ams_datamart_print",executionContext)

  }
}
