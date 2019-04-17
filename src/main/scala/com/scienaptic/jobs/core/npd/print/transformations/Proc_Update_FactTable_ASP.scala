package com.scienaptic.jobs.core.Print_jobs
import org.apache.spark.sql.functions._
import com.scienaptic.jobs.ExecutionContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr, udf, when}
import org.apache.spark.sql.types.IntegerType

class Proc_Update_stgtable_ASP {


  def execute(executionContext: ExecutionContext): Unit = {
    val spark: SparkSession = executionContext.spark

    val sourceMap = executionContext.configuration.sources

    var stgtable =spark.sql("select * from ams_datamart_print.stgtable")


    stgtable=stgtable.withColumn("AMS_Temp_Units",
      when((col("Units")>0) && (col("Dollars")>0),col("Units")).otherwise(lit(0).cast(IntegerType)))


    stgtable=stgtable.withColumn("AMS_Temp_Dollars",
      when((col("Units")>0) && (col("Dollars")>0),col("Dollars")).otherwise(lit(0).cast(IntegerType)))

    stgtable=stgtable.withColumn("AMS_ASP",
      when(col("AMS_Temp_Units")===0 ,lit(0).cast(IntegerType)).otherwise(col("AMS_Temp_Dollars")/col("AMS_Temp_Units")))

    //exportToHive(stgtable,"",stgtablename,"ams_datamart_print",executionContext)
    //exportToHive(stgtable,"","stgtable_temp","ams_datamart_print",executionContext)

  }

}
