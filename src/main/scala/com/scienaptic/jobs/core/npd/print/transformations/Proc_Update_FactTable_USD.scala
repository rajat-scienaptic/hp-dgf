package com.scienaptic.jobs.core.npd.print.transformations

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.NPDUtility.exportToHive
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.IntegerType

class Proc_Update_FactTable_USD {


  def execute(executionContext: ExecutionContext): Unit = {
    val spark: SparkSession = executionContext.spark

    val sourceMap = executionContext.configuration.sources

    var facttable =spark.sql("select * from ams_datamart_print.facttable")

    var Tbl_Master_Exchange_Rates =spark.sql("select Time_Period(s),Custom_Grouped_Max_Page_Width,NPD_Tech from ams_datamart_print.Tbl_Master_Exchange_Rates")


    var TmpFctTable=facttable.join(Tbl_Master_Exchange_Rates,
        facttable("Time_Period(s)")===Tbl_Master_Exchange_Rates("Time_Period(s)")
      ,"left")


    //not complete as the stored proc is not clear




    facttable=facttable.withColumn("AMS_US_Dollars",
      when(col("AMS_Exchange_Rate")===0 ,lit(0).cast(IntegerType)).otherwise(col("AMS_Temp_Dollars")/col("AMS_Exchange_Rate")))

    //exportToHive(facttable,"",facttablename,"ams_datamart_print",executionContext)
    exportToHive(facttable,"","stgtable","ams_datamart_print",executionContext)

  }
}
