package com.scienaptic.jobs.core.Print_jobs

import com.scienaptic.jobs.ExecutionContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

class Proc_Monthly_Update_FactTbl_ExchangeRateCA {


  def execute(executionContext: ExecutionContext): Unit = {
    val spark: SparkSession = executionContext.spark

    val sourceMap = executionContext.configuration.sources

    var stgtable =spark.sql("select * from ams_datamart_print.stgtable")

    var Tbl_Master_Exchange_Rates =spark.sql("select Time_Period_s_,CA_Exchange_Rate from ams_datamart_print.Tbl_Master_Exchange_Rates")
    Tbl_Master_Exchange_Rates=Tbl_Master_Exchange_Rates.withColumnRenamed("Time_Period_s_","timeperiod")


    var TmpFctTable=stgtable.join(Tbl_Master_Exchange_Rates,
      stgtable("Time_Period_s_")===Tbl_Master_Exchange_Rates("timeperiod"),"left")



    TmpFctTable=TmpFctTable.withColumn("AMS_Exchange_Rate_US",when(TmpFctTable("CA_Exchange_Rate").isNotNull,
      TmpFctTable("CA_Exchange_Rate")).otherwise(lit(null).cast(StringType)))

    TmpFctTable=TmpFctTable.drop("timeperiod","CA_Exchange_Rate")

    //exportToHive(TmpFctTable,"","stgtable_temp","ams_datamart_print",executionContext)

  }

}
