package com.scienaptic.jobs.core.npd.print

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.NPDUtility._

object LoadRawPrintTables {
  val DM_CA_Mth_Print_HW_Dist_SOURCE = "DM_CA_Mth_Print_HW_Dist"
  val DM_CA_Mth_Print_HW_Retail_SOURCE = "DM_CA_Mth_Print_HW_Retail"
  val DM_US_Mth_Print_HW_Dist_SOURCE = "DM_US_Mth_Print_HW_Dist"
  val DM_US_Mth_Print_HW_Reseller_SOURCE = "DM_US_Mth_Print_HW_Reseller"
  val DM_US_Mth_Print_HW_Retail_ALR_SOURCE = "DM_US_Mth_Print_HW_Retail_ALR"

  val DATAMART = "ams_datamart_print"

  /*def load_csv_to_table(executionContext: ExecutionContext,path:String,tablename:String):Unit={

    val spark: SparkSession = executionContext.spark
    var df=spark.read.option("escape","\"")
      .option("header", "true").csv(path)

      var esc="[ ,-]"
    df=df.toDF(df.columns.map(x=>x.replaceAll(esc,"_")):_*)
    df.write.mode(org.apache.spark.sql.SaveMode.Overwrite).saveAsTable("ams_datamart_print."+s"$tablename")

  }*/


  def execute(executionContext: ExecutionContext): Unit = {
    //val spark: SparkSession = executionContext.spark

    val sourceMap = executionContext.configuration.sources

    val DM_CA_Mth_Print_HW_Dist = sourceMap(DM_CA_Mth_Print_HW_Dist_SOURCE)
    var DM_CA_Mth_Print_HW_Dist_df=load_csv_to_table(executionContext,DM_CA_Mth_Print_HW_Dist.filePath,DATAMART,DM_CA_Mth_Print_HW_Dist.name)


    val DM_CA_Mth_Print_HW_Retail = sourceMap(DM_CA_Mth_Print_HW_Retail_SOURCE)
    var DM_CA_Mth_Print_HW_Retail_df=load_csv_to_table(executionContext,DM_CA_Mth_Print_HW_Retail.filePath,DATAMART,DM_CA_Mth_Print_HW_Retail.name)

    val DM_US_Mth_Print_HW_Dist = sourceMap(DM_US_Mth_Print_HW_Dist_SOURCE)
    var DM_US_Mth_Print_HW_Dist_df=load_csv_to_table(executionContext,DM_US_Mth_Print_HW_Dist.filePath,DATAMART,DM_US_Mth_Print_HW_Dist.name)

    val DM_US_Mth_Print_HW_Reseller = sourceMap(DM_US_Mth_Print_HW_Reseller_SOURCE)
    var DM_US_Mth_Print_HW_Reseller_df=load_csv_to_table(executionContext,DM_US_Mth_Print_HW_Reseller.filePath,DATAMART,DM_US_Mth_Print_HW_Reseller.name)

    val DM_US_Mth_Print_HW_Retail_ALR = sourceMap(DM_US_Mth_Print_HW_Retail_ALR_SOURCE)
    var DM_US_Mth_Print_HW_Retail_ALR_df=load_csv_to_table(executionContext,DM_US_Mth_Print_HW_Retail_ALR.filePath,DATAMART,DM_US_Mth_Print_HW_Retail_ALR.name)

  }
}
