package com.scienaptic.jobs.core.npd.pc.monthly.stagging

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.NPDUtility.load_csv_to_table

object PCMonthlyUS {

  val DM_PC_US_Mth_Reseller_SOURCE = "DM_PC_US_Mth_Reseller"
  val DM_PC_US_Mth_Reseller_BTO_SOURCE = "DM_PC_US_Mth_Reseller_BTO"
  val DM_US_PC_Monthly_Dist_SOURCE = "DM_US_PC_Monthly_Dist"
  val DM_US_PC_Monthly_Dist_BTO_SOURCE = "DM_US_PC_Monthly_Dist_BTO"
  val DM_US_PC_Monthly_Retail_SOURCE = "DM_US_PC_Monthly_Retail"

  val DATAMART = "ams_datamart_pc"

    def execute(executionContext: ExecutionContext): Unit = {
    //val spark: SparkSession = executionContext.spark

    val sourceMap = executionContext.configuration.sources

    val DM_PC_US_Mth_Reseller = sourceMap(DM_PC_US_Mth_Reseller_SOURCE)
    load_csv_to_table(executionContext,DM_PC_US_Mth_Reseller.filePath,DATAMART,DM_PC_US_Mth_Reseller.name)

    val DM_PC_US_Mth_Reseller_BTO = sourceMap(DM_PC_US_Mth_Reseller_BTO_SOURCE)
    load_csv_to_table(executionContext,DM_PC_US_Mth_Reseller_BTO.filePath,DATAMART,DM_PC_US_Mth_Reseller_BTO.name)

    val DM_US_PC_Monthly_Dist = sourceMap(DM_US_PC_Monthly_Dist_SOURCE)
    load_csv_to_table(executionContext,DM_US_PC_Monthly_Dist.filePath,DATAMART,DM_US_PC_Monthly_Dist.name)

    val DM_US_PC_Monthly_Dist_BTO = sourceMap(DM_US_PC_Monthly_Dist_BTO_SOURCE)
    load_csv_to_table(executionContext,DM_US_PC_Monthly_Dist_BTO.filePath,DATAMART,DM_US_PC_Monthly_Dist_BTO.name)

    val DM_US_PC_Monthly_Retail = sourceMap(DM_US_PC_Monthly_Retail_SOURCE)
    load_csv_to_table(executionContext,DM_US_PC_Monthly_Retail.filePath,DATAMART,DM_US_PC_Monthly_Retail.name)

  }
}
