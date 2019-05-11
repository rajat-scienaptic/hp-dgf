package com.scienaptic.jobs.core.npd.pc.weekly.stagging

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.NPDUtility.load_csv_to_table

object USWeeklyStaging {

  val DM_Weekly_Reseller_PC_US_SOURCE = "DM_Weekly_Reseller_PC_US"
  val DM_Weekly_Reseller_PC_US_BTO_SOURCE = "DM_Weekly_Reseller_PC_US_BTO"
  val DM_Weekly_Dist_PC_US_SOURCE = "DM_Weekly_Dist_PC_US"
  val DM_Weekly_Dist_PC_US_BTO_SOURCE = "DM_Weekly_Dist_PC_US_BTO"
  val DM_Weekly_Retail_PC_US_SOURCE = "DM_Weekly_Retail_PC_US"

  val SANDBOX_DATAMART = "npd_sandbox"

    def execute(executionContext: ExecutionContext): Unit = {
    //val spark: SparkSession = executionContext.spark

    val sourceMap = executionContext.configuration.sources

    val DM_Weekly_Reseller_PC_US = sourceMap(DM_Weekly_Reseller_PC_US_SOURCE)
    load_csv_to_table(executionContext,DM_Weekly_Reseller_PC_US.filePath,SANDBOX_DATAMART,DM_Weekly_Reseller_PC_US.name)

    val DM_Weekly_Reseller_PC_US_BTO = sourceMap(DM_Weekly_Reseller_PC_US_BTO_SOURCE)
    load_csv_to_table(executionContext,DM_Weekly_Reseller_PC_US_BTO.filePath,SANDBOX_DATAMART,DM_Weekly_Reseller_PC_US_BTO.name)

    val DM_Weekly_Dist_PC_US = sourceMap(DM_Weekly_Dist_PC_US_SOURCE)
    load_csv_to_table(executionContext,DM_Weekly_Dist_PC_US.filePath,SANDBOX_DATAMART,DM_Weekly_Dist_PC_US.name)

    val DM_Weekly_Dist_PC_US_BTO = sourceMap(DM_Weekly_Dist_PC_US_BTO_SOURCE)
    load_csv_to_table(executionContext,DM_Weekly_Dist_PC_US_BTO.filePath,SANDBOX_DATAMART,DM_Weekly_Dist_PC_US_BTO.name)

    val DM_Weekly_Retail_PC_US = sourceMap(DM_Weekly_Retail_PC_US_SOURCE)
    load_csv_to_table(executionContext,DM_Weekly_Retail_PC_US.filePath,SANDBOX_DATAMART,DM_Weekly_Retail_PC_US.name)

  }
}
