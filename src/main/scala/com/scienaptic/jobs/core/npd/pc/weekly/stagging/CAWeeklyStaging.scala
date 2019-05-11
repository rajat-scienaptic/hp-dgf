package com.scienaptic.jobs.core.npd.pc.weekly.stagging

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.NPDUtility.load_csv_to_table

object CAWeeklyStaging {

  val DM_PC_CA_Weekly_Retail_SOURCE = "DM_PC_CA_Weekly_Retail"

  val SANDBOX_DATAMART = "npd_sandbox"

    def execute(executionContext: ExecutionContext): Unit = {

    val sourceMap = executionContext.configuration.sources

    val DM_PC_CA_Weekly_Retail = sourceMap(DM_PC_CA_Weekly_Retail_SOURCE)
    load_csv_to_table(executionContext,DM_PC_CA_Weekly_Retail.filePath,SANDBOX_DATAMART,DM_PC_CA_Weekly_Retail.name)

  }

}
