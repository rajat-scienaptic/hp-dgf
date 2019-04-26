package com.scienaptic.jobs.core.npd.pc.monthly.staging

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.NPDUtility.load_csv_to_table

object CAMonthlyStaging {

  val DM_CA_PC_Monthly_Dist_SOURCE = "DM_CA_PC_Monthly_Dist"
  val DM_CA_PC_Monthly_Retail_SOURCE = "DM_CA_PC_Monthly_Retail"

  val DATAMART = "npd_sandbox"

    def execute(executionContext: ExecutionContext): Unit = {
    //val spark: SparkSession = executionContext.spark

    val sourceMap = executionContext.configuration.sources

    val DM_CA_PC_Monthly_Dist = sourceMap(DM_CA_PC_Monthly_Dist_SOURCE)
    load_csv_to_table(executionContext,DM_CA_PC_Monthly_Dist.filePath,DATAMART,DM_CA_PC_Monthly_Dist.name)

    val DM_CA_PC_Monthly_Retail = sourceMap(DM_CA_PC_Monthly_Retail_SOURCE)
    load_csv_to_table(executionContext,DM_CA_PC_Monthly_Retail.filePath,DATAMART,DM_CA_PC_Monthly_Retail.name)

  }

}
