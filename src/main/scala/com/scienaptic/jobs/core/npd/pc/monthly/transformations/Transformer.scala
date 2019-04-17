package com.scienaptic.jobs.core.npd.pc.monthly.transformations

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.core.npd.pc.monthly.transformations.MonthlyUSTransformations._

object Transformer {

  def execute(executionContext: ExecutionContext): Unit = {

    val DM_PC_US_Mth_Reseller_SOURCE = "Stg_DM_PC_US_Mth_Reseller"
    val DM_PC_US_Mth_Reseller_BTO_SOURCE = "Stg_DM_PC_US_Mth_Reseller_BTO"
    val DM_US_PC_Monthly_Dist_SOURCE = "Stg_DM_US_PC_Monthly_Dist"
    val DM_US_PC_Monthly_Dist_BTO_SOURCE = "Stg_DM_US_PC_Monthly_Dist_BTO"
    val DM_US_PC_Monthly_Retail_SOURCE = "Stg_DM_US_PC_Monthly_Retail"


    val spark = executionContext.spark;
    val df  = spark.sql("select * from "+DM_PC_US_Mth_Reseller_SOURCE)

    df
      .transform(withASP)
      //.transform(withASP)
      //.transform(withASP)

  }


}
