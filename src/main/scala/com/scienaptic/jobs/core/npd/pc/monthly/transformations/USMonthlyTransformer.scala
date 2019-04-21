package com.scienaptic.jobs.core.npd.pc.monthly.transformations

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.core.npd.pc.monthly.transformations.MonthlyUSTransformations._
import com.scienaptic.jobs.core.npd.common.CommonTransformations._
import org.apache.spark.sql.{DataFrame, SaveMode}

object USMonthlyTransformer {

  def withUSMonthly(df : DataFrame) = {

    val finalDF = df
      .transform(timePeriodsToDate)
      .transform(cleanDollars)
      .transform(withCalenderDetails)
      .transform(withASP)
      .transform(withSmartBuy)
      .transform(withTopSellers)
      .transform(withVendorFamily)
      .transform(withCategory)
      .transform(withCDW)
      .transform(withLenovoFocus)

    finalDF

  }

  def execute(executionContext: ExecutionContext): Unit = {

    val spark = executionContext.spark

    val DATAMART = "npd_sandbox"
    val TABLE_NAME = "fct_tbl_us_monthly_pc"

    val DM_PC_US_Mth_Reseller_STG = "Stg_DM_PC_US_Mth_Reseller"
    val DM_PC_US_Mth_Reseller_BTO_STG = "Stg_DM_PC_US_Mth_Reseller_BTO"
    val DM_US_PC_Monthly_Dist_STG = "Stg_DM_US_PC_Monthly_Dist"
    val DM_US_PC_Monthly_Dist_BTO_STG = "Stg_DM_US_PC_Monthly_Dist_BTO"
    val DM_US_PC_Monthly_Retail_STG = "Stg_DM_US_PC_Monthly_Retail"


    val USMthReseller_stg  = spark.sql("select * from "+DM_PC_US_Mth_Reseller_STG)
    val USMthResellerBTO_stg  = spark.sql("select * from "+DM_PC_US_Mth_Reseller_BTO_STG)
    val USMthDist_stg  = spark.sql("select * from "+DM_US_PC_Monthly_Dist_STG)
    val USMthDistBTO_stg  = spark.sql("select * from "+DM_US_PC_Monthly_Dist_BTO_STG)
    val USMthRetail_stg  = spark.sql("select * from "+DM_US_PC_Monthly_Retail_STG)


    val USMthReseller_int = USMthReseller_stg.transform(withUSMonthly)
    USMthReseller_int
      .write.mode(SaveMode.Overwrite)
      .saveAsTable(DATAMART+"."+TABLE_NAME)

    val USMthResellerBTO_int = USMthResellerBTO_stg.transform(withUSMonthly)
      .write.mode(SaveMode.Overwrite)
      .saveAsTable(DATAMART+"."+TABLE_NAME)

    val USMthDist_int = USMthDist_stg.transform(withUSMonthly)
    val USMthDistBTO_int = USMthDistBTO_stg.transform(withUSMonthly)
    val USMthRetail_int = USMthRetail_stg.transform(withUSMonthly)


    val finalUSMonthlyDf =  USMthReseller_int
      .union(USMthResellerBTO_int)
      .union(USMthDist_int)
      .union(USMthDistBTO_int)
      .union(USMthRetail_int)
      .write.mode(SaveMode.Overwrite)
      .saveAsTable(DATAMART+"."+TABLE_NAME);

  }


}
