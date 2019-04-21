package com.scienaptic.jobs.core.npd.pc.monthly.transformations

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.core.npd.pc.monthly.transformations.MonthlyUSTransformations._
import com.scienaptic.jobs.core.npd.common.CommonTransformations._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

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
    val USMthResellerBTO_int = USMthResellerBTO_stg.transform(withUSMonthly)
    val USMthDist_int = USMthDist_stg.transform(withUSMonthly)
    val USMthDistBTO_int = USMthDistBTO_stg.transform(withUSMonthly)
    val USMthRetail_int = USMthRetail_stg.transform(withUSMonthly)


    val cols1 = USMthReseller_int.columns.toSet
    val cols2 = USMthResellerBTO_int.columns.toSet
    val cols3 = USMthDist_int.columns.toSet
    val cols4 = USMthDistBTO_int.columns.toSet
    val cols5 = USMthRetail_int.columns.toSet
    val total = cols1 ++ cols2 ++ cols3 ++ cols4 ++ cols5 // union

    def missingToNull(myCols: Set[String], allCols: Set[String]) = {
      allCols.toList.map(x => x match {
        case x if myCols.contains(x) => col(x)
        case _ => lit(null).as(x)
      })
    }

    val finalUSMonthlyDf =  USMthReseller_int.select(missingToNull(cols1,total):_*)
      .union(USMthResellerBTO_int.select(missingToNull(cols2,total):_*))
      .union(USMthDist_int.select(missingToNull(cols3,total):_*))
      .union(USMthDistBTO_int.select(missingToNull(cols4,total):_*))
      .union(USMthRetail_int.select(missingToNull(cols5,total):_*))
      .write.mode(SaveMode.Overwrite)
      .saveAsTable(DATAMART+"."+TABLE_NAME);

  }


}
