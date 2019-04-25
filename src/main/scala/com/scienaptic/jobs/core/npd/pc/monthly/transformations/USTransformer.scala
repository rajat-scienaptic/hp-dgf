package com.scienaptic.jobs.core.npd.pc.monthly.transformations

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.core.npd.pc.monthly.transformations.USTransformations._
import com.scienaptic.jobs.core.npd.pc.monthly.transformations.CommonTransformations._
import com.scienaptic.jobs.core.npd.common.CommonTransformations._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

object USTransformer {

  def withAllTransformations(df : DataFrame) = {

    val finalDF = df
      .transform(timePeriodsToDate)
      .transform(withCalenderDetails)
      .transform(cleanDollars)
      .transform(withASP)
      //.transform(withSmartBuy)
      //.transform(withTopSellers)
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

    val USMthReseller_stg  = spark.sql("select * from "+DATAMART+"."+DM_PC_US_Mth_Reseller_STG)
      .withColumn("ams_source",lit("Reseller"))
    val USMthResellerBTO_stg  = spark.sql("select * from "+DATAMART+"."+DM_PC_US_Mth_Reseller_BTO_STG)
      .withColumn("ams_source",lit("ResellerBTO"))
    val USMthDist_stg  = spark.sql("select * from "+DATAMART+"."+DM_US_PC_Monthly_Dist_STG)
      .withColumn("ams_source",lit("Dist"))
    val USMthDistBTO_stg  = spark.sql("select * from "+DATAMART+"."+DM_US_PC_Monthly_Dist_BTO_STG)
      .withColumn("ams_source",lit("DistBTO"))
    val USMthRetail_stg  = spark.sql("select * from "+DATAMART+"."+DM_US_PC_Monthly_Retail_STG)
      .withColumn("ams_source",lit("Retail"))

    val cols1 = USMthReseller_stg.columns.toSet
    val cols2 = USMthResellerBTO_stg.columns.toSet
    val cols3 = USMthDist_stg.columns.toSet
    val cols4 = USMthDistBTO_stg.columns.toSet
    val cols5 = USMthRetail_stg.columns.toSet
    val total = cols1 ++ cols2 ++ cols3 ++ cols4 ++ cols5 // union

    def missingToNull(myCols: Set[String], allCols: Set[String]) = {
      allCols.toList.map(x => x match {
        case x if myCols.contains(x) => col(x)
        case _ => lit("NA").as(x)
      })
    }

    USMthReseller_stg
      .select(missingToNull(cols1,total):_*)
      .union(USMthResellerBTO_stg
        .select(missingToNull(cols2,total):_*))
      .union(USMthDist_stg
        .select(missingToNull(cols3,total):_*))
      .union(USMthDistBTO_stg
        .select(missingToNull(cols4,total):_*))
      .union(USMthRetail_stg
        .select(missingToNull(cols5,total):_*))
      .transform(withAllTransformations)
      .write.mode(SaveMode.Overwrite)
      .saveAsTable(DATAMART+"."+TABLE_NAME);

  }


}
