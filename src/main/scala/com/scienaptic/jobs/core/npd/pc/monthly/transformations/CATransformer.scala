package com.scienaptic.jobs.core.npd.pc.monthly.transformations

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.core.npd.common.CommonTransformations._
import com.scienaptic.jobs.core.npd.pc.monthly.transformations.CommonTransformations._
import com.scienaptic.jobs.core.npd.pc.monthly.transformations.CATransformations._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}

object CATransformer {

  def withAllTransformations(df : DataFrame) = {

    val cleanUpDollers = (str : String) => {
      str.replace("$","").replace(",","").toInt
    }

    def cleanDollersUDF = udf(cleanUpDollers)

    val finalDF = df
      .withColumn("tmp_date",to_date(col("timeper"), "MMM yyyy"))
      .drop("timeper")
      .withColumnRenamed("tmp_date","time_periods")
      .transform(withCalenderDetails)
      .withColumn("tmp_dollars",
        cleanDollersUDF(col("dollars")))
      .drop("dollars")
      .withColumnRenamed("tmp_dollars","dollars")
      .transform(withCAASP)
      .transform(withExchangeRates)
      .transform(withCACategory)
      .transform(withSmartBuy)
      //.transform(withCATopSellers)
      .transform(withCAPriceBand)

    finalDF

  }

  def execute(executionContext: ExecutionContext): Unit = {

    val spark = executionContext.spark

    val DATAMART = "npd_sandbox"
    val TABLE_NAME = "fct_tbl_ca_monthly_pc"

    val DM_CA_PC_Monthly_Dist_STG = "Stg_DM_CA_PC_Monthly_Dist"
    val DM_CA_PC_Monthly_Retail_STG = "Stg_DM_CA_PC_Monthly_Retail"

    val CAMthDist_stg  = spark.sql("select * from "+DATAMART+"."+DM_CA_PC_Monthly_Dist_STG)
    val CAMthRetail_stg  = spark.sql("select * from "+DATAMART+"."+DM_CA_PC_Monthly_Retail_STG)

    val CAMthDist_int = CAMthDist_stg.transform(withAllTransformations)
    val CAMthRetail_int = CAMthRetail_stg.transform(withAllTransformations)

    val cols1 = CAMthDist_int.columns.toSet
    val cols2 = CAMthRetail_int.columns.toSet
    val total = cols1 ++ cols2 // union

    def missingToNull(myCols: Set[String], allCols: Set[String]) = {
      allCols.toList.map(x => x match {
        case x if myCols.contains(x) => col(x)
        case _ => lit(null).as(x)
      })
    }

    val finalCAMonthlyDf =
      CAMthDist_int.select(missingToNull(cols1,total):_*)
      .union(CAMthRetail_int.select(missingToNull(cols2,total):_*))
      .write.mode(SaveMode.Overwrite)
      .saveAsTable(DATAMART+"."+TABLE_NAME);

  }

}
