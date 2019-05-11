package com.scienaptic.jobs.core.npd.pc.monthly.transformations

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.core.npd.pc.monthly.transformations.CATransformations._
import com.scienaptic.jobs.core.npd.pc.monthly.transformations.CommonTransformations._
import com.scienaptic.jobs.utility.NPDUtility
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}

object CATransformer {

  def withAllTransformations(df : DataFrame) = {

    val cleanUpDollers = (str : String) => {
      val dollar = str.replace("$","").replace(",","").toDouble
      Math.round(dollar * 100.0) / 100.0
    }

    val cleanUpUnits = (str : String) => {
      str.replace(",","").toInt
    }

    def cleanDollersUDF = udf(cleanUpDollers)
    def cleanUnitsUDF = udf(cleanUpUnits)

    val finalDF = df
      .withColumn("tmp_date",to_date(col("timeper"), "MMM yyyy")).drop("timeper").withColumnRenamed("tmp_date","time_periods")
      .transform(withCalenderDetails)
      .withColumn("tmp_dollars", cleanDollersUDF(col("dollars"))).drop("dollars").withColumnRenamed("tmp_dollars","dollars")
      .withColumn("tmp_units", cleanUnitsUDF(col("units"))).drop("units").withColumnRenamed("tmp_units","units")
      .transform(withExchangeRates)
      .transform(withCAASP)
      .withColumnRenamed("MODELA","model")
      .withColumnRenamed("os","op_sys")
      .transform(withVendorFamily)
      .transform(withOSGroup)
      .transform(withCACategory)
      .transform(withSmartBuy)
      .transform(withCATopSellers)
      .transform(withCAPriceBand)
      .transform(withCAPriceBand4)
      .transform(withCAPriceBandDetailed)
      .transform(with_CA_US_PriceBand)
      .transform(with_CA_US_PriceBand4)
      .transform(with_CA_US_PriceBandDetailed)

    finalDF

  }

  def execute(executionContext: ExecutionContext): Unit = {

    val spark = executionContext.spark

    val SANDBOX_DATAMART = "npd_sandbox"
    val AMS_DATAMART = "ams_datamart_pc"
    val TABLE_NAME = "fct_tbl_ca_monthly_pc"

    val DM_CA_PC_Monthly_Dist_STG = "Stg_DM_CA_PC_Monthly_Dist"
    val DM_CA_PC_Monthly_Retail_STG = "Stg_DM_CA_PC_Monthly_Retail"

    val CAMthDist_stg  = spark.sql("select * from "+SANDBOX_DATAMART+"."+DM_CA_PC_Monthly_Dist_STG)
    val CAMthRetail_stg  = spark.sql("select * from "+SANDBOX_DATAMART+"."+DM_CA_PC_Monthly_Retail_STG)

    val CAMthDist_int = CAMthDist_stg.transform(withAllTransformations)

    CAMthDist_int.write.mode(SaveMode.Overwrite)
      .saveAsTable(SANDBOX_DATAMART+"."+"int_DM_CA_PC_Monthly_Dist");

    val CAMthRetail_int = CAMthRetail_stg.transform(withAllTransformations)

    CAMthRetail_int.write.mode(SaveMode.Overwrite)
      .saveAsTable(SANDBOX_DATAMART+"."+"int_DM_CA_PC_Monthly_Retail");

    //val historicalFact = spark.sql("select * from "+SANDBOX_DATAMART+".fct_tbl_ca_monthly_pc_historical")

    val cols1 = CAMthDist_int.columns.toSet
    val cols2 = CAMthRetail_int.columns.toSet

    //val historic_columns = historicalFact.columns.toSet

    //val all_columns = historic_columns ++ cols1 ++ cols2 // union
    val all_columns =  cols1 ++ cols2 // union

    def missingToNull(myCols: Set[String]) = {
      all_columns.toList.map(x => x match {
        case x if myCols.contains(x) => col(x)
        case _ => lit("NA").as(x)
      })
    }

    //val finalDf = historicalFact.select(missingToNull(historic_columns):_*)
    val finalDf = CAMthDist_int.select(missingToNull(cols1):_*)
      .union(CAMthRetail_int.select(missingToNull(cols2):_*))
        .drop("actual_units")

    NPDUtility.writeToDataMart(spark,finalDf,AMS_DATAMART,TABLE_NAME)

  }

}
