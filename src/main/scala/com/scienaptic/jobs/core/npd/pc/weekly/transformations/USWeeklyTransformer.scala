package com.scienaptic.jobs.core.npd.pc.weekly.transformations

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.core.npd.pc.monthly.transformations.CommonTransformations._
import com.scienaptic.jobs.core.npd.pc.monthly.transformations.USTransformations._
import com.scienaptic.jobs.core.npd.pc.weekly.transformations.USWeeklyTransformations._
import com.scienaptic.jobs.utility.NPDUtility
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}

object USWeeklyTransformer {


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
      //.transform(timePeriodsToDate)
      .transform(withWeeksToDisplay)
      .withColumn("tmp_dollars", cleanDollersUDF(col("dollars"))).drop("dollars").withColumnRenamed("tmp_dollars","dollars")
      .withColumn("tmp_units", cleanUnitsUDF(col("units"))).drop("units").withColumnRenamed("tmp_units","units")
      .transform(withASP)
      .transform(withSmartBuy)
      .transform(withWeeklyTopSellers)
      .transform(withVendorFamily)
      .transform(withCategory)
      .transform(withPriceBand)
      .transform(withOSDetails)
      .transform(withItemDescription)
      .transform(withCDWFormFactor)
      .transform(withTopVendors)

    finalDF

  }

  def execute(executionContext: ExecutionContext): Unit = {

    val spark = executionContext.spark

    val SANDBOX_DATAMART = "npd_sandbox"
    val AMS_DATAMART = "ams_datamart_pc"

    val TABLE_NAME = "fct_tbl_us_weekly_pc"

    val DM_Weekly_Reseller_PC_US = "DM_Weekly_Reseller_PC_US"
    val DM_Weekly_Reseller_PC_US_BTO = "DM_Weekly_Reseller_PC_US_BTO"
    val DM_Weekly_Dist_PC_US = "DM_Weekly_Dist_PC_US"
    val DM_Weekly_Dist_PC_US_BTO = "DM_Weekly_Dist_PC_US_BTO"
    val DM_Weekly_Retail_PC_US = "DM_Weekly_Retail_PC_US"


    val Historical_Fact_Table = "fct_tbl_us_weekly_pc_historical"

    val USWeeklyReseller_stg  = spark.sql("select * from "+SANDBOX_DATAMART+".Stg_"+DM_Weekly_Reseller_PC_US)
      .withColumn("ams_source",lit("US Weekly Reseller"))
      .withColumn("ams_channel",lit("Reseller"))

    val USWeeklyResellerBTO_stg  = spark.sql("select * from "+SANDBOX_DATAMART+".Stg_"+DM_Weekly_Reseller_PC_US_BTO)
      .withColumn("ams_source",lit("US Weekly Reseller Reseller BTO"))
      .withColumn("ams_channel",lit("Reseller"))

    val USWeeklyDist_stg  = spark.sql("select * from "+SANDBOX_DATAMART+".Stg_"+DM_Weekly_Dist_PC_US)
      .withColumn("ams_source",lit("US Weekly Distributor"))
      .withColumn("ams_channel",lit("Distributor"))

    val USWeeklyDistBTO_stg  = spark.sql("select * from "+SANDBOX_DATAMART+".Stg_"+DM_Weekly_Dist_PC_US_BTO)
      .withColumn("ams_source",lit("US Weekly Distributor BTO"))
      .withColumn("ams_channel",lit("Distributor"))

    val USWeeklyRetail_stg  = spark.sql("select * from "+SANDBOX_DATAMART+".Stg_"+DM_Weekly_Retail_PC_US)
      .withColumn("ams_source",lit("US Weekly Retail"))
      .withColumn("ams_channel",lit("Retail"))


    val USWeeklyReseller_int  = USWeeklyReseller_stg.transform(withAllTransformations)
    val USWeeklyResellerBTO_int  = USWeeklyResellerBTO_stg.transform(withAllTransformations)
    val USWeeklyDist_int  = USWeeklyDist_stg.transform(withAllTransformations)
    val USWeeklyDistBTO_int  = USWeeklyDistBTO_stg.transform(withAllTransformations)
    val USWeeklyRetail_int = USWeeklyRetail_stg.transform(withAllTransformations)


    USWeeklyReseller_int.write.mode(SaveMode.Overwrite)
      .saveAsTable(SANDBOX_DATAMART+"."+"Int_Fact_"+DM_Weekly_Reseller_PC_US);

    USWeeklyResellerBTO_int.write.mode(SaveMode.Overwrite)
      .saveAsTable(SANDBOX_DATAMART+"."+"Int_Fact_"+DM_Weekly_Reseller_PC_US_BTO);

    USWeeklyDist_int.write.mode(SaveMode.Overwrite)
      .saveAsTable(SANDBOX_DATAMART+"."+"Int_Fact_"+DM_Weekly_Dist_PC_US);

    USWeeklyDistBTO_int.write.mode(SaveMode.Overwrite)
      .saveAsTable(SANDBOX_DATAMART+"."+"Int_Fact_"+DM_Weekly_Dist_PC_US_BTO);

    USWeeklyRetail_int.write.mode(SaveMode.Overwrite)
      .saveAsTable(SANDBOX_DATAMART+"."+"Int_Fact_"+DM_Weekly_Retail_PC_US);

    //val historicalFact = spark.sql("select * from "+AMS_DATAMART+"."+Historical_Fact_Table)

    val cols1 = USWeeklyReseller_int.columns.toSet
    val cols2 = USWeeklyResellerBTO_int.columns.toSet
    val cols3 = USWeeklyDist_int.columns.toSet
    val cols4 = USWeeklyDistBTO_int.columns.toSet
    val cols5 = USWeeklyRetail_int.columns.toSet
    //val historic_columns = historicalFact.columns.toSet

    //val all_columns = historic_columns ++ cols1 ++ cols2 ++ cols3 ++ cols4 ++ cols5
    val all_columns =   cols1 ++ cols2 ++ cols3 ++ cols4 ++ cols5

    def missingToNull(myCols: Set[String]) = {
      all_columns.toList.map(x => x match {
        case x if myCols.contains(x) => col(x)
        case _ => lit("NA").as(x)
      })
    }

    //val finalDf = historicalFact.select(missingToNull(historic_columns):_*)
    val finalDf = USWeeklyReseller_int.select(missingToNull(cols1):_*)
      .union(USWeeklyResellerBTO_int.select(missingToNull(cols2):_*))
      .union(USWeeklyDist_int.select(missingToNull(cols3):_*))
      .union(USWeeklyDistBTO_int.select(missingToNull(cols4):_*))
      .union(USWeeklyRetail_int.select(missingToNull(cols5):_*))

    NPDUtility.writeToDataMart(spark,finalDf,AMS_DATAMART,TABLE_NAME)

  }

}
