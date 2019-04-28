package com.scienaptic.jobs.core.npd.pc.monthly.transformations

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.core.npd.common.CommonTransformations._
import com.scienaptic.jobs.core.npd.pc.monthly.transformations.USTransformations._
import com.scienaptic.jobs.core.npd.pc.monthly.transformations.CommonTransformations._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}

object USTransformer {


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
      .transform(timePeriodsToDate)
      .transform(withCalenderDetails)
      .withColumn("tmp_dollars", cleanDollersUDF(col("dollars"))).drop("dollars").withColumnRenamed("tmp_dollars","dollars")
      .withColumn("tmp_units", cleanUnitsUDF(col("units"))).drop("units").withColumnRenamed("tmp_units","units")
      .transform(withASP)
      .transform(withSmartBuy)
      //.transform(withTopSellers)
      //.transform(withLenovoFocus)
      .transform(withVendorFamily)
      .transform(withCategory)
      //.transform(withCDW)
      .transform(withOSGroup)
      .transform(withPriceBand)


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


    val USMthReseller_int  = USMthReseller_stg.transform(withAllTransformations)
      .withColumn("ams_source",lit("Reseller"))

//    USMthReseller_int.write.mode(SaveMode.Overwrite)
//      .saveAsTable(DATAMART+"."+"Int_Fact_DM_PC_US_Mth_Reseller");

    val USMthResellerBTO_int  = USMthResellerBTO_stg.transform(withAllTransformations)
      .withColumn("ams_source",lit("ResellerBTO"))

//    USMthResellerBTO_int.write.mode(SaveMode.Overwrite)
//      .saveAsTable(DATAMART+"."+"Int_Fact_DM_PC_US_Mth_Reseller_BTO");

    val USMthDist_int  = USMthDist_stg.transform(withAllTransformations)
      .withColumn("ams_source",lit("Dist"))

//    USMthDist_int.write.mode(SaveMode.Overwrite)
//      .saveAsTable(DATAMART+"."+"Int_Fact_DM_US_PC_Monthly_Dist");


    val USMthDistBTO_int  = USMthDistBTO_stg.transform(withAllTransformations)
      .withColumn("ams_source",lit("DistBTO"))

//    USMthDistBTO_int.write.mode(SaveMode.Overwrite)
//      .saveAsTable(DATAMART+"."+"Int_Fact_DM_US_PC_Monthly_Dist_BTO");


    val USMthRetail_int = USMthRetail_stg.transform(withAllTransformations)
      .withColumn("ams_source",lit("Retail"))

//    USMthRetail_int.write.mode(SaveMode.Overwrite)
//      .saveAsTable(DATAMART+"."+"Int_Fact_DM_US_PC_Monthly_Retail");

    val historicalFact = spark.sql("select * from npd_sandbox.fct_tbl_us_monthly_pc_historical")

    val cols1 = USMthReseller_int.columns.toSet
    val cols2 = USMthResellerBTO_int.columns.toSet
    val cols3 = USMthDist_int.columns.toSet
    val cols4 = USMthDistBTO_int.columns.toSet
    val cols5 = USMthRetail_int.columns.toSet
    val historic_columns = historicalFact.columns.toSet

    val all_columns = historic_columns ++ cols1 ++ cols2 ++ cols3 ++ cols4 ++ cols5

    def missingToNull(myCols: Set[String]) = {
      all_columns.toList.map(x => x match {
        case x if myCols.contains(x) => col(x)
        case _ => lit("NA").as(x)
      })
    }


    historicalFact.select(missingToNull(historic_columns):_*)
      .union(USMthReseller_int.select(missingToNull(cols1):_*))
      .union(USMthResellerBTO_int.select(missingToNull(cols2):_*))
      .union(USMthDist_int.select(missingToNull(cols3):_*))
      .union(USMthDistBTO_int.select(missingToNull(cols4):_*))
      .union(USMthRetail_int.select(missingToNull(cols5):_*))
      .write.mode(SaveMode.Overwrite)
      //.partitionBy("ams_year")
      .saveAsTable(DATAMART+"."+TABLE_NAME);

  }


}
