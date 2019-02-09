package com.scienaptic.jobs.core

import java.text.SimpleDateFormat

import com.scienaptic.jobs.ExecutionContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import com.scienaptic.jobs.bean.UnionOperation.doUnion
import java.util.UUID
import org.apache.spark.storage.StorageLevel

import org.apache.spark.sql.functions.rank
import com.scienaptic.jobs.utility.Utils.renameColumns
import com.scienaptic.jobs.utility.CommercialUtility.{addDaystoDateStringUDF, checkPrevQtsGTBaseline, concatenateRank, createlist, extractWeekFromDateUDF}
import org.apache.spark.sql.expressions.Window
import com.scienaptic.jobs.utility.CommercialUtility._

object CommercialFeatEnggProcessor5 {
  val Cat_switch=1
  val min_baseline = 2
  val stability_weeks = 4
  val intro_weeks = 6

  val dat2000_01_01 = to_date(unix_timestamp(lit("2000-01-01"),"yyyy-MM-dd").cast("timestamp"))
  val dat9999_12_31 = to_date(unix_timestamp(lit("9999-12-31"),"yyyy-MM-dd").cast("timestamp"))

  def execute(executionContext: ExecutionContext): Unit = {

    val sparkConf = new SparkConf().setAppName("Test")
    val spark = SparkSession.builder
      //.master("local[*]")
      .master("yarn-client")
      .appName("Commercial-R=5")
      .config(sparkConf)
      .getOrCreate

    val baselineThreshold = if (min_baseline/2 > 0) min_baseline/2 else 0
    var commercial = spark.read.option("header","true").option("inferSchema","true").csv("/etherData/commercialTemp/CommercialFeatEngg/commercialBeforeIFS2Calc.csv")
      .withColumn("ES date", to_date(unix_timestamp(col("ES date"),"yyyy-MM-dd").cast("timestamp")))
      .withColumn("Week_End_Date", to_date(col("Week_End_Date")))
      .withColumn("GA date", to_date(unix_timestamp(col("GA date"),"yyyy-MM-dd").cast("timestamp")))
    val ifs2 = spark.read.option("header","true").option("inferSchema","true").csv("/etherData/commercialTemp/CommercialFeatEngg/ifs2.csv")
      .withColumn("Valid_Start_Date", to_date(col("Valid_Start_Date")))
      .withColumn("Valid_End_Date", to_date(col("Valid_End_Date")))
    var seasonalityNPD = spark.read.option("header","true").option("inferSchema","true").csv("/etherData/commercialTemp/CommercialFeatEngg/seasonalityNPD.csv")
    commercial.printSchema()
    val seasonalityNPDScanner = seasonalityNPD.where(col("L1_Category")==="Office - Personal")
      .withColumn("L1_Category", when(col("L1_Category")==="Office - Personal", "Scanners").otherwise(col("L1_Category")))
    //writeDF(seasonalityNPDScanner,"seasonalityNPDScanner")
    seasonalityNPD = doUnion(seasonalityNPD, seasonalityNPDScanner).get
    //writeDF(seasonalityNPD,"seasonalityNPD")
      commercial = commercial.join(seasonalityNPD, Seq("L1_Category","Week"), "left")
        .drop("Week")
        .withColumn("seasonality_npd2", when((col("USCyberMonday")===lit(1)) || (col("USThanksgivingDay")===lit(1)),0).otherwise(col("seasonality_npd").cast("int")))
        .join(ifs2.where(col("Account")==="Commercial").select("SKU","Hardware_GM","Supplies_GM","Hardware_Rev","Supplies_Rev", "Changed_Street_Price", "Valid_Start_Date", "Valid_End_Date"), Seq("SKU"), "left")
        .withColumn("Valid_Start_Date", when(col("Valid_Start_Date").isNull, dat2000_01_01).otherwise(col("Valid_Start_Date")))
        .withColumn("Valid_End_Date", when(col("Valid_End_Date").isNull, dat9999_12_31).otherwise(col("Valid_End_Date")))
        .where((col("Week_End_Date")>=col("Valid_Start_Date")) && (col("Week_End_Date")<col("Valid_End_Date")))
        .withColumn("Changed_Street_Price", when(col("Changed_Street_Price").isNull, 0).otherwise(col("Changed_Street_Price")))
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_Seasonality")

    val ifs2FilteredAccount = ifs2.where(col("Account").isin("Best Buy","Office Depot-Max","Staples")).cache()
    val ifs2RetailAvg = ifs2FilteredAccount
      .groupBy("SKU")
      .agg(mean("Hardware_GM").as("Hardware_GM_retail_avg"), mean("Hardware_Rev").as("Hardware_Rev_retail_avg"), mean("Supplies_GM").as("Supplies_GM_retail_avg"), mean("Supplies_Rev").as("Supplies_Rev_retail_avg")).cache()
    //writeDF(ifs2RetailAvg,"ifs2RetailAvg_WITH_HARDWARE_SUPPLIES")
      commercial = commercial.drop("Hardware_GM_retail_avg","Supplies_GM_retail_avg","Hardware_Rev_retail_avg","Supplies_Rev_retail_avg")
      .join(ifs2RetailAvg.select("SKU","Hardware_GM_retail_avg","Supplies_GM_retail_avg","Hardware_Rev_retail_avg","Supplies_Rev_retail_avg"), Seq("SKU"), "left")
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_BEFORE_IFS2_JOIN")
      commercial = commercial
      .withColumn("Hardware_GM_type", when(col("Hardware_GM").isNotNull, "Commercial").otherwise(when((col("Hardware_GM").isNull) && (col("Hardware_GM_retail_avg").isNotNull), "Retail").otherwise(lit(null))))
      .withColumn("Hardware_Rev_type", when(col("Hardware_Rev").isNotNull, "Commercial").otherwise(when((col("Hardware_Rev").isNull) && (col("Hardware_Rev_retail_avg").isNotNull), "Retail").otherwise(lit(null))))
      .withColumn("Supplies_GM_type", when(col("Supplies_GM").isNotNull, "Commercial").otherwise(when((col("Supplies_GM").isNull) && (col("Supplies_GM_retail_avg").isNotNull), "Retail").otherwise(lit(null))))
      .withColumn("Supplies_Rev_type", when(col("Supplies_Rev").isNotNull, "Commercial").otherwise(when((col("Supplies_Rev").isNull) && (col("Supplies_Rev_retail_avg").isNotNull), "Retail").otherwise(lit(null))))
      .withColumn("Hardware_GM", when(col("Hardware_GM").isNull, col("Hardware_GM_retail_avg")).otherwise(col("Hardware_GM")))
      .withColumn("Hardware_Rev", when(col("Hardware_Rev").isNull, col("Hardware_Rev_retail_avg")).otherwise(col("Hardware_Rev")))
      .withColumn("Supplies_GM", when(col("Supplies_GM").isNull, col("Supplies_GM_retail_avg")).otherwise(col("Supplies_GM")))
      .withColumn("Supplies_Rev", when(col("Supplies_Rev").isNull, col("Supplies_Rev_retail_avg")).otherwise(col("Supplies_Rev")))
        .cache()
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_WITH_HARDWARE_SUPPLIES_FEAT")*/

    val avgDiscountSKUAccountDF = commercial
      .groupBy("SKU_Name","Reseller_Cluster")
      .agg(sum(col("Qty")*col("IR")).as("Qty_IR"),sum(col("Qty")*col("Street Price").cast("double")).as("QTY_SP"))
      .withColumn("avg_discount_SKU_Account",col("Qty_IR")/col("QTY_SP"))
    //writeDF(avgDiscountSKUAccountDF,"avgDiscountSKUAccountDF")

    commercial = commercial.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      .join(avgDiscountSKUAccountDF.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("SKU_Name","Reseller_Cluster"), "left")
        .withColumn("avg_discount_SKU_Account", when(col("avg_discount_SKU_Account").isNull, 0).otherwise(col("avg_discount_SKU_Account")))
        .na.fill(0, Seq("avg_discount_SKU_Account"))
        .withColumn("supplies_GM_scaling_factor", lit(-0.3))
        .withColumn("Supplies_GM_unscaled", col("Supplies_GM"))
        .withColumn("Supplies_GM", col("Supplies_GM_unscaled")*(lit(1)+((col("Promo_Pct")-col("avg_discount_SKU_Account"))*col("supplies_GM_scaling_factor"))))
        .withColumn("Supplies_GM_no_promo", col("Supplies_GM_unscaled")*(lit(1)+((lit(0)-col("avg_discount_SKU_Account"))*col("supplies_GM_scaling_factor"))))
        .withColumn("Supplies_Rev_unscaled", col("Supplies_Rev"))
        .withColumn("Supplies_Rev", col("Supplies_Rev_unscaled")*(lit(1)+((col("Promo_Pct")-col("avg_discount_SKU_Account"))*col("supplies_GM_scaling_factor"))))
        .withColumn("Supplies_Rev_no_promo", col("Supplies_Rev_unscaled")*(lit(1)+((lit(0)-col("avg_discount_SKU_Account"))*col("supplies_GM_scaling_factor"))))
        .drop("Hardware_GM_retail_avg","Hardware_Rev_retail_avg","Supplies_GM_retail_avg","Supplies_Rev_retail_avg")
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_JOIN_AvgDISCOUNT_SKUACCOUNT")
    commercial = commercial
        .withColumn("L1_cannibalization_log", log(lit(1)-col("L1_cannibalization")))
        .withColumn("L2_cannibalization_log", log(lit(1)-col("L2_cannibalization")))
        .withColumn("L1_competition_log", log(lit(1)-col("L1_competition")))
        .withColumn("L2_competition_log", log(lit(1)-col("L1_competition")))
        .withColumn("L1_cannibalization_log", when(col("L1_cannibalization_log").isNull, 0).otherwise(col("L1_cannibalization_log")))
        .withColumn("L2_cannibalization_log", when(col("L2_cannibalization_log").isNull, 0).otherwise(col("L2_cannibalization_log")))
        .withColumn("L1_competition_log", when(col("L1_competition_log").isNull, 0).otherwise(col("L1_competition_log")))
        .withColumn("L2_competition_log", when(col("L2_competition_log").isNull, 0).otherwise(col("L2_competition_log")))
        .withColumn("Big_Deal", when(col("Big_Deal_Qty")>0, 1).otherwise(lit(0)))
        .withColumn("Big_Deal_Qty_log", log(when(col("Big_Deal_Qty")<1,1).otherwise(col("Big_Deal_Qty"))))
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_WITH_LOGS")*/
    commercial.write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/commercialTemp/CommercialFeatEngg/commercialWithCompCannDF.csv")

  }
}