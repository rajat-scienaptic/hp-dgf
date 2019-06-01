package com.scienaptic.jobs.core.pricing.commercial

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.bean.UnionOperation.doUnion
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

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
      .master("local[*]")
      //.master("yarn-client")
      .appName("Commercial-R=5")
      .config(sparkConf)
      .getOrCreate

    //val baselineThreshold = if (min_baseline/2 > 0) min_baseline/2 else 0
    var commercial = spark.read.option("header","true").option("inferSchema","true").csv("E:\\Scienaptic\\HP\\Pricing\\Data\\CR1\\May31_Run\\spark_output\\commercialBeforeIFS2Calc.csv")
      .withColumn("ES date", to_date(unix_timestamp(col("ES date"),"yyyy-MM-dd").cast("timestamp")))
      .withColumn("Week_End_Date", to_date(col("Week_End_Date")))
      .withColumn("GA date", to_date(unix_timestamp(col("GA date"),"yyyy-MM-dd").cast("timestamp")))
    val ifs2 = spark.read.option("header","true").option("inferSchema","true").csv("E:\\Scienaptic\\HP\\Pricing\\Data\\CR1\\May31_Run\\spark_output\\ifs2.csv")
      .withColumn("Valid_Start_Date", to_date(col("Valid_Start_Date")))
      .withColumn("Valid_End_Date", to_date(col("Valid_End_Date")))
    var seasonalityNPD = spark.read.option("header","true").option("inferSchema","true").csv("E:\\Scienaptic\\HP\\Pricing\\Data\\CR1\\May31_Run\\spark_output\\seasonalityNPD.csv")
    val seasonalityNPDScanner = seasonalityNPD.where(col("L1_Category")==="Office - Personal")
      .withColumn("L1_Category", when(col("L1_Category")==="Office - Personal", "Scanners").otherwise(col("L1_Category")))

    seasonalityNPD = doUnion(seasonalityNPD, seasonalityNPDScanner).get

    commercial = commercial.join(seasonalityNPD, Seq("L1_Category","Week"), "left")
        .drop("Week")
        .withColumn("seasonality_npd2", when((col("USCyberMonday")===lit(1)) || (col("USThanksgivingDay")===lit(1)),0).otherwise(col("seasonality_npd")))
    commercial = commercial
        .join(ifs2.where(col("Account")==="Commercial").select("SKU","Hardware_GM","Supplies_GM","Hardware_Rev","Supplies_Rev", "Changed_Street_Price", "Valid_Start_Date", "Valid_End_Date"), Seq("SKU"), "left")
        .withColumn("Valid_Start_Date", when(col("Valid_Start_Date").isNull, dat2000_01_01).otherwise(col("Valid_Start_Date")))
        .withColumn("Valid_End_Date", when(col("Valid_End_Date").isNull, dat9999_12_31).otherwise(col("Valid_End_Date")))
        .where((col("Week_End_Date")>=col("Valid_Start_Date")) && (col("Week_End_Date")<=col("Valid_End_Date")))  // CR1 - Filter relaxed
        .withColumn("Changed_Street_Price", when(col("Changed_Street_Price").isNull, 0).otherwise(col("Changed_Street_Price")))


    val ifs2FilteredAccount = ifs2.where(col("Account").isin("Best Buy","Office Depot-Max","Staples")).cache()
    val ifs2RetailAvg = ifs2FilteredAccount
      .groupBy("SKU")
      /* CR1 - FIX. na.rm=T not present in R code for mean of hardware/supplies variables. Need to check for NULLs within group */
      .agg(mean("Hardware_GM").as("Hardware_GM_retail_avg"), sum(when(col("Hardware_GM").isNull,1).otherwise(0)).as("HGM_NCount"),
        mean("Hardware_Rev").as("Hardware_Rev_retail_avg"), sum(when(col("Hardware_Rev").isNull,1).otherwise(0)).as("HRev_NCount"),
        mean("Supplies_GM").as("Supplies_GM_retail_avg"), sum(when(col("Supplies_GM").isNull,1).otherwise(0)).as("SGM_NCount"),
        mean("Supplies_Rev").as("Supplies_Rev_retail_avg"), sum(when(col("Supplies_Rev").isNull,1).otherwise(0)).as("SRev_NCount"))
      .withColumn("Hardware_GM_retail_avg", when(col("HGM_NCount")>0, null).otherwise(col("Hardware_GM_retail_avg")))
      .withColumn("Hardware_Rev_retail_avg", when(col("HRev_NCount")>0, null).otherwise(col("Hardware_Rev_retail_avg")))
      .withColumn("Supplies_GM_retail_avg", when(col("SGM_NCount")>0, null).otherwise(col("Supplies_GM_retail_avg")))
      .withColumn("Supplies_Rev_retail_avg", when(col("SRev_NCount")>0, null).otherwise(col("Supplies_Rev_retail_avg")))


    commercial = commercial.drop("Hardware_GM_retail_avg","Supplies_GM_retail_avg","Hardware_Rev_retail_avg","Supplies_Rev_retail_avg")
      .join(ifs2RetailAvg.select("SKU","Hardware_GM_retail_avg","Supplies_GM_retail_avg","Hardware_Rev_retail_avg","Supplies_Rev_retail_avg"), Seq("SKU"), "left")

    commercial = commercial
      .withColumn("Hardware_GM_type", when(col("Hardware_GM").isNotNull, "Commercial").otherwise(when(col("Hardware_GM").isNull && col("Hardware_GM_retail_avg").isNotNull, "Retail").otherwise(lit(null))))
      .withColumn("Hardware_Rev_type", when(col("Hardware_Rev").isNotNull, "Commercial").otherwise(when(col("Hardware_Rev").isNull && col("Hardware_Rev_retail_avg").isNotNull, "Retail").otherwise(lit(null))))
      .withColumn("Supplies_GM_type", when(col("Supplies_GM").isNotNull, "Commercial").otherwise(when(col("Supplies_GM").isNull && col("Supplies_GM_retail_avg").isNotNull, "Retail").otherwise(lit(null))))
      .withColumn("Supplies_Rev_type", when(col("Supplies_Rev").isNotNull, "Commercial").otherwise(when(col("Supplies_Rev").isNull && col("Supplies_Rev_retail_avg").isNotNull, "Retail").otherwise(lit(null))))
      .withColumn("Hardware_GM", when(col("Hardware_GM").isNull, col("Hardware_GM_retail_avg")).otherwise(col("Hardware_GM")))
      .withColumn("Hardware_Rev", when(col("Hardware_Rev").isNull, col("Hardware_Rev_retail_avg")).otherwise(col("Hardware_Rev")))
      .withColumn("Supplies_GM", when(col("Supplies_GM").isNull, col("Supplies_GM_retail_avg")).otherwise(col("Supplies_GM")))
      .withColumn("Supplies_Rev", when(col("Supplies_Rev").isNull, col("Supplies_Rev_retail_avg")).otherwise(col("Supplies_Rev")))
        .cache()

    val avgDiscountSKUAccountDF = commercial
      .groupBy("SKU_Name","Reseller_Cluster")
      .agg(sum(col("Qty")*col("IR")).as("Qty_IR"),sum(col("Qty")*col("Street Price").cast("double")).as("QTY_SP"))
      .withColumn("avg_discount_SKU_Account",col("Qty_IR")/col("QTY_SP"))

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

    commercial = commercial
        .withColumn("L1_cannibalization_log", log(lit(1)-col("L1_cannibalization")))
        .withColumn("L2_cannibalization_log", log(lit(1)-col("L2_cannibalization")))
        .withColumn("L1_competition_log", log(lit(1)-col("L1_competition")))
        .withColumn("L2_competition_log", log(lit(1)-col("L2_competition"))) //CR1 - Fixed issue. L2_competition instead of L1_competition
        .withColumn("L1_cannibalization_log", when(col("L1_cannibalization_log").isNull, 0).otherwise(col("L1_cannibalization_log")))
        .withColumn("L2_cannibalization_log", when(col("L2_cannibalization_log").isNull, 0).otherwise(col("L2_cannibalization_log")))
        .withColumn("L1_competition_log", when(col("L1_competition_log").isNull, 0).otherwise(col("L1_competition_log")))
        .withColumn("L2_competition_log", when(col("L2_competition_log").isNull, 0).otherwise(col("L2_competition_log")))
        .withColumn("Big_Deal", when(col("Big_Deal_Qty")>0, 1).otherwise(lit(0)))
        .withColumn("Big_Deal_Qty_log", log(when(col("Big_Deal_Qty")<1,1).otherwise(col("Big_Deal_Qty"))))

    commercial.coalesce(1).write.option("header","true").mode(SaveMode.Overwrite).csv("E:\\Scienaptic\\HP\\Pricing\\Data\\CR1\\May31_Run\\spark_output\\commercialWithCompCannDF.csv")

  }
}