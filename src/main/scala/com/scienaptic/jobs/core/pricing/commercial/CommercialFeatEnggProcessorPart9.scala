package com.scienaptic.jobs.core.pricing.commercial

import com.scienaptic.jobs.ExecutionContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

object CommercialFeatEnggProcessor9 {
  val Cat_switch=1
  val min_baseline = 2
  val stability_weeks = 4
  val intro_weeks = 6

  val dat2000_01_01 = to_date(unix_timestamp(lit("2000-01-01"),"yyyy-MM-dd").cast("timestamp"))
  val dat9999_12_31 = to_date(unix_timestamp(lit("9999-12-31"),"yyyy-MM-dd").cast("timestamp"))

  def execute(executionContext: ExecutionContext): Unit = {

    val sparkConf = new SparkConf().setAppName("Test")
    val spark = SparkSession.builder
      .master("yarn-client")
      .appName("Commercial-R-9")
      .config(sparkConf)
      .getOrCreate

    var commercial = spark.read.option("header","true").option("inferSchema","true").csv("E:\\Scienaptic\\HP\\Pricing\\Data\\CR1\\May31_Run\\spark_output\\commercialBeforeOpposite.csv")
      .withColumn("ES date", to_date(col("ES date")))
      .withColumn("Week_End_Date", to_date(col("Week_End_Date")))
      .withColumn("Valid_Start_Date", to_date(col("Valid_Start_Date")))
      .withColumn("Valid_End_Date", to_date(col("Valid_End_Date")))
      .withColumn("GA date", to_date(unix_timestamp(col("GA date"),"yyyy-MM-dd").cast("timestamp")))
      .persist(StorageLevel.MEMORY_AND_DISK).cache()
    val commercialEOLSpikeFilter = commercial.where((col("EOL")===0) && (col("spike")===0))
    var opposite = commercialEOLSpikeFilter
      .groupBy("SKU_Name","Reseller_Cluster")
      .agg(count("SKU_Name").as("n"),
        mean("Qty").as("Qty_total"))

    var opposite_Promo_flag = commercialEOLSpikeFilter.where(col("Promo_Flag")===1)
      .groupBy("SKU_Name","Reseller_Cluster")
      .agg(mean("Qty").as("Qty_promo"))

    var opposite_Promo_flag_ZERO = commercialEOLSpikeFilter.where(col("Promo_Flag")===0)
      .groupBy("SKU_Name","Reseller_Cluster")
      .agg(mean("Qty").as("Qty_no_promo"))
    //TODO: try remove these 2 joins. Optimize this
    opposite = opposite.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      .join(opposite_Promo_flag.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("SKU_Name","Reseller_Cluster"), "left")
      .join(opposite_Promo_flag_ZERO.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("SKU_Name","Reseller_Cluster"), "left").cache()

    opposite = opposite
      .withColumn("opposite", when((col("Qty_no_promo")>col("Qty_promo")) || (col("Qty_no_promo")<0), 1).otherwise(0))
      .withColumn("opposite", when(col("opposite").isNull, 0).otherwise(col("opposite")))
      .withColumn("no_promo_sales", when(col("Qty_promo").isNull, 1).otherwise(0))

    commercial = commercial.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      .join(opposite.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
        .select("SKU_Name", "Reseller_Cluster", "opposite","no_promo_sales"), Seq("SKU_Name","Reseller_Cluster"), "left")
      .withColumn("NP_Flag", col("Promo_Flag"))
      .withColumn("NP_IR", col("IR"))
      .withColumn("high_disc_Flag", when(col("Promo_Pct")<=0.55, 0).otherwise(1))

    val commercialPromoMean = commercial
      .groupBy("Reseller_Cluster","SKU_Name","Season")
      .agg(mean(col("Promo_Flag")).as("PromoFlagAvg"))

    commercial = commercial.join(commercialPromoMean, Seq("Reseller_Cluster","SKU_Name","Season"), "left")

    commercial = commercial
      .withColumn("always_promo_Flag", when(col("PromoFlagAvg")===1, 1).otherwise(0)).drop("PromoFlagAvg")
      .withColumn("EOL", when(col("Reseller_Cluster")==="CDW",
        when(col("SKU_Name")==="LJ Pro M402dn", 0).otherwise(col("EOL"))).otherwise(col("EOL")))

    commercial.write.option("header","true").mode(SaveMode.Overwrite).csv("E:\\Scienaptic\\HP\\Pricing\\Data\\CR1\\May31_Run\\spark_output\\commercialBeforeCannGroups.csv")
  }

}