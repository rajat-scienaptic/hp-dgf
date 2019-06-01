package com.scienaptic.jobs.core.pricing.commercial

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.CommercialUtility.{checkPrevQtsGTBaseline, concatenateRank}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

object CommercialFeatEnggProcessor7 {
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
      //.master("local[*]")
      .appName("Commercial-R-7")
      .config(sparkConf)
      .getOrCreate

    val baselineThreshold = if (min_baseline/2 > 0) min_baseline/2 else 0

    var commercial = spark.read.option("header","true").option("inferSchema","true").csv("E:\\Scienaptic\\HP\\Pricing\\Data\\CR1\\May31_Run\\spark_output\\commercialBeforeEOL.csv")
      .withColumn("ES date", to_date(col("ES date")))
      .withColumn("Week_End_Date", to_date(col("Week_End_Date")))
      .withColumn("Valid_Start_Date", to_date(col("Valid_Start_Date")))
      .withColumn("Valid_End_Date", to_date(col("Valid_End_Date")))
      .withColumn("GA date", to_date(unix_timestamp(col("GA date"),"yyyy-MM-dd").cast("timestamp")))
      .persist(StorageLevel.MEMORY_AND_DISK).cache()

    val windForSKUAndReseller = Window.partitionBy("SKU&Reseller")
      .orderBy("Week_End_Date")

    var EOLcriterion = commercial
      .groupBy("SKU_Name","Reseller_Cluster","Week_End_Date")
      .agg(sum("Qty").as("Qty"), sum("no_promo_med").as("no_promo_med"))
      .sort("SKU_Name","Reseller_Cluster","Week_End_Date")
      .withColumn("Qty&no_promo_med", concat_ws(";",col("Qty"), col("no_promo_med")))
      .withColumn("SKU&Reseller", concat(col("SKU_Name"), col("Reseller_Cluster"))).cache()

    EOLcriterion = EOLcriterion.orderBy("SKU_Name","Reseller_Cluster","Week_End_Date")
      .withColumn("rank", row_number().over(windForSKUAndReseller))

    var EOLWithCriterion1 = EOLcriterion
      .groupBy("SKU&Reseller").agg((collect_list(concat_ws("_",col("rank"),col("Qty")).cast("string"))).as("QtyArray"))

    EOLWithCriterion1 = EOLWithCriterion1
      .withColumn("QtyArray", when(col("QtyArray").isNull, null).otherwise(concatenateRank(col("QtyArray"))))

    EOLcriterion = EOLcriterion.join(EOLWithCriterion1, Seq("SKU&Reseller"), "left")
      .withColumn("EOL_criterion", when(col("rank")<=stability_weeks || col("Qty")<col("no_promo_med"), 0).otherwise(checkPrevQtsGTBaseline(col("QtyArray"), col("rank"), col("no_promo_med"), lit(stability_weeks))))
      .drop("rank","QtyArray","SKU&Reseller","Qty&no_promo_med")

    val EOLCriterionLast = EOLcriterion.where(col("EOL_criterion")===lit(1))
      .groupBy("SKU_Name","Reseller_Cluster")
      .agg(max("Week_End_Date").as("last_date"))

    val EOLCriterionMax = commercial
      .groupBy("SKU_Name","Reseller_Cluster")
      .agg(max("Week_End_Date").as("max_date"))

    EOLcriterion = EOLCriterionMax.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      .withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      .join(EOLCriterionLast.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("SKU_Name","Reseller_Cluster"), "left")
      .where(col("last_date").isNotNull)

    val maxMaxDate = EOLcriterion.agg(max("max_date")).head().getDate(0)
    EOLcriterion = EOLcriterion
      .where((col("max_date")=!=col("last_date")) || (col("last_date")=!=maxMaxDate))
      .drop("max_date")

    commercial = commercial.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      .join(EOLcriterion.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("SKU_Name","Reseller_Cluster"), "left")
      .withColumn("EOL", when(col("last_date").isNull, 0).otherwise(when(col("Week_End_Date")>col("last_date"),1).otherwise(0)))
      .drop("last_date").cache()

    commercial = commercial
      .withColumn("EOL", when(col("SKU").isin("G3Q47A","M9L75A","F8B04A","B5L24A","L2719A","D3Q19A","F2A70A","CF377A","L2747A","F0V69A","G3Q35A","C5F93A","CZ271A","CF379A","B5L25A","D3Q15A","B5L26A","L2741A","CF378A","L2749A","CF394A"),0).otherwise(col("EOL")))
      .withColumn("EOL", when((col("SKU")==="C5F94A") && (col("Season")=!="STS'17"), 0).otherwise(col("EOL")))//.repartition(500)

    commercial.write.option("header","true").mode(SaveMode.Overwrite).csv("E:\\Scienaptic\\HP\\Pricing\\Data\\CR1\\May31_Run\\spark_output\\commercialBeforeBOL.csv")
  }

}