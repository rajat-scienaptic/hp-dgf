package com.scienaptic.jobs.core.pricing.commercial

import com.scienaptic.jobs.ExecutionContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

object CommercialFeatEnggProcessor6 {
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
      .appName("Commercial-R-6")
      .config(sparkConf)
      .getOrCreate

    val baselineThreshold = if (min_baseline/2 > 0) min_baseline/2 else 0

    var commercial = spark.read.option("header","true").option("inferSchema","true").csv("E:\\Scienaptic\\HP\\Pricing\\Data\\CR1\\May31_Run\\spark_output\\commercialWithCompCannDF.csv")
      .withColumn("ES date", to_date(col("ES date")))
      .withColumn("Week_End_Date", to_date(col("Week_End_Date")))
      .withColumn("Valid_Start_Date", to_date(col("Valid_Start_Date")))
      .withColumn("Valid_End_Date", to_date(col("Valid_End_Date")))
      .withColumn("GA date", to_date(unix_timestamp(col("GA date"),"yyyy-MM-dd").cast("timestamp")))
      .persist(StorageLevel.MEMORY_AND_DISK).cache()

    val wind = Window.partitionBy("SKU_Name","Reseller_Cluster").orderBy("Qty")
    commercial.createOrReplaceTempView("commercial")
    //TODO: Combine 75 and 25 in one spark sql query.
    val percentil75DF = spark.sql("select SKU_Name, Reseller_Cluster, PERCENTILE(Qty, 0.75) OVER (PARTITION BY SKU_Name, Reseller_Cluster) as percentile_0_75 from commercial")
      .dropDuplicates("SKU_Name","Reseller_Cluster","percentile_0_75")

    commercial = commercial.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      .join(percentil75DF.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("SKU_Name", "Reseller_Cluster"), "left")
    commercial.createOrReplaceTempView("commercial")
    val percentile25DF = spark.sql("select SKU_Name, Reseller_Cluster, PERCENTILE(Qty, 0.25) OVER (PARTITION BY SKU_Name, Reseller_Cluster) as percentile_0_25 from commercial")
      .dropDuplicates("SKU_Name","Reseller_Cluster","percentile_0_25")

    commercial = commercial.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      .join(percentile25DF.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("SKU_Name", "Reseller_Cluster"), "left")

    commercial = commercial.withColumn("IQR", col("percentile_0_75")-col("percentile_0_25"))
      .withColumn("outlier", when(col("Qty")>col("percentile_0_75"), (col("Qty")-col("percentile_0_75"))/col("IQR")).otherwise(when(col("Qty")<col("percentile_0_25"), (col("Qty")-col("percentile_0_25"))/col("IQR")).otherwise(lit(0))))
      .withColumn("spike", when(abs(col("outlier"))<=8, 0).otherwise(1))
      .withColumn("spike", when((col("SKU_Name")==="OJ Pro 8610") && (col("Reseller_Cluster")==="Other - Option B") && (col("Week_End_Date")==="2014-11-01"),1).otherwise(col("spike")))
      .withColumn("spike", when((col("SKU").isin("F6W14A")) && (col("Week_End_Date")==="2017-07-10") && (col("Reseller_Cluster").isin("Other - Option B")), 1).otherwise(col("spike")))
      .withColumn("spike2", when((col("spike")===1) && (col("IR")>0), 0).otherwise(col("spike")))
      .drop("percentile_0_75", "percentile_0_25","IQR")
      .withColumn("Qty", col("Qty").cast("int"))//.repartition(1000).cache()

    commercial = commercial.withColumn("Qty",col("Qty").cast("int"))
    val commercialWithHolidayAndQtyFilter = commercial//.withColumn("Qty",col("Qty").cast("int"))
      .where((col("Promo_Flag")===0) && (col("USThanksgivingDay")===0) && (col("USCyberMonday")===0) && (col("spike")===0))
      .where(col("Qty")>0).cache()

    var npbl = commercialWithHolidayAndQtyFilter
      .groupBy("Reseller_Cluster","SKU_Name")
      .agg(mean("Qty").as("no_promo_avg"),
        stddev("Qty").as("no_promo_sd"),
        min("Qty").as("no_promo_min"),
        max("Qty").as("no_promo_max"))

    commercialWithHolidayAndQtyFilter.createOrReplaceTempView("npbl")
    val npblTemp = spark.sql("select SKU_Name, Reseller_Cluster, PERCENTILE(Qty, 0.50) OVER (PARTITION BY SKU_Name, Reseller_Cluster) as no_promo_med from npbl")
      .dropDuplicates("SKU_Name", "Reseller_Cluster","no_promo_med")

    npbl = npbl.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      .join(npblTemp.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("Reseller_Cluster","SKU_Name"), "inner")

    commercial = commercial.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      .join(npbl.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster")).select("SKU_Name", "Reseller_Cluster","no_promo_avg", "no_promo_med"),
        Seq("SKU_Name","Reseller_Cluster"), "left")
      .withColumn("no_promo_avg", when(col("no_promo_avg").isNull, 0).otherwise(col("no_promo_avg")))
      .withColumn("no_promo_med", when(col("no_promo_med").isNull, 0).otherwise(col("no_promo_med")))
      .withColumn("low_baseline", when(((col("no_promo_avg")>=min_baseline) && (col("no_promo_med")>=baselineThreshold)) ||
        ((col("no_promo_med")>=min_baseline) && (col("no_promo_avg")>=baselineThreshold)),0).otherwise(1))
      .withColumn("low_volume", when(col("Qty")>0,0).otherwise(1))
      .withColumn("raw_bl_avg", col("no_promo_avg")*(col("seasonality_npd")+lit(1)))
      .withColumn("raw_bl_med", col("no_promo_med")*(col("seasonality_npd")+lit(1)))

    commercial.coalesce(1).write.option("header","true").mode(SaveMode.Overwrite).csv("E:\\Scienaptic\\HP\\Pricing\\Data\\CR1\\May31_Run\\spark_output\\commercialBeforeEOL.csv")
  }
}