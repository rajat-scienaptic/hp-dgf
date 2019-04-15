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

object CommercialFeatEnggProcessor3 {
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
      .appName("Commercial-R-3")
      .config(sparkConf)
      .getOrCreate

    val baselineThreshold = if (min_baseline/2 > 0) min_baseline/2 else 0

    /* Avik Change Apr 13: This code was commented out.
     * Comment from HP in R Code: Consider this as an improvement, but difficult to represent given the noise in the data */
    var commercial = spark.read.option("header","true").option("inferSchema","true").csv("/etherData/commercialTemp/CommercialFeatEngg/commercialBeforeCannibalisation.csv")
    //var commercial = spark.read.option("header","true").option("inferSchema","true").csv("E:\\Scienaptic\\HP\\Pricing\\R\\SPARK_DEBUG_OUTPUTS\\commercialBeforeCannibalisation.csv")
      .withColumn("ES date", to_date(unix_timestamp(col("ES date"),"yyyy-MM-dd").cast("timestamp")))
      .withColumn("Week_End_Date", to_date(col("Week_End_Date")))
      .withColumn("GA date", to_date(unix_timestamp(col("GA date"),"yyyy-MM-dd").cast("timestamp")))
    //commercial.printSchema()
    /*
    commercial = commercial
      .withColumn("wed_cat", concat_ws(".",col("Week_End_Date"), col("L1_Category")))
    
    val commercialWithCompetitionDFTemp1 = commercial
        .groupBy("wed_cat")
        .agg(sum(col("Promo_Pct")*col("Qty")).as("z"), sum(col("Qty")).as("w"))

      commercial = commercial.join(commercialWithCompetitionDFTemp1, Seq("wed_cat"), "left")
        .withColumn("L1_cannibalization", (col("z")-(col("Promo_Pct")*col("Qty")))/(col("w")-col("Qty")))
        .drop("z","w","wed_cat")
        .withColumn("wed_cat", concat_ws(".", col("Week_End_Date"), col("L2_Category")))

    val commercialWithCompetitionDFTemp2 = commercial
        .groupBy("wed_cat")
        .agg(sum(col("Promo_Pct")*col("Qty")).as("z"), sum(col("Qty")).as("w"))

      commercial = commercial.join(commercialWithCompetitionDFTemp2, Seq("wed_cat"), "left")
        .withColumn("L2_cannibalization", (col("z")-(col("Promo_Pct")*col("Qty")))/(col("w")-col("Qty")))
        .drop("z","w","wed_cat")
        */
    //var commercialWithCompCannDF = commercialWithCompetitionDF

    val commercialWithAdj = commercial.withColumn("Adj_Qty", when(col("Qty")<=0,0).otherwise(col("Qty")))
    val commercialGroupWEDSKU = commercialWithAdj.groupBy("Week_End_Date","SKU")
      .agg(sum(col("Promo_Pct")*col("Adj_Qty")).as("sumSKU1"), sum("Adj_Qty").as("sumSKU2"))
      .join(commercialWithAdj, Seq("Week_End_Date","SKU"), "right")
    //writeDF(commercialGroupWEDSKU,"commercialGroupWEDSKU")
    val commercialGroupWEDL1Temp = commercialGroupWEDSKU
      .groupBy("Week_End_Date","Brand", "L1_Category")
      .agg(sum(col("Promo_Pct")*col("Adj_Qty")).as("sum1"), sum("Adj_Qty").as("sum2"))
    //writeDF(commercialGroupWEDL1Temp,"commercialGroupWEDL1Temp")

    val commercialGroupWEDL1 = commercialGroupWEDSKU.withColumn("L1_Category",col("L1_Category"))
      .join(commercialGroupWEDL1Temp.withColumn("L1_Category",col("L1_Category")), Seq("Week_End_Date","Brand", "L1_Category"), "left")
      .withColumn("L1_cannibalization", (col("sum1")-col("sumSKU1"))/(col("sum2")-col("sumSKU2")))
      .drop("sum1","sum2")
    //writeDF(commercialGroupWEDL1,"commercialGroupWEDL1_Cann")
    val commercialGroupWEDL1Temp2 = commercialGroupWEDL1
      .groupBy("Week_End_Date","Brand", "L2_Category")
      .agg(sum(col("Promo_Pct")*col("Adj_Qty")).as("sum1"), sum("Adj_Qty").as("sum2"))
    //writeDF(commercialGroupWEDL1Temp2,"commercialGroupWEDL1Temp2")

      commercial = commercialGroupWEDL1.withColumn("L2_Category",col("L2_Category"))
        .join(commercialGroupWEDL1Temp2.withColumn("L2_Category",col("L2_Category")), Seq("Week_End_Date","Brand", "L2_Category"), "left")
      .withColumn("L2_cannibalization", (col("sum1")-col("sumSKU1"))/(col("sum2")-col("sumSKU2")))
      .drop("sum1","sum2","sumSKU1","sumSKU2","Adj_Qty")
      .withColumn("L1_cannibalization", when(col("L1_cannibalization").isNull, 0).otherwise(col("L1_cannibalization")))
      .withColumn("L2_cannibalization", when(col("L2_cannibalization").isNull, 0).otherwise(col("L2_cannibalization")))
      .na.fill(0, Seq("L2_cannibalization","L1_cannibalization"))
      .withColumn("Sale_Price", col("Street Price")-col("IR")).persist(StorageLevel.MEMORY_AND_DISK)
    //writeDF(commercial,"commercialBeforeNPDCalc")

    commercial.write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/commercialTemp/CommercialFeatEngg/commercialBeforeNPDCalc.csv")

  }
}