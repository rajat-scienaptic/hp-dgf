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

object CommercialFeatEnggProcessor4 {
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
      .appName("Commercial-R-4")
      .config(sparkConf)
      .getOrCreate

    val baselineThreshold = if (min_baseline/2 > 0) min_baseline/2 else 0
    var commercial = spark.read.option("header","true").option("inferSchema","true").csv("/etherData/commercialTemp/CommercialFeatEngg/commercialBeforeNPDCalc.csv")
      .withColumn("ES date", to_date(unix_timestamp(col("ES date"),"yyyy-MM-dd").cast("timestamp")))
      .withColumn("Week_End_Date", to_date(col("Week_End_Date")))
      .withColumn("GA date", to_date(unix_timestamp(col("GA date"),"yyyy-MM-dd").cast("timestamp")))
    commercial.printSchema()
    val npd = spark.read.option("header","true").option("inferSchema","true").csv("/etherData/commercialTemp/CommercialFeatEngg/npd.csv")
    val AverageWeeklySales = commercial
      .groupBy("SKU","Reseller_Cluster","Sale_Price","Street Price")
      .agg(mean(col("Qty")).as("POS_Qty"))

    var npdChannelNotRetail = npd.where(col("Channel")=!="Retail")
      .withColumn("Year", year(col("Week_End_Date")).cast("string"))
//      .withColumn("Year_LEVELS", col("year"))
    npdChannelNotRetail = npdChannelNotRetail
      .withColumn("Week", when(col("Week_End_Date").isNull, null).otherwise(extractWeekFromDateUDF(col("Week_End_Date").cast("string"), lit("yyyy-MM-dd"))))
        .cache()
    val npdFilteredL1CategoryDF = npdChannelNotRetail.where(col("L1_Category").isNotNull)
    var seasonalityNPD = npdFilteredL1CategoryDF
      .groupBy("Week","L1_Category")
      .agg(sum("UNITS").as("UNITS"), countDistinct("Year").as("Years"))
      .withColumn("UNITS_average", col("UNITS")/col("Years"))
    ////writeDF(seasonalityNPD,"seasonalityNPD_Start")
    val seasonalityNPDSum = seasonalityNPD
      .groupBy("L1_Category")
      .agg(count("UNITS_average").as("number_weeks"),sum("UNITS_average").as("UNITS_average"))
      .withColumn("average", col("UNITS_average")/col("number_weeks"))
      .drop("UNITS_average","number_weeks")
    ////writeDF(seasonalityNPDSum,"seasonalityNPDSum")
    ////writeDF(seasonalityNPD.join(seasonalityNPDSum, Seq("L1_Category"), "left"),"SEASONALITYNPD_JOIN_NPDSUM")
    seasonalityNPD = seasonalityNPD.join(seasonalityNPDSum, Seq("L1_Category"), "left")
        .withColumn("seasonality_npd", (col("UNITS_average")/col("average"))-lit(1))
        .drop("UNITS","UNITS_average","average","Years")

    commercial = commercial
      .withColumn("Week", extractWeekFromDateUDF(col("Week_End_Date").cast("string"), lit("yyyy-MM-dd")))
    //.withColumn("Week_LEVELS",col("Week"))
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_With_WEEK")
    seasonalityNPD.write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/commercialTemp/CommercialFeatEngg/seasonalityNPD.csv")
    commercial.write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/commercialTemp/CommercialFeatEngg/commercialBeforeIFS2Calc.csv")
  }
}