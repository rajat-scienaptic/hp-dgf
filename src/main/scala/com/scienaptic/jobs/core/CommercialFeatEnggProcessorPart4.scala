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

    //val baselineThreshold = if (min_baseline/2 > 0) min_baseline/2 else 0
    var commercial = spark.read.option("header","true").option("inferSchema","true").csv("/etherData/commercialTemp/CommercialFeatEngg/commercialBeforeNPDCalc.csv")
    //var commercial = spark.read.option("header","true").option("inferSchema","true").csv("E:\\Scienaptic\\HP\\Pricing\\R\\SPARK_DEBUG_OUTPUTS\\commercialBeforeNPDCalc.csv")
      .withColumn("ES date", to_date(unix_timestamp(col("ES date"),"yyyy-MM-dd").cast("timestamp")))
      .withColumn("Week_End_Date", to_date(col("Week_End_Date")))
      .withColumn("GA date", to_date(unix_timestamp(col("GA date"),"yyyy-MM-dd").cast("timestamp")))
      /*Avik Change Apr 13: Limiting precision values for numerical values, as being used in group*/
      .withColumn("Sale_Price", round(col("Sale_Price"), 2))
      .withColumn("Street Price", round(col("Street Price"), 2))
    commercial.printSchema()
    //var npdDF = spark.read.option("header","true").option("inferSchema","true").csv("E:\\Scienaptic\\HP\\Pricing\\Data\\April8Run_Inputs\\NPD_weekly.csv")
    var npdDF = spark.read.option("header", "true").option("inferSchema", "true").csv("/etherData/managedSources/NPD/NPD_weekly.csv")
    var npd = renameColumns(npdDF)
    npd.columns.toList.foreach(x => {
      npd = npd.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    npd = npd.withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "MM/dd/yyyy").cast("timestamp"))).cache()
    //val npd = spark.read.option("header","true").option("inferSchema","true").csv("E:\\Scienaptic\\HP\\Pricing\\R\\SPARK_DEBUG_OUTPUTS\\npd.csv")
    //val npd = spark.read.option("header","true").option("inferSchema","true").csv("/etherData/commercialTemp/CommercialFeatEngg/npd.csv")
    val AverageWeeklySales = commercial
      .groupBy("SKU","Reseller_Cluster","Sale_Price","Street Price")
      .agg(mean(col("Qty")).as("POS_Qty"))

    var npdChannelNotRetail = npd.where(col("Channel")=!="Retail")
      .withColumn("Year", year(col("Week_End_Date")).cast("string"))
    npdChannelNotRetail = npdChannelNotRetail
      .withColumn("Week", when(col("Week_End_Date").isNull, null).otherwise(extractWeekFromDateUDF(col("Week_End_Date").cast("string"), lit("yyyy-MM-dd"))))
      /* Avik Change Apr 13: UDF ambiguous for years 2014, 2018, 2022  */
      //.withColumn("Week",when((col("Year").cast("string")==="2014") || (col("Year").cast("string")==="2018"), col("Week")+lit(1)).otherwise(col("Week")))
      .cache()
    val npdFilteredL1CategoryDF = npdChannelNotRetail.where(col("L1_Category").isNotNull)
    var seasonalityNPD = npdFilteredL1CategoryDF
      .groupBy("Week","L1_Category")
      .agg(sum("UNITS").as("UNITS"), countDistinct("Year").as("Years"))
      .withColumn("UNITS_average", col("UNITS")/col("Years"))
    //writeDF(seasonalityNPD,"Line544_Spark")
    val seasonalityNPDSum = seasonalityNPD
      .groupBy("L1_Category")
      .agg(count("L1_Category").as("number_weeks"),sum("UNITS_average").as("UNITS_average"))
      .withColumn("average", col("UNITS_average")/col("number_weeks"))
    //writeDF(seasonalityNPDSum,"seasonalityNPDSum")
    seasonalityNPD = seasonalityNPD.join(seasonalityNPDSum.drop("UNITS_average","number_weeks"), Seq("L1_Category"), "left")
        .withColumn("seasonality_npd", (col("UNITS_average")/col("average"))-lit(1))
        .drop("UNITS","UNITS_average","average","Years")
    //writeDF(seasonalityNPD,"Line577_Spark")
    commercial = commercial
      .withColumn("Week", extractWeekFromDateUDF(col("Week_End_Date").cast("string"), lit("yyyy-MM-dd")))
        .drop("Year")
    //writeDF(commercial,"commercialBeforeIFS2Calc")
    //writeDF(seasonalityNPD,"seasonalityNPD")
    seasonalityNPD.write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/commercialTemp/CommercialFeatEngg/seasonalityNPD.csv")
    commercial.write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/commercialTemp/CommercialFeatEngg/commercialBeforeIFS2Calc.csv")
  }
}