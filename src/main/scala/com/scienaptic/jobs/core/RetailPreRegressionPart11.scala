package com.scienaptic.jobs.core

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.Utils.renameColumns
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable

object RetailPreRegressionPart11 {

  val Cat_switch = 1
  val dat2000_01_01 = to_date(unix_timestamp(lit("2000-01-01"), "yyyy-MM-dd").cast("timestamp"))
  val dat9999_12_31 = to_date(unix_timestamp(lit("9999-12-31"), "yyyy-MM-dd").cast("timestamp"))
  val dateFormatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
  val dateFormatterMMddyyyyWithSlash = new SimpleDateFormat("MM/dd/yyyy")
  val dateFormatterMMddyyyyWithHyphen = new SimpleDateFormat("dd-MM-yyyy")
  val maximumRegressionDate = "2050-01-01"
  val minimumRegressionDate = "2014-01-01"
  val monthDateFormat = new SimpleDateFormat("MMM", Locale.ENGLISH)

  val stability_weeks = 4
  val stability_range = 0.7
  val intro_weeks = 8
  val min_baseline = 5
  val baselineThreshold = if (min_baseline / 2 > 0) min_baseline / 2 else 0

  val indexerForAdLocation = new StringIndexer().setInputCol("Ad_Location").setOutputCol("Ad_Location_fact")
  val pipelineForForAdLocation = new Pipeline().setStages(Array(indexerForAdLocation))

  val convertFaultyDateFormat = udf((dateStr: String) => {
    try {
      if (dateStr.contains("-")) {
        dateFormatter.format(dateFormatterMMddyyyyWithHyphen.parse(dateStr))
      }
      else {
        dateFormatter.format(dateFormatterMMddyyyyWithSlash.parse(dateStr))
      }
    } catch {
      case _: Exception => dateStr
    }
  })

  val pmax = udf((col1: Double, col2: Double, col3: Double) => math.max(col1, math.max(col2, col3)))
  val pmax2 = udf((col1: Double, col2: Double) => math.max(col1, col2))
  val pmin = udf((col1: Double, col2: Double, col3: Double) => math.min(col1, math.min(col2, col3)))
  val pmin2 = udf((col1: Double, col2: Double) => math.min(col1, col2))

  val getMonthNumberFromString = udf((month: String) => {
    val date: Date = monthDateFormat.parse(month)
    val cal: Calendar = Calendar.getInstance
    cal.setTime(date)
    cal.get(Calendar.MONTH)
  })

  val concatenateRankWithDist = udf((x: mutable.WrappedArray[String]) => {
    //val concatenateRank = udf((x: List[List[Any]]) => {
    try {
      //      val sortedList = x.map(x => x.getAs[Int](0).toString + "." + x.getAs[Double](1).toString).sorted
      val sortedList = x.toList.map(x => (x.split("_")(0).toInt, x.split("_")(1).toDouble))
      sortedList.sortBy(x => x._1).map(x => x._2.toDouble)
    } catch {
      case _: Exception => null
    }
  })

  val checkPrevDistInvGTBaseline = udf((distributions: mutable.WrappedArray[Double], rank: Int, distribution: Double) => {
    var totalGt = 0
    if (rank <= stability_weeks)
      0
    else {
      val start = rank - stability_weeks - 1
      for (i <- start until rank - 1) {

        // checks if every distribution's abs value is less than the stability range
        if (math.abs(distributions(i) - distribution) <= stability_range) {
          totalGt += 1
        }
      }
      if (totalGt >= 1)
        1
      else
        0
    }
  })

  def execute(executionContext: ExecutionContext): Unit = {
    val spark: SparkSession = executionContext.spark


    var retailWithCompetitionDF = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/retailTemp/RetailFeatEngg/retail-L1L2Cann-half-PART10.csv")
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("EOL_Date", to_date(unix_timestamp(col("EOL_Date"), "yyyy-MM-dd").cast("timestamp"))).cache()

    var retailWithCompCannDF = retailWithCompetitionDF

    //DON'T remove join
    val retailWithAdj = retailWithCompCannDF.withColumn("Adj_Qty", when(col("POS_Qty") <= 0, 0).otherwise(col("POS_Qty")))
    val retailGroupWEDSKU = retailWithAdj.groupBy("Week_End_Date", "SKU")
      .agg(sum(col("Promo_Pct") * col("Adj_Qty")).as("sumSKU1"), sum("Adj_Qty").as("sumSKU2"))
      .withColumn("Week_End_Date", col("Week_End_Date"))
      .join(retailWithAdj.withColumn("Week_End_Date", col("Week_End_Date")), Seq("Week_End_Date", "SKU"), "right")

    //write
    //    retailGroupWEDSKU.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("D:\\files\\temp\\retail-Feb06-r-1161.csv")

    val retailGroupWEDL1Temp = retailGroupWEDSKU
      .groupBy("Week_End_Date", "Brand", "L1_Category")
      .agg(sum(col("Promo_Pct") * col("Adj_Qty")).as("sum1"), sum("Adj_Qty").as("sum2"))

    val retailGroupWEDL1 = retailGroupWEDSKU.withColumn("L1_Category", col("L1_Category"))
      .join(retailGroupWEDL1Temp.withColumn("L1_Category", col("L1_Category")), Seq("Week_End_Date", "Brand", "L1_Category"), "left")
      .withColumn("L1_cannibalization", (col("sum1") - col("sumSKU1")) / (col("sum2") - col("sumSKU2"))) // chenged the name to L2_cannibalization
      .drop("sum1", "sum2")
    //    retailGroupWEDSKU.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("D:\\files\\temp\\retail-Feb06-r-1166.csv")

    val retailGroupWEDL1Temp2 = retailGroupWEDL1
      .groupBy("Week_End_Date", "Brand", "L2_Category")
      .agg(sum(col("Promo_Pct") * col("Adj_Qty")).as("sum1"), sum("Adj_Qty").as("sum2"))

    //    retailWithCompCannDF.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("D:\\files\\temp\\retail-Feb06-r-1172.csv")
    retailWithCompCannDF = retailGroupWEDL1.withColumn("L2_Category", col("L2_Category"))
      .join(retailGroupWEDL1Temp2.withColumn("L2_Category", col("L2_Category")), Seq("Week_End_Date", "Brand", "L2_Category"), "left")
      .withColumn("L2_cannibalization", (col("sum1") - col("sumSKU1")) / (col("sum2") - col("sumSKU2")))
      .drop("sum1", "sum2", "sumSKU1", "sumSKU2", "Adj_Qty")
      .withColumn("L1_cannibalization", when(col("L1_cannibalization").isNull || col("L1_cannibalization") === "", 0).otherwise(col("L1_cannibalization")))
      .withColumn("L2_cannibalization", when(col("L2_cannibalization").isNull || col("L2_cannibalization") === "", 0).otherwise(col("L2_cannibalization")))
      .na.fill(0, Seq("L2_cannibalization", "L1_cannibalization"))

    retailWithCompCannDF = retailWithCompCannDF
      //Avik Change: When Total IR is null, it should give Street price as sale price
      .withColumn("Total_IR", when(col("Total_IR").isNull, 0).otherwise(col("Total_IR")))
      .withColumn("Sale_Price", col("Street_Price") - col("Total_IR"))
      .withColumn("Price_Range_20_Perc_high", lit(1.2) * col("Sale_Price"))
    retailWithCompCannDF.write.option("header", true).mode(SaveMode.Overwrite).csv("/etherData/retailTemp/RetailFeatEngg/retail-L1L2Cann-PART11.csv")

  }
}
