package com.scienaptic.jobs.core

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.bean.{RetailHoliday, RetailHolidayTranspose, UnionOperation}
import com.scienaptic.jobs.core.RetailPreRegressionPart01._
import com.scienaptic.jobs.utility.CommercialUtility.{createlist, extractWeekFromDateUDF}
import com.scienaptic.jobs.utility.Utils.renameColumns
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}

import scala.collection.mutable

object RetailPreRegressionPart12 {

  val Cat_switch = 1
  val dat2000_01_01 = to_date(unix_timestamp(lit("2000-01-01"), "yyyy-MM-dd").cast("timestamp"))
  val dat9999_12_31 = to_date(unix_timestamp(lit("9999-12-31"), "yyyy-MM-dd").cast("timestamp"))
  val dateFormatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
  val dateFormatterMMddyyyyWithSlash = new SimpleDateFormat("MM/dd/yyyy")
  val dateFormatterMMddyyyyWithHyphen = new SimpleDateFormat("dd-MM-yyyy")
  val maximumRegressionDate = "2018-12-29"
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

    var retailWithCompCannDF = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/retailTemp/RetailFeatEngg/retail-L1L2Cann-PART11.csv")
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("EOL_Date", to_date(unix_timestamp(col("EOL_Date"), "yyyy-MM-dd").cast("timestamp"))).cache()

    var npd = renameColumns(executionContext.spark.read.option("header", "true").option("inferSchema", "true").csv("/etherData/managedSources/NPD/NPD_weekly.csv")).cache()
    npd.columns.toList.foreach(x => {
      npd = npd.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    npd = npd.cache()
      .withColumn("Week_End_Date", when(col("Week_End_Date").isNull || col("Week_End_Date") === "", lit(null)).otherwise(
        when(col("Week_End_Date").contains("-"), to_date(unix_timestamp(col("Week_End_Date"), "dd-MM-yyyy").cast("timestamp")))
          .otherwise(to_date(unix_timestamp(col("Week_End_Date"), "MM/dd/yyyy").cast("timestamp")))
      ))

    var IFS2 = renameColumns(executionContext.spark.read.option("header", "true").option("inferSchema", true).csv("/etherData/managedSources/IFS2/IFS2_most_recent.csv"))
    IFS2.columns.toList.foreach(x => {
      IFS2 = IFS2.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    IFS2 = IFS2
      .withColumn("Valid_Start_Date", to_date(unix_timestamp(col("Valid_Start_Date"), "MM/dd/yyyy").cast("timestamp")))
      .withColumn("Valid_End_Date", to_date(unix_timestamp(col("Valid_End_Date"), "MM/dd/yyyy").cast("timestamp"))).cache()


    var npdChanneRetail = npd.where(col("Channel") === "Retail")
      .withColumn("Year", when(col("Week_End_Date").isNull, null).otherwise(year(col("Week_End_Date")).cast("string")))
      .withColumn("Week", when(col("Week_End_Date").isNull, null).otherwise(extractWeekFromDateUDF(col("Week_End_Date").cast("string"), lit("yyyy-MM-dd")).cast(StringType)))
    val npdFilteredL1CategoryDF = npdChanneRetail.where(col("L1_Category").isNotNull)
    var seasonalityNPD = npdFilteredL1CategoryDF
      .groupBy("Week", "L1_Category")
      .agg(sum("UNITS").as("UNITS"), countDistinct("Year").as("Years"))
      .withColumn("UNITS_average", col("UNITS") / col("Years"))

    val seasonalityNPDSum = seasonalityNPD
      .groupBy("L1_Category")
      .agg(count("UNITS_average").as("number_weeks"), sum("UNITS_average").as("UNITS_average"))
      .withColumn("average", col("UNITS_average") / col("number_weeks"))
      .drop("UNITS_average", "number_weeks")

    seasonalityNPD = seasonalityNPD.join(seasonalityNPDSum, Seq("L1_Category"), "left")
      .withColumn("seasonality_npd", (col("UNITS_average") / col("average")) - lit(1))
      .drop("UNITS", "UNITS_average", "average", "Years")


    retailWithCompCannDF = retailWithCompCannDF
      .withColumn("Week", when(col("Week_End_Date").isNull, null).otherwise(extractWeekFromDateUDF(col("Week_End_Date").cast("string"), lit("yyyy-MM-dd")).cast(StringType)))
      .join(seasonalityNPD, Seq("L1_Category", "Week"), "left").drop("Week")
      .withColumn("seasonality_npd2", when((col("USCyberMonday") === lit(1)) || (col("USThanksgivingDay") === lit(1)), 0).otherwise(col("seasonality_npd").cast(IntegerType)))
      .withColumn("seasonality_npd2", when(col("PL") === "4X", lit(1)).otherwise(col("seasonality_npd2")))


    retailWithCompCannDF = retailWithCompCannDF
      .join(IFS2.filter(col("Account").isin("Amazon-Proper", "Best Buy", "Office Depot-Max", "Staples", "Costco", "Sam's Club", "HP Shopping", "Walmart"))
        .select("SKU", "Account", "Street_Price", "Hardware_GM", "Supplies_GM", "Hardware_Rev", "Supplies_Rev", "Valid_Start_Date", "Valid_End_Date"), Seq("SKU", "Account", "Street_Price"), "left")
      .withColumn("Valid_Start_Date", when(col("Valid_Start_Date").isNull, dat2000_01_01).otherwise(col("Valid_Start_Date")))
      .withColumn("Valid_End_Date", when(col("Valid_End_Date").isNull, dat9999_12_31).otherwise(col("Valid_End_Date")))
      //      .withColumn("Street_Price", when(col("Street_Price").isNull, col("Street_Price_Org")).otherwise(col("Street_Price")))
      .where((col("Week_End_Date") >= col("Valid_Start_Date")) && (col("Week_End_Date") < col("Valid_End_Date")))

    val aveGM = retailWithCompCannDF.dropDuplicates("SKU", "Account")
      .groupBy("SKU")
      .agg(mean(col("Hardware_GM")).as("aveHWGM"),
        mean(col("Supplies_GM")).as("aveSuppliesGM"))

    //    aveGM.coalesce(1).write.mode(SaveMode.Overwrite).option("header", true).csv("D:\\files\\temp\\retail-r-1419.csv")

    retailWithCompCannDF = retailWithCompCannDF
      .join(aveGM, Seq("SKU"), "left")
      .withColumn("Hardware_GM", when(col("Hardware_GM").isNull, col("aveHWGM")).otherwise(col("Hardware_GM")))
      .withColumn("Hardware_GM", when(col("aveHWGM").isNull, lit(null).cast(StringType)).otherwise(col("Hardware_GM")))
      .withColumn("Supplies_GM", when(col("Supplies_GM").isNull, col("aveSuppliesGM")).otherwise(col("Supplies_GM")))
      .withColumn("Supplies_GM", when(col("aveSuppliesGM").isNull, lit(null).cast(StringType)).otherwise(col("Supplies_GM")))
      .withColumn("Supplies_GM", when(col("PL") === "4X", 0).otherwise(col("Supplies_GM")))

    // write
    retailWithCompCannDF.write.mode(SaveMode.Overwrite).option("header", true).csv("/etherData/retailTemp/RetailFeatEngg/retail-Seasonality-Hardware-PART12.csv")

  }
}
