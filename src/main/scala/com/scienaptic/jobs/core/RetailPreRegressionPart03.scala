package com.scienaptic.jobs.core

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale, UUID}

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.bean.{RetailHoliday, RetailHolidayTranspose, UnionOperation}
import com.scienaptic.jobs.bean.UnionOperation.doUnion
import com.scienaptic.jobs.utility.CommercialUtility.{addDaystoDateStringUDF, concatenateRank, createlist, extractWeekFromDateUDF}
import com.scienaptic.jobs.utility.Utils.renameColumns
import org.apache.spark
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType}

import scala.collection.mutable
object RetailPreRegressionPart03 {

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
    import spark.implicits._

    var retailJoincompAdTotalDFDF  = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/retailTemp/RetailFeatEngg/retail-r-retailJoincompAdTotalDFDF-PART02.csv")
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))

    var calendar = renameColumns(executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/managedSources/Calendar/Retail/master_calendar_retail.csv")).cache()
    calendar.columns.toList.foreach(x => {
      calendar = calendar.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    calendar = calendar.cache()
      .filter(!col("Account").isin("Rest of Retail"))
      .withColumn("Account", when(col("Account").isin("Amazon.Com"), "Amazon-Proper").otherwise(col("Account")))
      //      .withColumn("Week_End_Date", when(col("Week_End_Date").isNull || col("Week_End_Date") === "", null).otherwise(to_date(unix_timestamp(convertFaultyDateFormat(col("Week_End_Date")), "yyyy-MM-dd").cast("timestamp"))))
      //      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "MM/dd/yyyy").cast("timestamp")))
      .withColumn("NP_IR_original", col("NP_IR"))
      .withColumn("ASP_IR_original", col("ASP_IR"))
      .withColumn("Week_End_Date", when(col("Week_End_Date").isNull || col("Week_End_Date") === "", lit(null)).otherwise(
        when(col("Week_End_Date").contains("-"), to_date(unix_timestamp(col("Week_End_Date"), "dd-MM-yyyy").cast("timestamp")))
          .otherwise(to_date(unix_timestamp(col("Week_End_Date"), "MM/dd/yyyy").cast("timestamp")))
      ))
      .drop("Season")


    var retailJoinCalendarDF = retailJoincompAdTotalDFDF
      .join(calendar, Seq("Account", "SKU", "Week_End_Date"), "left")
      .withColumn("Merchant_Gift_Card", when(col("Merchant_Gift_Card").isNull, 0).otherwise(col("Merchant_Gift_Card")))
      .withColumn("Flash_IR", when(col("Online") === 0, 0).otherwise(col("Flash_IR")))
      .withColumn("Flash_IR", when(col("Flash_IR").isNull, 0).otherwise(col("Flash_IR")))

    // write
    //    retailJoinCalendarDF.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("D:\\files\\temp\\retail-Feb06-r-371.csv")

    val uniqueSKUNames = retailJoinCalendarDF.filter(col("Merchant_Gift_Card") > 0).select("SKU_Name").distinct().collect().map(_ (0).asInstanceOf[String]).toList

    retailJoinCalendarDF = retailJoinCalendarDF.withColumn("GC_SKU_Name", when(col("SKU_Name").isin(uniqueSKUNames: _*), col("SKU_Name").cast("string"))
      .otherwise(lit("NA")))

    val SKUWhoChange = retailJoinCalendarDF.filter(col("Changed_Street_Price") =!= 0).select("SKU").distinct().collect().map(_ (0)).toList

    // TODO done : check col data type for pmax UDF
    retailJoinCalendarDF = retailJoinCalendarDF.withColumn("GAP_IR", when((col("SKU").isin(SKUWhoChange: _*)) && (col("Season").isin("BTB'18")), col("NP_IR"))
      .otherwise(col("GAP_IR")))
      .withColumn("GAP_IR", when(col("GAP_IR").isNull, 0).otherwise(col("GAP_IR")))
      .withColumn("Other_IR_original", when(col("NP_IR_original").isNull && col("ASP_IR_original").isNull, col("GAP_IR")).otherwise(0))
      .withColumn("NP_IR_original", when(col("NP_IR_original").isNull, 0).otherwise(col("NP_IR_original")))
      .withColumn("ASP_IR_original", when(col("ASP_IR_original").isNull, 0).otherwise(col("ASP_IR_original")))
      .withColumn("Total_IR_original", greatest(col("NP_IR_original"), col("ASP_IR_original"), col("Other_IR_original")))
      .withColumn("NP_IR_original", when(col("Flash_IR") > 0, col("Flash_IR")).otherwise(col("NP_IR_original")))
      .withColumn("NP_IR", col("NP_IR_original"))
      .withColumn("Total_IR", greatest(col("NP_IR_original"), col("ASP_IR_original"), col("GAP_IR")))
      .withColumn("ASP_IR", when(col("ASP_IR_original") >= (col("Total_IR") - col("NP_IR"))
        , col("Total_IR") - col("NP_IR")).otherwise(col("ASP_IR_original")))
      .withColumn("Other_IR", col("Total_IR") - (col("NP_IR") + col("ASP_IR")))
      .withColumn("Ad", when(col("Total_IR") === 0, 0).otherwise(col("Ad")))

    // write
    //        retailJoinCalendarDF.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("D:\\files\\temp\\retail-Feb07-r-424.csv")
    var masterSprintCalendar = renameColumns(executionContext.spark.read.option("header", "true").option("inferSchema", "true").csv("/etherData/managedSources/S-Print/Master_Calendar/Master_Calender_s-print.csv")).cache()
    masterSprintCalendar.columns.toList.foreach(x => {
      masterSprintCalendar = masterSprintCalendar.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    masterSprintCalendar = masterSprintCalendar.cache()
      .select("Account", "SKU", "Rebate_SS", "Week_End_Date")
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "MM/dd/yyyy").cast("timestamp")))

    val retailJoinMasterSprintCalendarDF = retailJoinCalendarDF
      .join(masterSprintCalendar, Seq("SKU", "Account", "Week_End_Date"), "left")
      .withColumn("NP_IR", when(col("Brand").isin("Samsung"), col("Rebate_SS")).otherwise(col("NP_IR")))
      //      .withColumn("NP_IR", when(col("Rebate_SS").isNull, null).otherwise(col("NP_IR")))
      .withColumn("NP_IR", when(col("NP_IR").isNull, 0).otherwise(col("NP_IR")))
      .drop("Rebate_SS")

    var aggUpstream = renameColumns(executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/managedSources/Upstream/AggUpstream.csv")).cache()
    aggUpstream.columns.toList.foreach(x => {
      aggUpstream = aggUpstream.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    aggUpstream = aggUpstream.cache()
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("Ave", when(col("Ave") === "NA" || col("Ave") === "", null).otherwise(col("Ave")))
      .withColumn("Min", when(col("Min") === "NA" || col("Min") === "", null).otherwise(col("Min")))
      .withColumn("Max", when(col("Max") === "NA" || col("Max") === "", null).otherwise(col("Max")))

    val focusedAccounts = List("HP Shopping", "Amazon-Proper", "Best Buy", "Office Depot-Max", "Staples")

    val retailJoinAggUpstreamWithNATreatmentDF = aggUpstream
      .join(retailJoinMasterSprintCalendarDF, Seq("SKU", "Account", "Week_End_Date", "Online"), "right")
      //Avik Change: When Total IR is null, it should give Street price as sale price
      .withColumn("Total_IR", when(col("Total_IR").isNull, 0).otherwise(col("Total_IR")))
      .withColumn("GAP_Price", col("Street_Price") - col("Total_IR"))
      .withColumn("ImpAve", when((col("Ave").isNull) && (col("GAP_Price").isNotNull), col("GAP_Price")).otherwise(col("Ave")))
      .withColumn("ImpMin", when((col("Min").isNull) && (col("GAP_Price").isNotNull), col("GAP_Price")).otherwise(col("Min")))
      .withColumn("NoAvail", when(col("InStock").isNull && col("OnlyInStore").isNull && col("OutofStock").isNull && col("DelayDel").isNull, 1).otherwise(0))
      .withColumn("NoAvail", when(col("POS_Qty") > 0 && col("OutofStock").isin(7), 1).otherwise(col("NoAvail")))
      .withColumn("OutofStock", when(col("POS_Qty") > 0 && col("OutofStock").isin(7), 0).otherwise(col("OutofStock")))
      .withColumn("InStock", when(col("InStock").isNull, 0).otherwise(col("InStock")))
      .withColumn("DelayDel", when(col("DelayDel").isNull, 0).otherwise(col("DelayDel")))
      .withColumn("OutofStock", when(col("OutofStock").isNull, 0).otherwise(col("OutofStock")))
      .withColumn("OnlyInStore", when(col("OnlyInStore").isNull, 0).otherwise(col("OnlyInStore")))
      .withColumn("NoAvail", when(col("NoAvail").isNull, 0).otherwise(col("NoAvail")))

    var retailJoinAggUpstreamDF = retailJoinAggUpstreamWithNATreatmentDF
      .filter(col("Account").isin(focusedAccounts: _*))

    // remove write
      retailJoinAggUpstreamWithNATreatmentDF.write.mode(SaveMode.Overwrite).option("header", true).csv("/etherData/retailTemp/RetailFeatEngg/retail-retailJoinAggUpstreamWithNATreatmentDF-PART03.csv")
      retailJoinAggUpstreamDF.write.mode(SaveMode.Overwrite).option("header", true).csv("/etherData/retailTemp/RetailFeatEngg/retail-retailJoinAggUpstreamDF-PART03.csv")

  }
}
