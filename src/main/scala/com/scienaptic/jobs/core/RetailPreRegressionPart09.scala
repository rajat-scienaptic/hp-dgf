package com.scienaptic.jobs.core

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.bean.{RetailHoliday, RetailHolidayTranspose, UnionOperation}
import com.scienaptic.jobs.core.RetailPreRegressionPart01.{checkPrevDistInvGTBaseline, concatenateRankWithDist, stability_range}
import com.scienaptic.jobs.utility.CommercialUtility.createlist
import com.scienaptic.jobs.utility.Utils.renameColumns
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

import scala.collection.mutable

object RetailPreRegressionPart09 {

  val Cat_switch = 1
  val dat2000_01_01 = to_date(unix_timestamp(lit("2000-01-01"), "yyyy-MM-dd").cast("timestamp"))
  val dat9999_12_31 = to_date(unix_timestamp(lit("9999-12-31"), "yyyy-MM-dd").cast("timestamp"))
  val dateFormatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
  val dateFormatterMMddyyyyWithSlash = new SimpleDateFormat("MM/dd/yyyy")
  val dateFormatterMMddyyyyWithHyphen = new SimpleDateFormat("dd-MM-yyyy")
  val maximumRegressionDate = "2019-03-09"
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

  var retailWithCompetitionDF  = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/retailTemp/RetailFeatEngg/retail-L1L2-PART08.csv")
    .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
    .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
    .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
    .withColumn("EOL_Date", to_date(unix_timestamp(col("EOL_Date"), "yyyy-MM-dd").cast("timestamp"))).cache()

    val retailBrandinHP = retailWithCompetitionDF.where(col("Brand").isin("HP"))
      .withColumn("POSQty_pmax", greatest(col("POS_Qty"), lit(0)))

    val HPComp1 = retailBrandinHP
      .groupBy("Week_End_Date", "L1_Category", "Account")
      .agg(sum("POSQty_pmax").as("sum2"), (sum(col("Promo_Pct") * col("POSQty_pmax"))).as("sum1"))
      .withColumn("sum1", when(col("sum1") < 0, 0).otherwise(col("sum1")))
      .withColumn("sum2", when(col("sum2") < 0, 0).otherwise(col("sum2")))
      .withColumn("L1_competition_HP_ssmodel", col("sum1") / col("sum2"))
      .drop("sum1", "sum2", "temp_sum1", "temp_sum2")
    //.join(commercialBrandinHP, Seq("Week_End_Date","L1_Category"), "right")

    val HPComp2 = retailBrandinHP
      .groupBy("Week_End_Date", "L2_Category", "Account")
      .agg(sum("POSQty_pmax").as("sum2"), (sum(col("Promo_Pct") * col("POSQty_pmax"))).as("sum1"))
      .withColumn("sum1", when(col("sum1") < 0, 0).otherwise(col("sum1")))
      .withColumn("sum2", when(col("sum2") < 0, 0).otherwise(col("sum2")))
      .withColumn("L2_competition_HP_ssmodel", col("sum1") / col("sum2"))
      .drop("sum1", "sum2", "temp_sum1", "temp_sum2")

    retailWithCompetitionDF = retailWithCompetitionDF.withColumn("L1_Category", col("L1_Category"))
      .join(HPComp1, Seq("Week_End_Date", "L1_Category", "Account"), "left")

    // write
    //    retailWithCompetitionDF.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("D:\\files\\temp\\retail-Feb06-r-1168.csv")

    retailWithCompetitionDF = retailWithCompetitionDF
      .join(HPComp2, Seq("Week_End_Date", "L2_Category", "Account"), "left")

    // write
    //    retailWithCompetitionDF.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("D:\\files\\temp\\retail-Feb06-r-1171.csv")

    retailWithCompetitionDF = retailWithCompetitionDF
      .withColumn("L1_competition_HP_ssmodel", when((col("L1_competition_HP_ssmodel").isNull) || (col("L1_competition_HP_ssmodel") < 0), 0).otherwise(col("L1_competition_HP_ssmodel")))
      .withColumn("L2_competition_HP_ssmodel", when((col("L2_competition_HP_ssmodel").isNull) || (col("L2_competition_HP_ssmodel") < 0), 0).otherwise(col("L2_competition_HP_ssmodel")))
      .na.fill(0, Seq("L1_competition_HP_ssmodel", "L2_competition_HP_ssmodel"))

    // write
    //    retailWithCompetitionDF.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("D:\\files\\temp\\retail-Feb06-r-1184.csv")

    val ODOMHPmodel1 = retailWithCompetitionDF
      .filter(col("Brand").isin("Samsung") && col("Account").isin("Office Depot-Max", "Amazon-Proper"))
      .withColumn("temp_sum2", greatest(col("POS_Qty"), lit(0)))
      .withColumn("temp_sum1", col("Promo_Pct") * greatest(col("POS_Qty"), lit(0)))
      .groupBy("Week_End_Date", "L1_Category", "Account")
      .agg(sum("temp_sum2").as("sum2"), (sum("temp_sum1").as("sum1")))
      .withColumn("L1_competition_ssdata_HPmodel", col("sum1") / col("sum2"))
      .drop("sum1", "sum2", "temp_sum1", "temp_sum2")

    val ODOMHPmodel2 = retailWithCompetitionDF
      .filter(col("Brand").isin("Samsung") && col("Account").isin("Office Depot-Max", "Amazon-Proper"))
      .withColumn("temp_sum2", greatest(col("POS_Qty"), lit(0)))
      .withColumn("temp_sum1", col("Promo_Pct") * greatest(col("POS_Qty"), lit(0)))
      .groupBy("Week_End_Date", "L2_Category", "Account")
      .agg(sum("temp_sum2").as("sum2"), (sum("temp_sum1").as("sum1")))
      .withColumn("L2_competition_ssdata_HPmodel", col("sum1") / col("sum2"))
      .drop("sum1", "sum2", "temp_sum1", "temp_sum2")

    retailWithCompetitionDF = retailWithCompetitionDF
      .join(ODOMHPmodel1, Seq("Week_End_Date", "L1_Category", "Account"), "left")
      .join(ODOMHPmodel2, Seq("Week_End_Date", "L2_Category", "Account"), "left")
      .withColumn("L1_competition_ssdata_HPmodel", when((col("L1_competition_ssdata_HPmodel").isNull) || (col("L1_competition_ssdata_HPmodel") < 0), 0).otherwise(col("L1_competition_ssdata_HPmodel")))
      .withColumn("L2_competition_ssdata_HPmodel", when((col("L2_competition_ssdata_HPmodel").isNull) || (col("L2_competition_ssdata_HPmodel") < 0), 0).otherwise(col("L2_competition_ssdata_HPmodel")))
      .na.fill(0, Seq("L1_competition_ssdata_HPmodel", "L2_competition_ssdata_HPmodel"))


    // write
    retailWithCompetitionDF.write.option("header", true).mode(SaveMode.Overwrite).csv("/etherData/retailTemp/RetailFeatEngg/retail-L1L2-HP-PART09.csv")

  }
}
