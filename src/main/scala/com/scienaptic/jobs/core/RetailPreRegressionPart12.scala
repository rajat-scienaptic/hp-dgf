package com.scienaptic.jobs.core

import java.util.{Calendar, Date}

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.core.RetailPreRegressionPart11.monthDateFormat
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.collection.mutable

object RetailPreRegressionPart12 {

  val getMonthNumberFromString = udf((month: String) => {
    val date: Date = monthDateFormat.parse(month)
    val cal: Calendar = Calendar.getInstance
    cal.setTime(date)
    cal.get(Calendar.MONTH)
  })

  val stability_weeks = 4
  val stability_range = 0.7
  val intro_weeks = 8
  val min_baseline = 5
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

    var retailWithCompCannDF  = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/retailTemp/RetailFeatEngg/retail-NoPromo-SkuCategory-PART11.csv")
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("EOL_Date", to_date(unix_timestamp(col("EOL_Date"), "yyyy-MM-dd").cast("timestamp"))).cache()
    val maxWED = retailWithCompCannDF.agg(max("Week_End_Date")).head().getDate(0)
    val maxWEDSeason = retailWithCompCannDF.where(col("Week_End_Date") === maxWED).sort(col("Week_End_Date").desc).select("Season").head().getString(0)
    val latestSeasonRetail = retailWithCompCannDF.where(col("Season") === maxWEDSeason)

    val windForSeason = Window.orderBy(col("Week_End_Date").desc)
    val uniqueSeason = retailWithCompCannDF
      .withColumn("rank", row_number().over(windForSeason))
      .where(col("rank") === 2).select("Season").head().getString(0)

    val latestSeason = latestSeasonRetail.select("Week_End_Date").distinct().count()
    if (latestSeason < 13) {
      retailWithCompCannDF = retailWithCompCannDF
        .withColumn("Season_most_recent", when(col("Season") === maxWEDSeason, uniqueSeason).otherwise(col("Season")))
      //ifelse(commercial$Season eq unique(commercial$Season(order(commercial$Week.End.Date)))(length(unique(commercial$Season))), as.character(unique(commercial$Season(order(commercial$Week.End.Date)))(length(unique(commercial$Season)) - 1)), as.character(commercial$Season))
    } else {
      retailWithCompCannDF = retailWithCompCannDF
        .withColumn("Season_most_recent", col("Season"))
    }

    retailWithCompCannDF = retailWithCompCannDF
      .withColumn("trend", lit(1))


    //    retailWithCompCannDF.coalesce(1).write.mode(SaveMode.Overwrite).option("header", true).csv("D:\\files\\temp\\retail-r-1500.csv")

    val retailWithCompCannForTrendDF = retailWithCompCannDF
      .groupBy("Week_End_Date")
      .agg(min("Week_End_Date").as("minWED"))
      .withColumn("WEDDiff", (datediff(col("Week_End_Date"), col("minWED")) / 7)).drop("minWED")

    retailWithCompCannDF = retailWithCompCannDF.withColumn("Week_End_Date", col("Week_End_Date"))
      .join(retailWithCompCannForTrendDF.withColumn("Week_End_Date", col("Week_End_Date")), Seq("Week_End_Date"), "left")
      .withColumn("trend", col("trend") + col("WEDDiff")).drop("WEDDiff")
      .withColumn("Promo_Pct_Ave", lit(lit(1) - col("ImpAve") / col("Street_Price")))
      .withColumn("Promo_Pct_Min", lit(lit(1) - col("ImpMin") / col("Street_Price")))
      .withColumn("wed_cat", concat_ws(".", col("Week_End_Date"), col("L1_Category")))

    var retailWithCompCannDFtmep1 = retailWithCompCannDF
      .groupBy("wed_cat")
      .agg(sum(col("Promo_Pct") * col("POS_Qty")).as("z"), sum(col("POS_Qty")).as("w"))

    retailWithCompCannDF = retailWithCompCannDF.join(retailWithCompCannDFtmep1, Seq("wed_cat"), "left")
      .withColumn("L1_cannibalization_OnOffline_Min", (col("z") - (col("Promo_Pct") * col("POS_Qty"))) / (col("w") - col("POS_Qty")))
      .drop("z", "w", "wed_cat")
      .withColumn("wed_cat", concat_ws(".", col("Week_End_Date"), col("L2_Category")))


    var retailWithCompetitionDFtmep2 = retailWithCompCannDF
      .groupBy("wed_cat")
      .agg(sum(col("Promo_Pct") * col("POS_Qty")).as("z"), sum(col("POS_Qty")).as("w"))

    retailWithCompCannDF = retailWithCompCannDF.join(retailWithCompetitionDFtmep2, Seq("wed_cat"), "left")
      .withColumn("L2_cannibalization_OnOffline_Min", (col("z") - (col("Promo_Pct") * col("POS_Qty"))) / (col("w") - col("POS_Qty")))
      .drop("z", "w", "wed_cat")


    retailWithCompCannDF.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("/etherData/retailTemp/RetailFeatEngg/retail-Season-L1L2CannOfflineOnline-PART12.csv")
  }
}
