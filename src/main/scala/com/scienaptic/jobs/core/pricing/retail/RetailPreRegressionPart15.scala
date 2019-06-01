package com.scienaptic.jobs.core.pricing.retail

import java.util.{Calendar, Date}

import com.scienaptic.jobs.ExecutionContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object RetailPreRegressionPart15 {

  def execute(executionContext: ExecutionContext): Unit = {
    val spark: SparkSession = executionContext.spark

    var retailWithCompCannDF  = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("E:\\Scienaptic\\HP\\Pricing\\Data\\CR1\\May31_Run\\spark_out_retail\\retail-NoPromo-SkuCategory-PART14.csv")
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
      .groupBy("trend")
      .agg(min("Week_End_Date").as("minWED"))

    retailWithCompCannDF = retailWithCompCannDF
      .join(retailWithCompCannForTrendDF, Seq("trend"), "left")
      .withColumn("WEDDiff", (datediff(col("Week_End_Date"), col("minWED")) / 7)).drop("minWED")
      .withColumn("trend", col("trend") + col("WEDDiff")).drop("WEDDiff")
      .withColumn("Promo_Pct_Ave", lit(lit(1) - col("ImpAve") / col("Street_Price")))
      .withColumn("Promo_Pct_Min", lit(lit(1) - col("ImpMin") / col("Street_Price")))
      .withColumn("wed_cat", concat_ws(".", col("Week_End_Date"), col("L1_Category")))

    //AVIK Change: Code commented in R
    /*var retailWithCompCannDFtmep1 = retailWithCompCannDF
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
      .drop("z", "w", "wed_cat")*/


    retailWithCompCannDF.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("E:\\Scienaptic\\HP\\Pricing\\Data\\CR1\\May31_Run\\spark_out_retail\\retail-Season-L1L2CannOfflineOnline-PART15.csv")
  }
}
