package com.scienaptic.jobs.core

import java.util.{Calendar, Date}

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.core.RetailPreRegressionPart11.monthDateFormat
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object RetailPreRegressionPart14 {

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

    var retailGroupWEDL1InnerCompCann3  = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/retailTemp/RetailFeatEngg/retail-L1L2InnerCann-PART13.csv")
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("EOL_Date", to_date(unix_timestamp(col("EOL_Date"), "yyyy-MM-dd").cast("timestamp"))).cache()

    // Modify Cannibalization 3 ####
    val retailWithAdj3 = retailGroupWEDL1InnerCompCann3.withColumn("Adj_Qty", when(col("POS_Qty") <= 0, 0).otherwise(col("POS_Qty")))
    val retailGroupWEDSKUOnline2 = retailWithAdj3.groupBy("Week_End_Date", "SKU", "Online")
      .agg(sum(col("Promo_Pct_Min") * col("Adj_Qty")).as("sumSKU1"), sum("Adj_Qty").as("sumSKU2"))
      .join(retailWithAdj3, Seq("Week_End_Date", "SKU", "Online"), "right")

    val retailGroupWEDSKUPriceBrandTemp1 = retailGroupWEDSKUOnline2
      .groupBy("Week_End_Date", "Online", "PriceBand", "Brand")
      .agg(sum(col("Promo_Pct_Min") * col("Adj_Qty")).as("sum1"), sum("Adj_Qty").as("sum2"))

    val retailGroupWEDSKUPriceBrand1 = retailGroupWEDSKUOnline2.withColumn("PriceBand", col("PriceBand"))
      .join(retailGroupWEDSKUPriceBrandTemp1.withColumn("PriceBand", col("PriceBand")), Seq("Week_End_Date", "Online", "PriceBand", "Brand"), "left")
      .withColumn("PriceBand_cannibalization_OnOffline_Min", (col("sum1") - col("sumSKU1")) / (col("sum2") - col("sumSKU2")))
      .drop("sum1", "sum2", "sumSKU1", "sumSKU2")
    // TODO : check -> PriceBand Inner Cann  //    group_by(Account, add=TRUE)

    // remove below
    //    retailGroupWEDL1InnerCompCann3 = retailGroupWEDSKUOnline2
    //      .join(retailGroupWEDSKUPriceBrandTemp1, Seq("Week_End_Date", "Online", "PriceBand", "Brand"), "right")
    //      .withColumn("PriceBand_cannibalization_OnOffline_Min", (col("sum1") - col("sumSKU1")) / (col("sum2") - col("sumSKU2")))


    //        retailGroupWEDL1InnerCompCann3.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("D:\\files\\temp\\retail-Feb06-r-1628.csv")

    // remove ends
    val retailGroupWEDSKUPriceBrandTemp2 = retailGroupWEDSKUPriceBrand1
      .groupBy("Week_End_Date", "Online", "PriceBand", "Brand", "Account")
      .agg(sum(col("Promo_Pct_Min") * col("Adj_Qty")).as("sumInner1"), sum("Adj_Qty").as("sumInner2"))

    retailGroupWEDL1InnerCompCann3 = retailGroupWEDSKUPriceBrand1
      .join(retailGroupWEDSKUPriceBrandTemp2, Seq("Week_End_Date", "Online", "PriceBand", "Brand", "Account"), "left")
      .withColumn("PriceBand_Innercannibalization_OnOffline_Min", (col("sumInner1") - (col("Promo_Pct_Min") * col("Adj_Qty"))) / (col("sumInner2") - col("Adj_Qty")))
      .drop("sumInner1", "sumInner2")
      .withColumn("PriceBand_cannibalization_OnOffline_Min", when(col("PriceBand_cannibalization_OnOffline_Min").isNull, 0).otherwise(col("PriceBand_cannibalization_OnOffline_Min")))
      .withColumn("PriceBand_Innercannibalization_OnOffline_Min", when(col("PriceBand_Innercannibalization_OnOffline_Min").isNull, 0).otherwise(col("PriceBand_Innercannibalization_OnOffline_Min")))
      .na.fill(0, Seq("PriceBand_cannibalization_OnOffline_Min", "PriceBand_Innercannibalization_OnOffline_Min"))

    retailGroupWEDL1InnerCompCann3.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("/etherData/retailTemp/RetailFeatEngg/retail-PriceBandCannOfflineOnline-PART14.csv")


  }
}
