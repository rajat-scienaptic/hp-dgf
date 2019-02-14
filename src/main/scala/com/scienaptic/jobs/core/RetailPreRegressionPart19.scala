package com.scienaptic.jobs.core

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.core.RetailPreRegressionPart01.Cat_switch
import com.scienaptic.jobs.utility.CommercialUtility.extractWeekFromDateUDF
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}

import scala.collection.mutable

object RetailPreRegressionPart19 {

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

    var retailGroupWEDL1InnerCompCann3  = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/retailTemp/RetailFeatEngg/retail-PriceBandCannOfflineOnline-PART18.csv")
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("EOL_Date", to_date(unix_timestamp(col("EOL_Date"), "yyyy-MM-dd").cast("timestamp"))).cache()

    var retailWithCompCann2DF = retailGroupWEDL1InnerCompCann3
      .withColumn("PriceBand_Copy", col("PriceBand"))
      .withColumn("L1_Category_Copy", col("L1_Category"))
      .withColumn("Cate", concat_ws(".", col("L1_Category_Copy"), col("PriceBand_Copy")))
      .withColumn("Cate", when(col("Cate").isin("Home and Home Office.200-300", "Home and Home Office.300-500"), "Home and Home Office.200-500")
        .when(col("Cate").isin("Office - Personal.<100", "Office - Personal.100-150"), "Office - Personal.<150")
        .when(col("Cate").isin("Scanners.200-300", "Scanners.300-500"), "Scanners.200-500").otherwise(col("Cate")))

    //    retailWithCompCann2DF.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("D:\\files\\temp\\retail-Feb06-r-1613.csv")

    //Modify Cannibalization 4 ####
    val retailWithAdj4 = retailWithCompCann2DF.withColumn("Adj_Qty", when(col("POS_Qty") <= 0, 0).otherwise(col("POS_Qty")))
    val retailGroupWEDSKUOnline3 = retailWithAdj4.groupBy("Week_End_Date", "SKU", "Online")
      .agg(sum(col("Promo_Pct_Min") * col("Adj_Qty")).as("sumSKU1"), sum("Adj_Qty").as("sumSKU2"))
      .join(retailWithAdj4, Seq("Week_End_Date", "SKU", "Online"), "right")
      .withColumn("Cate", col("Cate"))

    val retailGroupWEDPriceBrandWithOnlineCateTemp1 = retailGroupWEDSKUOnline3
      .groupBy("Week_End_Date", "Online", "Cate", "Brand")
      .agg(sum(col("Promo_Pct_Min") * col("Adj_Qty")).as("sum1"), sum("Adj_Qty").as("sum2"))

    val retailGroupWEDPriceBrandWithOnlineCate1 = retailGroupWEDSKUOnline3
      .join(retailGroupWEDPriceBrandWithOnlineCateTemp1, Seq("Week_End_Date", "Online", "Cate", "Brand"), "left")
      .withColumn("Cate_cannibalization_OnOffline_Min", (col("sum1") - col("sumSKU1")) / (col("sum2") - col("sumSKU2")))
      .drop("sum1", "sum2", "sumSKU1", "sumSKU2")

    val retailGroupWEDPriceBrandWithOnlineCateTemp2 = retailGroupWEDPriceBrandWithOnlineCate1
      .groupBy("Week_End_Date", "Online", "Cate", "Brand", "Account")
      .agg(sum(col("Promo_Pct_Min") * col("Adj_Qty")).as("sumInner1"), sum("Adj_Qty").as("sumInner2"))

    retailWithCompCann2DF = retailGroupWEDPriceBrandWithOnlineCate1
      .join(retailGroupWEDPriceBrandWithOnlineCateTemp2, Seq("Week_End_Date", "Online", "Cate", "Brand", "Account"), "left")
      .withColumn("Cate_Innercannibalization_OnOffline_Min", (col("sumInner1") - (col("Promo_Pct_Min") * col("Adj_Qty"))) / (col("sumInner2") - col("Adj_Qty")))
      .drop("sumInner1", "sumInner2")
      .withColumn("Cate_cannibalization_OnOffline_Min", when(col("Cate_cannibalization_OnOffline_Min").isNull, 0).otherwise(col("Cate_cannibalization_OnOffline_Min")))
      .withColumn("Cate_Innercannibalization_OnOffline_Min", when(col("Cate_Innercannibalization_OnOffline_Min").isNull, 0).otherwise(col("Cate_Innercannibalization_OnOffline_Min")))
      .na.fill(0, Seq("Cate_cannibalization_OnOffline_Min", "Cate_Innercannibalization_OnOffline_Min"))

    var retailWithCompCann3DF = retailWithCompCann2DF
      .withColumn("BOPIS", when(col("BOPIS").isNull, 0).otherwise(col("BOPIS")))
      .withColumn("BOPISbtbhol", when(col("BOPIS") === 1 && col("Season").isin("BTB'16", "BTB'17", "HOL'16"), 1).otherwise(lit(0)))
      .withColumn("BOPISbts", when(col("BOPIS") === 1 && col("Season").isin("BTS'16", "BTS'17"), lit(1)).otherwise(lit(0)))
      .withColumn("Special_Programs", when(col("BOPIS") === 1 && col("Account").isin("Staples", "BOPIS"), 1).otherwise(col("Special_Programs")))
      .distinct()

    retailWithCompCann3DF.write.option("header", true).mode(SaveMode.Overwrite).csv("/etherData/retailTemp/RetailFeatEngg/retail-CateCannOfflineOnline-PART19.csv")

  }
}
