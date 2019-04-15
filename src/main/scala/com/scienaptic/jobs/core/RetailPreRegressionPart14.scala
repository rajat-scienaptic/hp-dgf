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

object RetailPreRegressionPart14 {

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

    var retailWithCompCannDF  = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/retailTemp/RetailFeatEngg/retail-SuppliesGM-PART13.csv")
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("EOL_Date", to_date(unix_timestamp(col("EOL_Date"), "yyyy-MM-dd").cast("timestamp"))).cache()

    val SKUCategeory = retailWithCompCannDF
      .filter(col("EOL_criterion") === 0 && col("BOL_criterion") === 0)
      .groupBy("SKU", "Account")
      .agg(count(col("SKU")).as("n"), mean(col("POS_Qty")).as("POS_Qty"), countDistinct("Promo_Pct").as("var_of_discount"))
      .filter(col("n") >= 5 && col("POS_QTY") >= 100 && col("var_of_discount") >= 3)
      .drop("n", "POS_QTY", "var_of_discount")
      .withColumn("SKU_category", col("SKU"))

    retailWithCompCannDF = retailWithCompCannDF
      .join(SKUCategeory, Seq("SKU", "Account"), "left")
      .withColumn("SKU_category", when(col("SKU_category").isNull, lit("Low Volume SKU") + col("L1_category")).otherwise(col("SKU_Name")))

    val retailWithHolidayAndQtyFilter = retailWithCompCannDF.where((col("Promo_Flag") === 0) && (col("EOL_criterion") === 0) && (col("BOL_criterion") === 0) && (col("USThanksgivingDay") === 0) && (col("USCyberMonday") === 0) && (col("POS_Qty") > 0))
    var npbl = retailWithHolidayAndQtyFilter
      .groupBy("Account", "SKU_Name")
      .agg(mean("POS_Qty").as("no_promo_avg"), stddev("POS_Qty").as("no_promo_sd"), min("POS_Qty").as("no_promo_min"), max("POS_Qty").as("no_promo_max"))

    retailWithHolidayAndQtyFilter.createOrReplaceTempView("npbl")
    val npblTemp = spark.sql("select SKU_Name,Account, PERCENTILE(POS_Qty, 0.50) OVER (PARTITION BY SKU_Name, Account) as no_promo_med from npbl")
      .dropDuplicates("SKU_Name", "Account", "no_promo_med")

    npbl = npbl.withColumn("SKU_Name", col("SKU_Name")).withColumn("Account", col("Account"))
      .join(npblTemp.withColumn("SKU_Name", col("SKU_Name")).withColumn("Account", col("Account")), Seq("Account", "SKU_Name"), "inner")
      .withColumn("no_promo_med", when(col("SKU_Name") === "Envy 5535" && col("Account") === "Office Depot-Max", 21).otherwise(col("no_promo_med")))
      .withColumn("no_promo_med", when(col("SKU_Name") === "OJ Pro 8610" && col("Account") === "Office Depot-Max", 4057).otherwise(col("no_promo_med")))
      .withColumn("no_promo_med", when(col("SKU_Name") === "OJ Pro 6830" && col("Account") === "Staples", 232).otherwise(col("no_promo_med")))

    retailWithCompCannDF = retailWithCompCannDF
      .join(npbl, Seq("SKU_Name", "Account"), "left")
      .withColumn("no_promo_avg", when(col("no_promo_avg").isNull, 0).otherwise(col("no_promo_avg")))
      .withColumn("no_promo_med", when(col("no_promo_med").isNull, 0).otherwise(col("no_promo_med")))
      .withColumn("low_baseline", when(((col("no_promo_avg") >= min_baseline) && (col("no_promo_med") > baselineThreshold)) || ((col("no_promo_med") >= min_baseline) && (col("no_promo_avg") >= baselineThreshold)), 0).otherwise(1))
      .withColumn("low_volume", when(col("POS_Qty") > 0, 0).otherwise(1))
      .withColumn("raw_bl_avg", col("no_promo_avg") * (col("seasonality_npd2") + 1))
      .withColumn("raw_bl_med", col("no_promo_med") * (col("seasonality_npd2") + 1))
      .withColumn("low_baseline", when(col("Online") === 1, 0).otherwise(col("low_baseline")))

    val retailLowConfidence = retailWithCompCannDF
      .filter(col("Promo_Flag") === 0 && col("low_volume") === 0 && col("EOL_criterion") === 0 && col("BOL_criterion") === 0 &&
        col("USThanksgivingDay") === 0 && col("USCyberMonday") === 0)
      .groupBy("SKU_Name", "Account")
      .agg(count("SKU").as("n"))
      .filter(col("n") < 5)
      .drop("n")
      .withColumn("low_confidence", lit(1))

    retailWithCompCannDF = retailWithCompCannDF
      .join(retailLowConfidence, Seq("Account", "SKU_Name"), "left")
      .withColumn("low_confidence", when(col("low_confidence").isNull, lit(0)).otherwise(col("low_confidence")))
      .withColumn("NP_Flag", when((col("Account") === "Costco" || col("Account") === "Sam's Club") && col("Promo_Flag") === 1, lit(1)).otherwise(col("NP_Flag")))
      .withColumn("high_disc_Flag", when(col("Promo_Pct") <= 0.55, 0).otherwise(lit(1)))

    // CHECK  -> always_promo <- ave(retail$Promo.Flag, retail$Account, retail$SKU, retail$Season, FUN=mean)
    //  retail$always_promo.Flag <- ifelse(always_promo==1,1,0)
    val retailPromoMean = retailWithCompCannDF
      .groupBy("Account", "SKU_Name", "Season")
      .agg(mean(col("Promo_Flag")).as("PromoFlagAvg"))

    retailWithCompCannDF = retailWithCompCannDF.join(retailPromoMean, Seq("Account", "SKU_Name", "Season"), "left")
      .withColumn("always_promo_Flag", when(col("PromoFlagAvg") === 1, 1).otherwise(0)).drop("PromoFlagAvg")

    retailWithCompCannDF.write.option("header", true).mode(SaveMode.Overwrite).csv("/etherData/retailTemp/RetailFeatEngg/retail-NoPromo-SkuCategory-PART14.csv")




  }
}
