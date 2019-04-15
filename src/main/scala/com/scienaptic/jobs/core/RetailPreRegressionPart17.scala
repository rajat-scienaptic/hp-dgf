package com.scienaptic.jobs.core

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import com.scienaptic.jobs.ExecutionContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable

object RetailPreRegressionPart17 {

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


    var retailGroupWEDL1InnerCompCann2  = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/retailTemp/RetailFeatEngg/retail-L1L2InnerCann-half-PART16.csv")
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("EOL_Date", to_date(unix_timestamp(col("EOL_Date"), "yyyy-MM-dd").cast("timestamp"))).cache()


    //    retailGroupWEDL1InnerCompCann2.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("D:\\files\\temp\\retail-Feb06-r-1548.csv")
    ////
    val retailGroupWEDSKU3Online = retailGroupWEDL1InnerCompCann2
      .groupBy("Week_End_Date", "Online", "Brand", "L2_Category")
      .agg(sum(col("Promo_Pct_Min") * col("Adj_Qty")).as("sum1"), sum("Adj_Qty").as("sum2"))
    //      .join(retailWithAdj2, Seq("Week_End_Date", "SKU", "Online"), "right")

    var retailGroupWEDL1CompCann3 = retailGroupWEDL1InnerCompCann2
      .join(retailGroupWEDSKU3Online, Seq("Week_End_Date", "Online", "Brand", "L2_Category"), "left")
      .withColumn("L2_cannibalization_OnOffline_Min", (col("sum1") - col("sumSKU1")) / (col("sum2") - col("sumSKU2")))
      .drop("sum1", "sum2", "sumInner1", "sumInner2")

    ////
    val retailGroupWEDSKU4InnerOnline = retailGroupWEDL1CompCann3
      .groupBy("Week_End_Date", "Online", "Brand", "Account", "L2_Category")
      .agg(sum(col("Promo_Pct_Min") * col("Adj_Qty")).as("sumInner1"), sum("Adj_Qty").as("sumInner2"))
    //      .join(retailWithAdj2, Seq("Week_End_Date", "SKU", "Online"), "right")

    var retailGroupWEDL1InnerCompCann3 = retailGroupWEDL1CompCann3
      .join(retailGroupWEDSKU4InnerOnline, Seq("Week_End_Date", "Online", "Brand", "L2_Category", "Account"), "left")
      .withColumn("L2_Innercannibalization_OnOffline_Min", (col("sumInner1") - (col("Promo_Pct_Min") * col("Adj_Qty"))) / (col("sumInner2") - col("Adj_Qty")))
      .drop("sum1", "sum2", "sumInner1", "sumInner2", "sumSKU1", "sumSKU2", "Adj_Qty")
      .withColumn("L1_Innercannibalization_OnOffline_Min", when(col("L1_Innercannibalization_OnOffline_Min").isNull, 0).otherwise(col("L1_Innercannibalization_OnOffline_Min")))
      .withColumn("L2_Innercannibalization_OnOffline_Min", when(col("L2_Innercannibalization_OnOffline_Min").isNull, 0).otherwise(col("L2_Innercannibalization_OnOffline_Min")))
      .na.fill(0, Seq("L1_Innercannibalization_OnOffline_Min", "L2_Innercannibalization_OnOffline_Min"))
      // TODO done: retail$PriceBand<-cut(retail$Street.Price,   #TODO: binning  *quantile
      //                        breaks=c(0,100,150,200,300,500,10000),
      //                        labels=c("<100","100-150","150-200","200-300","300-500","500+"))// check : https://rpubs.com/pierrelafortune/cutdocumentation
      .withColumn("PriceBand", when(col("Street_Price").between(0, 100), lit("<100"))
      .when(col("Street_Price").between(100, 150), lit("100-150"))
      .when(col("Street_Price").between(150, 200), lit("150-200"))
      .when(col("Street_Price").between(200, 300), lit("200-300"))
      .when(col("Street_Price").between(300, 500), lit("300-500"))
      .when(col("Street_Price").between(500, 10000), lit("500+"))
    )
      .withColumn("PriceBand", when(col("PriceBand").isNull, "NA").otherwise(col("PriceBand")))

    retailGroupWEDL1InnerCompCann3.write.option("header", true).mode(SaveMode.Overwrite).csv("/etherData/retailTemp/RetailFeatEngg/retail-L1L2InnerCann-PART17.csv")


  }
}
