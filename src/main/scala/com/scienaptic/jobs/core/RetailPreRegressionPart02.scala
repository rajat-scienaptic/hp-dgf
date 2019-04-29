package com.scienaptic.jobs.core

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.bean.UnionOperation.doUnion
import com.scienaptic.jobs.utility.Utils.renameColumns
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

import scala.collection.mutable

object RetailPreRegressionPart02 {

  val Cat_switch = 1
  val dat2000_01_01 = to_date(unix_timestamp(lit("2000-01-01"), "yyyy-MM-dd").cast("timestamp"))
  val dat9999_12_31 = to_date(unix_timestamp(lit("9999-12-31"), "yyyy-MM-dd").cast("timestamp"))
  val dateFormatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
  val dateFormatterMMddyyyyWithSlash = new SimpleDateFormat("MM/dd/yyyy")
  val dateFormatterMMddyyyyWithHyphen = new SimpleDateFormat("dd-MM-yyyy")
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

    var retailJoinAdPositionDF  = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/retailTemp/RetailFeatEngg/retail-r-retailJoinAdPositionDF-PART01.csv")
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      //.withColumn("Week_End_Date", to_date(col("Week_End_Date")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))


    var GAP1JoinSKUMappingDF  = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/retailTemp/RetailFeatEngg/retail-r-GAP1JoinSKUMappingDF-PART01.csv")
      .withColumn("Week_End_Date", to_date(col("Week_End_Date")))
      //.withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))


    //GAP_1$SKU <- as.character(GAP_1$SKU)
    //GAP_1$SKU_HP <- as.character(GAP_1$SKU_HP)

    val GAP1AggregateDF = GAP1JoinSKUMappingDF
      .filter(col("Account").isin("Best Buy", "Office Depot-Max", "Staples") && col("Brand").isin("HP", "Samsung"))
      .groupBy("SKU", "Account", "Week_End_Date", "Online")
      .agg(max(col("Ad")).as("Ad"), max(col("Total_IR")).as("GAP_IR"))
      //      .withColumn("GAP_IR", lit(null).cast(StringType))
      //      .withColumn("Days_on_Promo", lit(null).cast(StringType))

    val retailJoinGAP1AggregateDF = retailJoinAdPositionDF
      .join(GAP1AggregateDF.select("SKU", "Account", "Week_End_Date", "Online", "GAP_IR", "Ad"), Seq("SKU", "Account", "Week_End_Date", "Online"), "left")
      .withColumn("GAP_IR", when(col("GAP_IR").isNull, 0).otherwise(col("GAP_IR")))
      .withColumn("Ad", when(col("Ad").isNull, 0).otherwise(col("Ad")))

    val aggregateGAP1Days = GAP1JoinSKUMappingDF
      .filter(col("Account").isin("Costco", "Sam's Club") && col("Brand") === "HP")
      .withColumn("conc_col",concat(col("SKU"),col("Account"),col("Week_End_Date")))
      .groupBy("SKU", "Account", "Week_End_Date","conc_col")
      .agg(count("conc_col").as("count_group"), sum(when(col("Days_on_Promo").isNotNull,1).otherwise(0)).as("count_days"), max(col("Days_on_Promo")).as("max_Days_on_Promo"))
      .withColumn("Days_on_Promo", when(col("count_group") =!=col("count_days"),null).otherwise(col("max_Days_on_Promo")))
      .drop("conc_col","max_Days_on_Promo")

    val retailJoinAggregateGAP1DaysDF = retailJoinGAP1AggregateDF
      .join(aggregateGAP1Days.select("SKU", "Account", "Week_End_Date", "Days_on_Promo"), Seq("SKU", "Account", "Week_End_Date"), "left")
      .withColumn("Days_on_Promo", when(col("Days_on_Promo").isNull, 0).otherwise(col("Days_on_Promo")))

    var adAccountDF = GAP1AggregateDF
      .groupBy("SKU", "Week_End_Date", "Online")
      .pivot("Account")
      .agg(first(col("Ad")))
    adAccountDF = adAccountDF
      //      .join(GAP1AggregateDF, Seq("uuid"), "right")
      .drop("uuid", "Account", "Ad") // check account if needed


    val accountList = List("Best Buy", "Office Depot-Max", "Staples")
    val adAccountDFColumns = adAccountDF.columns

    // check if all columns exists and if not then assign null
    accountList.foreach(x => {
      if (!adAccountDFColumns.contains(x))
        adAccountDF = adAccountDF.withColumn(x, lit(null).cast(StringType))
    })

    // NA treatment
    accountList.foreach(account => {
      adAccountDF = adAccountDF.withColumn(account, when(col(account).isNull, 0).otherwise(col(account)))
        .withColumnRenamed(account, "Ad_" + account)
    })

    // TODO done: is NA treatment necessary here ?
    val retailJoinAdAccountDF = retailJoinAggregateGAP1DaysDF
      .join(adAccountDF, Seq("SKU", "Week_End_Date", "Online"), "left")
      .withColumn("Ad_Best Buy", when(col("Ad_Best Buy").isNull, 0).otherwise(col("Ad_Best Buy")))
      .withColumn("Ad_Office Depot-Max", when(col("Ad_Office Depot-Max").isNull, 0).otherwise(col("Ad_Office Depot-Max")))
      .withColumn("Ad_Staples", when(col("Ad_Staples").isNull, 0).otherwise(col("Ad_Staples")))


    val GAP2 = GAP1JoinSKUMappingDF
      .groupBy("Brand", "SKU", "Account", "Week_End_Date", "L2_Category", "L1_Category", "Category_1", "Category_2")
      .agg(max(col("Ad")).as("Ad"))

    val compAd = GAP2
      .groupBy("L2_Category", "Brand", "Week_End_Date", "Account")
      .agg(mean(col("Ad")).as("Ad_avg"), sum(col("Ad")).as("Ad_total"))
      .filter(!col("Brand").isin("Xerox"))

    var compAdAvgDF = compAd
      .groupBy("L2_Category", "Week_End_Date", "Account")
      .pivot("Brand")
      .agg(first("Ad_avg"))
      //      .join(compAd, Seq("uuid"), "right") // check if needed
      .drop("uuid", "Brand", "Ad_avg") // check brand if needed

    val allBrands = List("Brother", "Canon", "Epson", "Lexmark", "Samsung", "HP")
    val compAd2Columns = compAdAvgDF.columns
    allBrands.foreach(x => {
      if (!compAd2Columns.contains(x))
        compAdAvgDF = compAdAvgDF.withColumn(x, lit(null).cast(StringType))
    })
    allBrands.foreach(brand => {
      compAdAvgDF = compAdAvgDF.withColumn(brand, when(col(brand).isNull, 0).otherwise(col(brand)))
        .withColumnRenamed(brand, "Ad_ratio_" + brand)
    })

    var retailJoinCompAdAvgDF = retailJoinAdAccountDF
      .join(compAdAvgDF, Seq("L2_Category", "Week_End_Date", "Account"), "left")

    allBrands.foreach(brand => {
      retailJoinCompAdAvgDF = retailJoinCompAdAvgDF.withColumn("Ad_ratio_" + brand, when(col("Ad_ratio_" + brand).isNull, 0).otherwise(col("Ad_ratio_" + brand)))
    })

    compAdAvgDF = compAdAvgDF.drop("Ad_avg")

    // TODO done : Need to verify Ad_total for aggregation
    var compAdTotalDF = compAd
      .groupBy("L2_Category", "Week_End_Date", "Account")
      .pivot("Brand")
      .agg(first("Ad_total"))
      //      .join(compAd, Seq("uuid"), "right") // check if needed
      .drop("uuid", "Ad_total", "Brand") // check brand if needed

    val compAdTotalColumns = compAdTotalDF.columns
    allBrands.foreach(x => {
      if (!compAdTotalColumns.contains(x))
        compAdTotalDF = compAdTotalDF.withColumn(x, lit(null).cast(StringType))
    })
    allBrands.foreach(brand => {
      compAdTotalDF = compAdTotalDF.withColumn(brand, when(col(brand).isNull, 0).otherwise(col(brand)))
        .withColumnRenamed(brand, "Ad_total_" + brand)
    })

    var retailJoincompAdTotalDFDF = retailJoinCompAdAvgDF
      .join(compAdTotalDF, Seq("L2_Category", "Week_End_Date", "Account"), "left")

    allBrands.foreach(brand => {
      retailJoincompAdTotalDFDF = retailJoincompAdTotalDFDF.withColumn("Ad_total_" + brand, when(col("Ad_total_" + brand).isNull, 0).otherwise(col("Ad_total_" + brand)))
    })

    // need utility for the below
    retailJoincompAdTotalDFDF = retailJoincompAdTotalDFDF.withColumn("Total_Ad_No_", col("Ad_total_Brother") +
      col("Ad_total_Canon") + col("Ad_total_Lexmark") + col("Ad_total_Samsung") + col("Ad_total_Epson") + col("Ad_total_HP"))


    allBrands.foreach(brand => {
      retailJoincompAdTotalDFDF = retailJoincompAdTotalDFDF.withColumn("Ad_share_" + brand, when((col("Ad_total_" + brand) / col("Total_Ad_No_")).isNull, 0)
        .otherwise(col("Ad_total_" + brand) / col("Total_Ad_No_")))
    })

    retailJoincompAdTotalDFDF = retailJoincompAdTotalDFDF
      .drop("Total_Ad_No_")
      .withColumn("Ad_Location", when(col("Ad_Location").isNull, "No_Ad").otherwise(col("Ad_Location").cast("string")))
      .withColumn("Ad_Location", when(col("Ad_Best Buy") === 1 && col("Ad_Location") === "No_Ad", "No_Info")
        .when(col("Ad_Office Depot-Max") === 1 && col("Ad_Location") === "No_Ad", "No_Info")
        .when(col("Ad_Staples") === 1 && col("Ad_Location") === "No_Ad", "No_Info")
        .otherwise(col("Ad_Location").cast("string")))
      .dropDuplicates() //All Ad variables match till here

    retailJoincompAdTotalDFDF.write.mode(SaveMode.Overwrite).option("header", true).csv("/etherData/retailTemp/RetailFeatEngg/retail-r-retailJoincompAdTotalDFDF-PART02.csv")
    // Part 1 Ends here

  }
}