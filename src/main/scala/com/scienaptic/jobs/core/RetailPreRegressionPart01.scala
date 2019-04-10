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
import scala.collection.mutable.ListBuffer

object RetailPreRegressionPart01 {

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
    import spark.implicits._

    val currentTS = executionContext.spark.read.json("/etherData/state/currentTS.json").select("ts").head().getString(0)
    var retail = renameColumns(executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/Pricing/Outputs/POS_Retail/posqty_output_retail_"+currentTS+".csv"))
    retail.columns.toList.foreach(x => {
      retail = retail.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    retail = retail.cache()
      .withColumn("Account", when(col("Account") === "Costco Wholesale", "Costco")
        .when(col("Account").isin("Wal-Mart Online"), "Walmart")
        .when(col("Account").isin("Amazon.Com"), "Amazon-Proper")
        .when(col("Account").isin("Micro Electronics Inc", "Fry's Electronics Inc", "Target Stores"), "Rest of Retail").otherwise(col("Account")))
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .where(col("Week_End_Date") >= minimumRegressionDate)
      .where(col("Week_End_Date") <= maximumRegressionDate)
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumnRenamed("Street_Price", "Street_Price_Org")


    var IFS2 = renameColumns(executionContext.spark.read.option("header", "true").option("inferSchema", true).csv("/etherData/managedSources/IFS2/IFS2_most_recent.csv"))
    IFS2.columns.toList.foreach(x => {
      IFS2 = IFS2.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    IFS2 = IFS2
      .withColumn("Valid_Start_Date", to_date(unix_timestamp(col("Valid_Start_Date"), "MM/dd/yyyy").cast("timestamp")))
      .withColumn("Valid_End_Date", to_date(unix_timestamp(col("Valid_End_Date"), "MM/dd/yyyy").cast("timestamp"))).cache()


    var retailMergeIFS2DF = retail
      //      .drop("Street_Price") // removed to avoid ambiguity
      .join(IFS2.dropDuplicates("SKU", "Street_Price").select("SKU", "Changed_Street_Price", "Street_Price", "Valid_Start_Date", "Valid_End_Date"), Seq("SKU"), "left")
      .withColumn("Changed_Street_Price", when(col("Changed_Street_Price").isNull, 0).otherwise(col("Changed_Street_Price")))
      .withColumn("Changed_Street_Price", when((col("Changed_Street_Price") === 1) && (col("Category").isin("Home","SMB")), 0).otherwise(col("Changed_Street_Price")))
      .withColumn("Special_Programs", lit("None"))
      .withColumn("Valid_Start_Date", when(col("Valid_Start_Date").isNull, dat2000_01_01).otherwise(col("Valid_Start_Date")))
      .withColumn("Valid_End_Date", when(col("Valid_End_Date").isNull, dat9999_12_31).otherwise(col("Valid_End_Date")))
      .withColumn("Street_Price", when(col("Street_Price").isNull, col("Street_Price_Org")).otherwise(col("Street_Price")))
      .where((col("Week_End_Date") >= col("Valid_Start_Date")) && (col("Week_End_Date") < col("Valid_End_Date")))
      .drop("Street_Price_Org", "Valid_Start_Date", "Valid_End_Date")

    var retailBBYBundleAccountDF = retailMergeIFS2DF.filter((col("Account") === "Best Buy") &&
      (col("Raw_POS_Qty") > col("POS_Qty")))

    retailBBYBundleAccountDF = retailBBYBundleAccountDF
      .withColumn("Special_Programs", lit("BBY Bundle"))
      .filter(col("SKU").isNotNull)
      .withColumn("POS_Qty", col("Raw_POS_Qty") - col("POS_Qty"))

    retailMergeIFS2DF = doUnion(retailMergeIFS2DF, retailBBYBundleAccountDF).get
      .withColumn("POS_Qty", when(col("POS_Qty").isNull, 0).otherwise(col("POS_Qty")))
      .withColumn("Distribution_Inv", when(col("Distribution_Inv").isNull, 0).otherwise(col("Distribution_Inv")))
      .withColumn("Distribution_Inv", when(col("Distribution_Inv") < 0, 0).otherwise(col("Distribution_Inv")))
      .withColumn("Distribution_Inv", when(col("Distribution_Inv") > 1, 1).otherwise(col("Distribution_Inv")))
      .withColumn("SKU", when(col("SKU") === "J9V91A", "J9V90A").otherwise(col("SKU")))
      .withColumn("SKU", when(col("SKU") === "J9V92A", "J9V90A").otherwise(col("SKU")))
      .withColumn("SKU", when(col("SKU") === "M9L74A", "M9L75A").otherwise(col("SKU")))
      .drop("Raw_POS_Qty")

    val retailAggregatePOSDF = retailMergeIFS2DF
      .groupBy("SKU", "Account", "Week_End_Date", "Online", "Special_Programs")
      .agg(sum(col("POS_Qty")).as("POS_Qty"))

    retailMergeIFS2DF = retailMergeIFS2DF.drop("POS_Qty")
    val wind = Window.partitionBy("SKU", "Account", "Week_End_Date", "Online", "Special_Programs")
      .orderBy(col("Distribution_Inv").asc)
    val retailJoinRetailTreatmentAndAggregatePOSDF = retailMergeIFS2DF
      .join(retailAggregatePOSDF, Seq("SKU", "Account", "Week_End_Date", "Online", "Special_Programs"), "left")
      .withColumn("rank", row_number().over(wind))
      .filter(col("rank") === 1)
      .drop("rank")
//      .dropDuplicates("SKU", "Account", "Week_End_Date", "Online", "Special_Programs") // duplicated line:112 R

    var SKUMapping = renameColumns(executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/managedSources/S-Print/SKU_Mapping/s-print_SKU_mapping.csv"))
    SKUMapping.columns.toList.foreach(x => {
      SKUMapping = SKUMapping.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    SKUMapping = SKUMapping.cache()

    var GAP1 = renameColumns(executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/Pricing/Outputs/POS_GAP/gap_data_full_"+currentTS+".csv")).cache()
    GAP1.columns.toList.foreach(x => {
      GAP1 = GAP1.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    GAP1 = GAP1.cache()
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("Total_IR", when(col("Account") === "Best Buy" && col("SKU") === "V1N08A", 0).otherwise(col("Total_IR")))
      .withColumn("Total_IR", when(col("Account") === "Office Depot-Max" && col("SKU") === "V1N07A", 0).otherwise(col("Total_IR")))

    val adPositionDF = GAP1.filter(col("Brand").isin("HP", "Samsung"))
      .select("SKU", "Week_End_Date", "Account", "Online", "Ad_Location", "Brand", "Product")
      .filter(col("Ad_Location").isNotNull)
      .filter(col("Ad_Location") =!= 0)
      .distinct()

    val adPositionJoinSKUMappingDF = adPositionDF.join(SKUMapping, Seq("Product"), "left")
      .withColumn("SKU", when(col("Brand").isin("Samsung"), col("SKU_HP")).otherwise(col("SKU")))
      .drop("Brand", "Product", "SKU_HP")
      .distinct()

    val retailJoinAdPositionDF = retailJoinRetailTreatmentAndAggregatePOSDF
      .join(adPositionJoinSKUMappingDF, Seq("SKU", "Account", "Week_End_Date", "Online"), "left")

    val GAP1JoinSKUMappingDF = GAP1.join(SKUMapping, Seq("Product"), "left")
      .withColumn("SKU", when(col("Brand").isin("Samsung"), col("SKU_HP").cast("string")).otherwise(col("SKU").cast("string")))
      .drop("Product", "SKU_HP")
      //      .withColumn("SKU_HP", lit(null).cast(StringType))
      .distinct()



    retailJoinAdPositionDF.write.mode(SaveMode.Overwrite).option("header", true).csv("/etherData/retailTemp/RetailFeatEngg/retail-r-retailJoinAdPositionDF-PART01.csv")
    GAP1JoinSKUMappingDF.write.mode(SaveMode.Overwrite).option("header", true).csv("/etherData/retailTemp/RetailFeatEngg/retail-r-GAP1JoinSKUMappingDF-PART01.csv")
    // Part 1 Ends here

  }
}