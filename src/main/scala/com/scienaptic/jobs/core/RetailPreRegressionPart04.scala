package com.scienaptic.jobs.core

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.bean.UnionOperation
import com.scienaptic.jobs.core.RetailPreRegressionPart01.{checkPrevDistInvGTBaseline, concatenateRankWithDist, stability_range}
import com.scienaptic.jobs.utility.CommercialUtility.createlist
import com.scienaptic.jobs.utility.Utils.renameColumns
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.collection.mutable

object RetailPreRegressionPart04 {

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


    var retailEOL  = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/retailTemp/RetailFeatEngg/retail-EOL-PART03.csv")
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("EOL_Date", to_date(unix_timestamp(col("EOL_Date"), "yyyy-MM-dd").cast("timestamp"))).cache()

    var BOL = retailEOL.select("SKU", "ES_date", "GA_date")
      .dropDuplicates()
      .where((col("ES_date").isNotNull) || (col("GA_date").isNotNull))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date_wday", lit(7) - dayofweek(col("ES_date")).cast("int")) //As dayofweek returns in range 1-7 we want 0-6
      .withColumn("GA_date_wday", lit(7) - dayofweek(col("GA_date")).cast("int"))
      .withColumn("GA_date", to_date(expr("date_add(GA_date, GA_date_wday)")))
      .withColumn("ES_date", to_date(expr("date_add(ES_date, ES_date_wday)")))
      .drop("GA_date_wday", "ES_date_wday")

    // write
    //    BOL.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("D:\\files\\temp\\retail-Feb06-r-694.csv")

    val windForSKUnAccount = Window.partitionBy("SKU&Account").orderBy(/*"SKU", "Account", */ "Week_End_Date")
    var BOLCriterion = retailEOL
      .groupBy("SKU", "Account", "Week_End_Date")
      .agg(max("Distribution_Inv").as("Distribution_Inv")) //TODO: Changed from sum to max
      .sort("SKU", "Account", "Week_End_Date")
      .withColumn("SKU&Account", concat(col("SKU"), col("Account")))
      //      .withColumn("uuid", lit(generateUUID()))
      .withColumn("rank", row_number().over(windForSKUnAccount))
      .withColumn("BOL_criterion", when(col("rank") < intro_weeks, 0).otherwise(1)) //BOL_criterion$BOL_criterion <- ave(BOL_criterion$Qty, paste0(BOL_criterion$SKU, BOL_criterion$Reseller.Cluster), FUN = BOL_criterion_v3)
      .drop("rank", "Distribution_Inv", "uuid")

    BOLCriterion = BOLCriterion.join(BOL.select("SKU", "GA_date"), Seq("SKU"), "left")
    val minWEDDate = to_date(lit(BOLCriterion.agg(min("Week_End_Date")).head().getDate(0)))

    BOLCriterion = BOLCriterion.withColumn("GA_date", when(col("GA_date").isNull, minWEDDate).otherwise(col("GA_date")))
      .where(col("Week_End_Date") >= col("GA_date") && col("GA_date").isNotNull)

    ////////////
    // write
    //    BOLCriterion.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("D:\\files\\temp\\retail-Feb06-r-762.csv")

    val BOLCriterionFirst = BOLCriterion.where(col("BOL_criterion") === 1)
      .groupBy("SKU", "Account")
      .agg(min("Week_End_Date").as("first_date"))
    //      .join(BOLCriterion.where(col("BOL_criterion") === 1), Seq("SKU", "Account"), "right")
    //write
    //    BOLCriterionFirst.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("D:\\files\\temp\\retail-Feb06-r-770.csv")

    val BOLCriterionMax = retailEOL
      .groupBy("SKU", "Account")
      .agg(max("Week_End_Date").as("max_date"))
    //      .join(retailEOL, Seq("SKU", "Account"), "right")
    // max
    //    BOLCriterionMax.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("D:\\files\\temp\\retail-Feb06-r-777.csv")

    val BOLCriterionMin = retailEOL
      .groupBy("SKU", "Account")
      .agg(min("Week_End_Date").as("min_date"))
    //      .join(retailEOL, Seq("SKU", "Account"), "right")
    // write
    //    BOLCriterionMin.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("D:\\files\\temp\\retail-Feb06-r-783.csv")

    BOLCriterion = BOLCriterionMax.withColumn("Account", col("Account")) // with column is on purpose as join cannot find Account from max dataframe
      .join(BOLCriterionFirst, Seq("SKU", "Account"), "left")
      .join(BOLCriterionMin, Seq("SKU", "Account"), "left")

    BOLCriterion = BOLCriterion.withColumn("first_date", when(col("first_date").isNull, col("max_date")).otherwise(col("first_date")))
      .drop("max_date")
    val minMinDateBOL = BOLCriterion.agg(min("min_date")).head().getDate(0)
    BOLCriterion = BOLCriterion
      .where(!((col("min_date") === col("first_date")) && (col("first_date") === minMinDateBOL)))
      .withColumn("diff_weeks", ((datediff(to_date(col("first_date")), to_date(col("min_date")))) / 7) + 1)

    /*
     do not un comment
    BOL_criterion <- BOL_criterion[rep(row.names(BOL_criterion), BOL_criterion$diff_weeks),]
    * BOL_criterion$add <- t(as.data.frame(strsplit(row.names(BOL_criterion), "\\.")))[,2]
    BOL_criterion$add <- ifelse(grepl("\\.",row.names(BOL_criterion))==FALSE,0,as.numeric(BOL_criterion$add))
    BOL_criterion$add <- BOL_criterion$add*7
    BOL_criterion$Week.End.Date <- BOL_criterion$min_date+BOL_criterion$add
    */
    BOLCriterion = BOLCriterion.withColumn("diff_weeks", when(col("diff_weeks").isNull || col("diff_weeks") <= 0, 0).otherwise(col("diff_weeks")))
      .withColumn("diff_weeks", col("diff_weeks").cast("int"))
      .withColumn("repList", createlist(col("diff_weeks"))).withColumn("add", explode(col("repList"))).drop("repList")
      .withColumn("add", col("add") * lit(7))
      .withColumn("Week_End_Date", expr("date_add(min_date, add)")) //CHECK: check if min_date is in timestamp format!

    BOLCriterion = BOLCriterion.drop("min_date", "fist_date", "diff_weeks", "add")
      .withColumn("BOL_criterion", lit(1))

    // write
    //    BOLCriterion.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("D:\\files\\temp\\retail-Feb06-r-818.csv")

    // write
    //     BOLCriterion.write.mode(SaveMode.Overwrite).option("header", true).csv("D:\\files\\temp\\BOLCriterion.csv")

    retailEOL = retailEOL.join(BOLCriterion, Seq("SKU", "Account", "Week_End_Date"), "left")
      .withColumn("BOL_criterion", when(col("BOL_criterion").isNull, 0).otherwise(col("BOL_criterion")))
      .withColumn("BOL_criterion", when(col("EOL_criterion").isNull, null).otherwise(col("BOL_criterion")))
      .withColumn("BOL_criterion", when(col("EOL_criterion") === 1, 0).otherwise(col("BOL_criterion")))
      .withColumn("BOL_criterion", when(datediff(col("Week_End_Date"), col("GA_date")) < (7 * 6), 1).otherwise(col("BOL_criterion")))
      .withColumn("BOL_criterion", when(col("GA_date").isNull, 0).otherwise(col("BOL_criterion")))
      .withColumn("BOL_criterion", when(col("Account").isin("Amazon-Proper") && col("SKU") === "M2U85A" && (col("Week_End_Date") === "2018-04-14" || col("Week_End_Date") === "2018-04-21"), 0).otherwise(col("BOL_criterion"))) // multiple date condition merged into 1
      .withColumn("EOL_criterion", when(col("EOL_criterion").isNull, 0).otherwise(col("EOL_criterion")))
      .withColumn("BOL_criterion", when(col("BOL_criterion").isNull, 0).otherwise(col("BOL_criterion")))
      .withColumn("ASP_IR", when(col("EOL_criterion") === 1, when(col("Other_IR") =!= 0, col("Other_IR")).otherwise(col("ASP_IR"))).otherwise(col("ASP_IR")))
      .withColumn("Other_IR", when(col("EOL_criterion") === 1, when(col("Other_IR") =!= 0, 0).otherwise(col("Other_IR"))).otherwise(col("Other_IR")))
      .withColumn("ASP_Flag", when(col("ASP_IR") > 0, 1).otherwise(lit(0)))
      .withColumn("Other_IR_Flag", when(col("Other_IR") > 0, 1).otherwise(lit(0)))

    // comment this 2 line
        retailEOL.write.option("header", true).mode(SaveMode.Overwrite).csv("/etherData/retailTemp/RetailFeatEngg/retail-BOL-PART04.csv")
  }
}
