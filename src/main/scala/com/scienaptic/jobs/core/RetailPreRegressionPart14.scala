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

    var retailWithCompCann3DF  = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/retailTemp/RetailFeatEngg/retail-CateCannOfflineOnline-PART13.csv")
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("EOL_Date", to_date(unix_timestamp(col("EOL_Date"), "yyyy-MM-dd").cast("timestamp"))).cache()

    retailWithCompCann3DF = retailWithCompCann3DF
      .withColumn("cann_group", lit("None"))
      .withColumn("cann_group", when(col("SKU_Name").contains("M20") || col("SKU_Name").contains("M40"), "M201_M203/M402").otherwise(col("cann_group")))
      .withColumn("cann_group", when(col("SKU_Name").contains("M22") || col("SKU_Name").contains("M42"), "M225_M227/M426").otherwise(col("cann_group")))
      .withColumn("cann_group", when(col("SKU_Name").contains("M25") || col("SKU_Name").contains("M45"), "M252_M254/M452").otherwise(col("cann_group")))
      .withColumn("cann_group", when(col("SKU_Name").contains("M27") || col("SKU_Name").contains("M28") || col("SKU_Name").contains("M47"), "M277_M281/M477").otherwise(col("cann_group")))
      .withColumn("cann_group", when(col("SKU_Name").contains("8710") || col("SKU_Name").contains("8720"), "Weber").otherwise(col("cann_group")))
      .withColumn("cann_group", when(col("SKU_Name").contains("6968") || col("SKU_Name").contains("6978"), "Muscatel").otherwise(col("cann_group")))
      .withColumn("cann_group", when(col("SKU_Name").contains("6255") || col("SKU_Name").contains("7155") || col("SKU_Name").contains("7855"), "Palermo").otherwise(col("cann_group")))
      .withColumn("cann_receiver", lit("None"))
      .withColumn("cann_receiver", when(col("SKU_Name").contains("M40"), "M402").otherwise(col("cann_receiver")))
      .withColumn("cann_receiver", when(col("SKU_Name").contains("M42"), "M426").otherwise(col("cann_receiver")))
      .withColumn("cann_receiver", when(col("SKU_Name").contains("M45"), "M452").otherwise(col("cann_receiver")))
      .withColumn("cann_receiver", when(col("SKU_Name").contains("M47"), "M477").otherwise(col("cann_receiver")))

    //    retailWithCompCann3DF.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("D:\\files\\temp\\retail-Feb06-r-1683.csv")

    // Direct Cann
    // TODO : check the following with Commercial code
    /*retail$Direct.Cann.201[i]<-ifelse(grepl("M201",retail$SKU.Name[i]) | grepl("M203", retail$SKU.Name[i]), mean(retail$NP.IR[grepl("M40", retail$SKU.Name) & retail$Week.End.Date==retail$Week.End.Date[i]]+retail$ASP.IR[grepl("M40", retail$SKU.Name) & retail$Week.End.Date==retail$Week.End.Date[i]]),0)
        retail$Direct.Cann.225[i]<-ifelse(grepl("M225",retail$SKU.Name[i]) | grepl("M227", retail$SKU.Name[i]), mean(retail$NP.IR[grepl("M42", retail$SKU.Name) & retail$Week.End.Date==retail$Week.End.Date[i]]+retail$ASP.IR[grepl("M42", retail$SKU.Name) & retail$Week.End.Date==retail$Week.End.Date[i]]),0)
        retail$Direct.Cann.252[i]<-ifelse(grepl("M252",retail$SKU.Name[i]) | grepl("M254", retail$SKU.Name[i]), mean(retail$NP.IR[grepl("M45", retail$SKU.Name) & retail$Week.End.Date==retail$Week.End.Date[i]]+retail$ASP.IR[grepl("M45", retail$SKU.Name) & retail$Week.End.Date==retail$Week.End.Date[i]]),0)
        retail$Direct.Cann.277[i]<-ifelse(grepl("M277",retail$SKU.Name[i]) | grepl("M281", retail$SKU.Name[i]), mean(retail$NP.IR[grepl("M47", retail$SKU.Name) & retail$Week.End.Date==retail$Week.End.Date[i]]+retail$ASP.IR[grepl("M47", retail$SKU.Name) & retail$Week.End.Date==retail$Week.End.Date[i]]),0)
        retail$Direct.Cann.Weber[i]<-ifelse(retail$cann_group[i] == "Weber", mean(retail$NP.IR[retail$cann_group == "Muscatel" & retail$Week.End.Date==retail$Week.End.Date[i]]+retail$ASP.IR[retail$cann_group == "Muscatel" & retail$Week.End.Date==retail$Week.End.Date[i]]),0)
        retail$Direct.Cann.Muscatel.Weber[i]<-ifelse(retail$cann_group[i] == "Muscatel", mean(retail$NP.IR[retail$cann_group == "Weber" & retail$Week.End.Date==retail$Week.End.Date[i]]+retail$ASP.IR[retail$cann_group == "Weber" & retail$Week.End.Date==retail$Week.End.Date[i]]),0)
        retail$Direct.Cann.Muscatel.Palermo[i]<-ifelse(retail$cann_group[i] == "Muscatel", mean(retail$NP.IR[retail$cann_group == "Palermo" & retail$Week.End.Date==retail$Week.End.Date[i]]+retail$ASP.IR[retail$cann_group == "Palermo" & retail$Week.End.Date==retail$Week.End.Date[i]]),0)
        retail$Direct.Cann.Palermo[i]<-ifelse(retail$cann_group[i] == "Palermo", mean(retail$NP.IR[retail$cann_group == "Muscatel" & retail$Week.End.Date==retail$Week.End.Date[i]]+retail$ASP.IR[retail$cann_group == "Muscatel" & retail$Week.End.Date==retail$Week.End.Date[i]]),0)
        */
    retailWithCompCann3DF = retailWithCompCann3DF
      .withColumn("is201", when(col("SKU_Name").contains("M201") or col("SKU_Name").contains("M203"), 1).otherwise(0))
      .withColumn("is225", when(col("SKU_Name").contains("M225") or col("SKU_Name").contains("M227"), 1).otherwise(0))
      .withColumn("is252", when(col("SKU_Name").contains("M252") or col("SKU_Name").contains("M254"), 1).otherwise(0))
      .withColumn("is277", when(col("SKU_Name").contains("M277") or col("SKU_Name").contains("M281"), 1).otherwise(0))
      .withColumn("isM40", when(col("SKU_Name").contains("M40"), 1).otherwise(0))
      .withColumn("isM42", when(col("SKU_Name").contains("M42"), 1).otherwise(0))
      .withColumn("isM45", when(col("SKU_Name").contains("M45"), 1).otherwise(0))
      .withColumn("isM47", when(col("SKU_Name").contains("M47"), 1).otherwise(0))
      .withColumn("isWeber", when(col("cann_group") === "Weber", 1).otherwise(0))
      .withColumn("isMuscatel", when(col("cann_group") === "Muscatel", 1).otherwise(0))
      .withColumn("isPalermo", when(col("cann_group") === "Palermo", 1).otherwise(0))
      .withColumn("isMuscatelForWeber", when(col("cann_group") === "Muscatel", 1).otherwise(0))
      .withColumn("isWeberForMuscatel", when(col("cann_group") === "Weber", 1).otherwise(0))
      .withColumn("isPalermoForMuscatel", when(col("cann_group") === "Palermo", 1).otherwise(0))
      .withColumn("isPalermoForMuscatel", when(col("cann_group") === "Muscatel", 1).otherwise(0))

    val retailWeek1 = retailWithCompCann3DF.where(col("isM40") === 1).groupBy("Week_End_Date").agg(mean(col("NP_IR") + col("ASP_IR")).alias("Direct_Cann_201"))
      .withColumn("is201", lit(1))
    val retailWeek2 = retailWithCompCann3DF.where(col("isM42") === 1).groupBy("Week_End_Date").agg(mean(col("NP_IR") + col("ASP_IR")).alias("Direct_Cann_225"))
      .withColumn("is225", lit(1))
    val retailWeek3 = retailWithCompCann3DF.where(col("isM45") === 1).groupBy("Week_End_Date").agg(mean(col("NP_IR") + col("ASP_IR")).alias("Direct_Cann_252"))
      .withColumn("is252", lit(1))
    val retailWeek4 = retailWithCompCann3DF.where(col("isM47") === 1).groupBy("Week_End_Date").agg(mean(col("NP_IR") + col("ASP_IR")).alias("Direct_Cann_277"))
      .withColumn("is277", lit(1))

    val retailWeek5 = retailWithCompCann3DF.where(col("isMuscatelForWeber") === 1).groupBy("Week_End_Date").agg(mean(col("NP_IR") + col("ASP_IR")).alias("Direct_Cann_Weber"))
      .withColumn("isWeber", lit(1))
    val retailWeek6 = retailWithCompCann3DF.where(col("isWeberForMuscatel") === 1).groupBy("Week_End_Date").agg(mean(col("NP_IR") + col("ASP_IR")).alias("Direct_Cann_Muscatel_Weber"))
      .withColumn("isMuscatel", lit(1))
    val retailWeek7 = retailWithCompCann3DF.where(col("isPalermoForMuscatel") === 1).groupBy("Week_End_Date").agg(mean(col("NP_IR") + col("ASP_IR")).alias("Direct_Cann_Muscatel_Palermo"))
      .withColumn("isMuscatel", lit(1))
    val retailWeek8 = retailWithCompCann3DF.where(col("isPalermoForMuscatel") === 1).groupBy("Week_End_Date").agg(mean(col("NP_IR") + col("ASP_IR")).alias("Direct_Cann_Palermo"))
      .withColumn("isPalermo", lit(1))

    retailWithCompCann3DF = retailWithCompCann3DF.join(retailWeek1, Seq("is201", "Week_End_Date"), "left")
      .join(retailWeek2, Seq("is225", "Week_End_Date"), "left")
      .join(retailWeek3, Seq("is252", "Week_End_Date"), "left")
      .join(retailWeek4, Seq("is277", "Week_End_Date"), "left")
      .join(retailWeek5, Seq("isWeber", "Week_End_Date"), "left")
      .join(retailWeek6, Seq("isMuscatel", "Week_End_Date"), "left")
      .join(retailWeek7, Seq("isMuscatel", "Week_End_Date"), "left")
      .join(retailWeek8, Seq("isPalermo", "Week_End_Date"), "left")
      .withColumn("Direct_Cann_201", when(col("Direct_Cann_201").isNull, 0).otherwise(col("Direct_Cann_201")))
      .withColumn("Direct_Cann_225", when(col("Direct_Cann_225").isNull, 0).otherwise(col("Direct_Cann_225")))
      .withColumn("Direct_Cann_252", when(col("Direct_Cann_252").isNull, 0).otherwise(col("Direct_Cann_252")))
      .withColumn("Direct_Cann_277", when(col("Direct_Cann_277").isNull, 0).otherwise(col("Direct_Cann_277")))
      .withColumn("Direct_Cann_Weber", when(col("Direct_Cann_Weber").isNull, 0).otherwise(col("Direct_Cann_Weber")))
      .withColumn("Direct_Cann_Muscatel_Weber", when(col("Direct_Cann_Muscatel_Weber").isNull, 0).otherwise(col("Direct_Cann_Muscatel_Weber")))
      .withColumn("Direct_Cann_Muscatel_Palermo", when(col("Direct_Cann_Muscatel_Palermo").isNull, 0).otherwise(col("Direct_Cann_Muscatel_Palermo")))
      .withColumn("Direct_Cann_Palermo", when(col("Direct_Cann_Palermo").isNull, 0).otherwise(col("Direct_Cann_Palermo")))
      .withColumn("LBB", when(col("LBB").isNull, 0).otherwise(col("LBB")))
      .withColumn("LBB", when(col("Account") === "Amazon-Proper", col("LBB")).otherwise(lit(0)))

    retailWithCompCann3DF.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("/etherData/retailTemp/RetailFeatEngg/retail-DirectCann-PART14.csv")

  }
}
