package com.scienaptic.jobs.core

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.bean.{RetailHoliday, RetailHolidayTranspose, UnionOperation}
import com.scienaptic.jobs.core.RetailPreRegressionPart01.{checkPrevDistInvGTBaseline, concatenateRankWithDist, stability_range}
import com.scienaptic.jobs.utility.CommercialUtility.createlist
import com.scienaptic.jobs.utility.Utils.renameColumns
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

import scala.collection.mutable

object RetailPreRegressionPart06 {

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
    val allBrands = List("Brother", "Canon", "Epson", "Lexmark", "Samsung", "HP")

    var retailEOL  = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/retailTemp/RetailFeatEngg/retail-Holidays-PART05.csv")
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("EOL_Date", to_date(unix_timestamp(col("EOL_Date"), "yyyy-MM-dd").cast("timestamp"))).cache()

    var npd = renameColumns(executionContext.spark.read.option("header", "true").option("inferSchema", "true").csv("/etherData/managedSources/NPD/NPD_weekly.csv")).cache()
    npd.columns.toList.foreach(x => {
      npd = npd.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    npd = npd.cache()
      .withColumn("Week_End_Date", when(col("Week_End_Date").isNull || col("Week_End_Date") === "", lit(null)).otherwise(
        when(col("Week_End_Date").contains("-"), to_date(unix_timestamp(col("Week_End_Date"), "dd-MM-yyyy").cast("timestamp")))
          .otherwise(to_date(unix_timestamp(col("Week_End_Date"), "MM/dd/yyyy").cast("timestamp")))
      ))


    /*================= Brand not Main Brands =======================*/

    val npdChannelBrandFilterRetail = npd.where((col("Channel") === "Retail") && (col("Brand").isin("Canon", "Epson", "Brother", "Lexmark", "Samsung")))
      .where((col("DOLLARS") > 0) && (col("MSRP__") > 0))

    val L1Competition = npdChannelBrandFilterRetail
      .groupBy("L1_Category", "Week_End_Date", "Brand")
      .agg((sum("DOLLARS") / sum("MSRP__")).as("dolMSRPRatio"))
      .withColumn("L1_competition", lit(1) - col("dolMSRPRatio")).drop("dolMSRPRatio")
    //.join(npdChannelBrandFilterNotRetail, Seq("L1_Category","Week_End_Date","Brand"), "right")

    var L1Comp = L1Competition
      /*.withColumn("uuid", generateUUID())*/
      .groupBy("L1_Category", "Week_End_Date" /*, "uuid"*/).pivot("Brand").agg(first("L1_competition")).drop("uuid")
    val L1CompColumns = L1Comp.columns
    allBrands.foreach(x => {
      if (!L1CompColumns.contains(x))
        L1Comp = L1Comp.withColumn(x, lit(null))
    })
    allBrands.foreach(x => {
      L1Comp = L1Comp.withColumn(x, when(col(x).isNull, 0).otherwise(col(x)))
        .withColumnRenamed(x, "L1_competition_" + x)
    })


    val L2Competition = npdChannelBrandFilterRetail
      .groupBy("L2_Category", "Week_End_Date", "Brand")
      .agg((sum("DOLLARS") / sum("MSRP__")).as("dolMSRPRatio"))
      .withColumn("L2_competition", lit(1) - col("dolMSRPRatio")).drop("dolMSRPRatio")
    //.join(npdChannel6FilterNotRetail, Seq("L2_Category","Week_End_Date","Brand"), "right")

    var L2Comp = L2Competition /*.withColumn("uuid", generateUUID())*/
      .groupBy("L2_Category", "Week_End_Date" /*, "uuid"*/)
      .pivot("Brand")
      .agg(first("L2_competition")).drop("uuid")

    val L2CompColumns = L2Comp.columns
    allBrands.foreach(x => {
      if (!L2CompColumns.contains(x))
        L2Comp = L2Comp.withColumn(x, lit(null))
    })
    allBrands.foreach(x => {
      L2Comp = L2Comp.withColumn(x, when(col(x).isNull, 0).otherwise(col(x)))
        .withColumnRenamed(x, "L2_competition_" + x)
    })

    var retailWithCompetitionDF = retailEOL.join(L1Comp, Seq("L1_Category", "Week_End_Date"), "left")
      .join(L2Comp, Seq("L2_Category", "Week_End_Date"), "left")

    allBrands.foreach(x => {
      val l1Name = "L1_competition_" + x
      val l2Name = "L2_competition_" + x
      retailWithCompetitionDF = retailWithCompetitionDF.withColumn(l1Name, when((col(l1Name).isNull) || (col(l1Name) < 0), 0).otherwise(col(l1Name)))
        .withColumn(l2Name, when(col(l2Name).isNull || col(l2Name) < 0, 0).otherwise(col(l2Name)))
    })
    retailWithCompetitionDF = retailWithCompetitionDF.na.fill(0, Seq("L1_competition_Brother", "L1_competition_Canon", "L1_competition_Epson", "L1_competition_Lexmark", "L1_competition_Samsung"))
      .na.fill(0, Seq("L2_competition_Brother", "L2_competition_Epson", "L2_competition_Canon", "L2_competition_Lexmark", "L2_competition_Samsung"))

    /*====================================== Brand Not HP ================================= */

    val npdChannelNotRetailBrandNotHP = npd.where((col("Channel") === "Retail") && (col("Brand") =!= "HP"))
      .where((col("DOLLARS") > 0) && (col("MSRP__") > 0))

    val L1CompetitionNonHP = npdChannelNotRetailBrandNotHP
      .groupBy("L1_Category", "Week_End_Date")
      .agg((sum("DOLLARS") / sum("MSRP__")).as("dolMSRPRatio"))
      .withColumn("L1_competition", lit(1) - col("dolMSRPRatio")).drop("dolMSRPRatio")
    //.join(npdChannelNotRetailBrandNotHP, Seq("L1_Category","Week_End_Date"), "right")

    val L2CompetitionNonHP = npdChannelNotRetailBrandNotHP
      .groupBy("L2_Category", "Week_End_Date")
      .agg((sum("DOLLARS") / sum("MSRP__")).as("dolMSRPRatio"))
      .withColumn("L2_competition", lit(1) - col("dolMSRPRatio")).drop("dolMSRPRatio")
    //.join(npdChannelNotRetailBrandNotHP, Seq("L2_Category","Week_End_Date"), "right")

    retailWithCompetitionDF = retailWithCompetitionDF.join(L1CompetitionNonHP, Seq("L1_Category", "Week_End_Date"), "left")
      .join(L2CompetitionNonHP, Seq("L2_Category", "Week_End_Date"), "left")
      .withColumn("L1_competition", when((col("L1_competition").isNull) || (col("L1_competition") < 0), 0).otherwise(col("L1_competition")))
      .withColumn("L2_competition", when((col("L2_competition").isNull) || (col("L2_competition") < 0), 0).otherwise(col("L2_competition")))
      .na.fill(0, Seq("L1_competition", "L2_competition"))

    // write

    /*=================================== Brand Not Samsung ===================================*/

    val npdChannelNotRetailBrandNotSamsung = npd.where((col("Channel") === "Retail") && (col("Brand") =!= "Samsung"))
      .where((col("DOLLARS") > 0) && (col("MSRP__") > 0))
    val L1CompetitionSS = npdChannelNotRetailBrandNotSamsung
      .groupBy("L1_Category", "Week_End_Date")
      .agg((sum("DOLLARS") / sum("MSRP__")).as("dolMSRPRatio"))
      .withColumn("L1_competition_ss", lit(1) - col("dolMSRPRatio")).drop("dolMSRPRatio")
    //.join(npdChannelNotRetailBrandNotSamsung, Seq("L1_Category","Week_End_Date"), "right")

    val L2CompetitionSS = npdChannelNotRetailBrandNotSamsung
      .groupBy("L2_Category", "Week_End_Date")
      .agg((sum("DOLLARS") / sum("MSRP__")).as("dolMSRPRatio"))
      .withColumn("L2_competition_ss", lit(1) - col("dolMSRPRatio")).drop("dolMSRPRatio")
    //.join(npdChannelNotRetailBrandNotSamsung, Seq("L2_Category","Week_End_Date"), "right")

    retailWithCompetitionDF = retailWithCompetitionDF.join(L1CompetitionSS, Seq("L1_Category", "Week_End_Date"), "left")
      .join(L2CompetitionSS, Seq("L2_Category", "Week_End_Date"), "left")

    //write
    //    retailWithCompetitionDF.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("D:\\files\\temp\\retail-Feb06-r-1110.csv")

    retailWithCompetitionDF = retailWithCompetitionDF
      .withColumn("L1_competition_ss", when((col("L1_competition_ss").isNull) || (col("L1_competition_ss") < 0), 0).otherwise(col("L1_competition_ss")))
      .withColumn("L2_competition_ss", when((col("L2_competition_ss").isNull) || (col("L2_competition_ss") < 0), 0).otherwise(col("L2_competition_ss")))
      .na.fill(0, Seq("L1_competition_ss", "L2_competition_ss"))

    retailWithCompetitionDF = retailWithCompetitionDF
      .withColumn("L1_competition", when(col("Brand").isin("Samsung"), col("L1_competition_ss")).otherwise(col("L1_competition")))
      .withColumn("L2_competition", when(col("Brand").isin("Samsung"), col("L2_competition_ss")).otherwise(col("L2_competition")))
      .drop("L2_competition_ss", "L1_competition_ss")
    /* ========================================================================================== */

    retailWithCompetitionDF.write.option("header", true).mode(SaveMode.Overwrite).csv("/etherData/retailTemp/RetailFeatEngg/retail-L1L2-PART06.csv")
  }
}
