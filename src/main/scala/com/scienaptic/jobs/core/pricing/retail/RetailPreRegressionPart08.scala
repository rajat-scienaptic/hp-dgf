package com.scienaptic.jobs.core.pricing.retail

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.Utils.renameColumns
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object RetailPreRegressionPart08 {

  def execute(executionContext: ExecutionContext): Unit = {
    val spark: SparkSession = executionContext.spark
    val allBrands = List("Brother", "Canon", "Epson", "Lexmark", "Samsung", "HP")

    val retailEOL  = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/retailTemp/RetailFeatEngg/retail-Holidays-PART07.csv")
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("EOL_Date", to_date(unix_timestamp(col("EOL_Date"), "yyyy-MM-dd").cast("timestamp"))).cache()

    var npd = renameColumns(spark.read.option("header", "true").option("inferSchema", "true").csv("/etherData/managedSources/NPD/NPD_weekly.csv")).cache()
    npd.columns.toList.foreach(x => {
      npd = npd.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    npd = npd.cache()
      .withColumn("Week_End_Date", to_date(col("Week_End_Date")))
      /*.withColumn("Week_End_Date", when(col("Week_End_Date").isNull || col("Week_End_Date") === "", lit(null)).otherwise(
        when(col("Week_End_Date").contains("-"), to_date(unix_timestamp(col("Week_End_Date"), "dd-MM-yyyy").cast("timestamp")))
          .otherwise(to_date(unix_timestamp(col("Week_End_Date"), "MM/dd/yyyy").cast("timestamp")))
      ))*/

    /* ================= Brand not Main Brands ======================= */
    val npdChannelBrandFilterRetail = npd.where((col("Channel") === "Retail") && (col("Brand").isin("Canon", "Epson", "Brother", "Lexmark", "Samsung")))
      .where((col("DOLLARS") > 0) && (col("MSRP__") > 0))

    val L1Competition = npdChannelBrandFilterRetail
      .groupBy("L1_Category", "Week_End_Date", "Brand")
      .agg((sum("DOLLARS") / sum("MSRP__")).as("dolMSRPRatio"))
      .withColumn("L1_competition", lit(1) - col("dolMSRPRatio")).drop("dolMSRPRatio")

    var L1Comp = L1Competition
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

    var L2Comp = L2Competition
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

    val L2CompetitionNonHP = npdChannelNotRetailBrandNotHP
      .groupBy("L2_Category", "Week_End_Date")
      .agg((sum("DOLLARS") / sum("MSRP__")).as("dolMSRPRatio"))
      .withColumn("L2_competition", lit(1) - col("dolMSRPRatio")).drop("dolMSRPRatio")

    retailWithCompetitionDF = retailWithCompetitionDF.join(L1CompetitionNonHP, Seq("L1_Category", "Week_End_Date"), "left")
      .join(L2CompetitionNonHP, Seq("L2_Category", "Week_End_Date"), "left")
      .withColumn("L1_competition", when((col("L1_competition").isNull) || (col("L1_competition") < 0), 0).otherwise(col("L1_competition")))
      .withColumn("L2_competition", when((col("L2_competition").isNull) || (col("L2_competition") < 0), 0).otherwise(col("L2_competition")))
      .na.fill(0, Seq("L1_competition", "L2_competition"))

    /*=================================== Brand Not Samsung ===================================*/
    val npdChannelNotRetailBrandNotSamsung = npd.where((col("Channel") === "Retail") && (col("Brand") =!= "Samsung"))
      .where((col("DOLLARS") > 0) && (col("MSRP__") > 0))
    val L1CompetitionSS = npdChannelNotRetailBrandNotSamsung
      .groupBy("L1_Category", "Week_End_Date")
      .agg((sum("DOLLARS") / sum("MSRP__")).as("dolMSRPRatio"))
      .withColumn("L1_competition_ss", lit(1) - col("dolMSRPRatio")).drop("dolMSRPRatio")

    val L2CompetitionSS = npdChannelNotRetailBrandNotSamsung
      .groupBy("L2_Category", "Week_End_Date")
      .agg((sum("DOLLARS") / sum("MSRP__")).as("dolMSRPRatio"))
      .withColumn("L2_competition_ss", lit(1) - col("dolMSRPRatio")).drop("dolMSRPRatio")

    retailWithCompetitionDF = retailWithCompetitionDF.join(L1CompetitionSS, Seq("L1_Category", "Week_End_Date"), "left")
      .join(L2CompetitionSS, Seq("L2_Category", "Week_End_Date"), "left")

    retailWithCompetitionDF = retailWithCompetitionDF
      .withColumn("L1_competition_ss", when((col("L1_competition_ss").isNull) || (col("L1_competition_ss") < 0), 0).otherwise(col("L1_competition_ss")))
      .withColumn("L2_competition_ss", when((col("L2_competition_ss").isNull) || (col("L2_competition_ss") < 0), 0).otherwise(col("L2_competition_ss")))
      .na.fill(0, Seq("L1_competition_ss", "L2_competition_ss"))

    retailWithCompetitionDF = retailWithCompetitionDF
      .withColumn("L1_competition", when(col("Brand").isin("Samsung"), col("L1_competition_ss")).otherwise(col("L1_competition")))
      .withColumn("L2_competition", when(col("Brand").isin("Samsung"), col("L2_competition_ss")).otherwise(col("L2_competition")))
      .drop("L2_competition_ss", "L1_competition_ss")

    retailWithCompetitionDF.write.option("header", true).mode(SaveMode.Overwrite).csv("/etherData/retailTemp/RetailFeatEngg/retail-L1L2-PART08.csv")
  }
}
