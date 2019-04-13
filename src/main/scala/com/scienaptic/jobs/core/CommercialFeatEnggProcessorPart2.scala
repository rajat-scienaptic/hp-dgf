package com.scienaptic.jobs.core

import java.text.SimpleDateFormat

import com.scienaptic.jobs.ExecutionContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import com.scienaptic.jobs.bean.UnionOperation.doUnion
import java.util.UUID
import org.apache.spark.storage.StorageLevel

import org.apache.spark.sql.functions.rank
import com.scienaptic.jobs.utility.Utils.renameColumns
import com.scienaptic.jobs.utility.CommercialUtility.{addDaystoDateStringUDF, checkPrevQtsGTBaseline, concatenateRank, createlist, extractWeekFromDateUDF}
import org.apache.spark.sql.expressions.Window
import com.scienaptic.jobs.utility.CommercialUtility._

object CommercialFeatEnggProcessor2 {
  val Cat_switch=1
  val min_baseline = 2
  val stability_weeks = 4
  val intro_weeks = 6

  val dat2000_01_01 = to_date(unix_timestamp(lit("2000-01-01"),"yyyy-MM-dd").cast("timestamp"))
  val dat9999_12_31 = to_date(unix_timestamp(lit("9999-12-31"),"yyyy-MM-dd").cast("timestamp"))

  def execute(executionContext: ExecutionContext): Unit = {

    val sparkConf = new SparkConf().setAppName("Test")
    val spark = SparkSession.builder
      //.master("local[*]")
      .master("yarn-client")
      .appName("Commercial-R-2")
      .config(sparkConf)
      .getOrCreate

    val baselineThreshold = if (min_baseline / 2 > 0) min_baseline / 2 else 0

    import spark.implicits._
    var commercial = spark.read.option("header", "true").option("inferSchema", "true").csv("/etherData/commercialTemp/CommercialFeatEngg/commercialBeforeNPD.csv")
    //var commercial = spark.read.option("header", "true").option("inferSchema", "true").csv("E:\\Scienaptic\\HP\\Pricing\\R\\SPARK_DEBUG_OUTPUTS\\commercialBeforeNPD.csv")
      .withColumn("ES date", to_date(unix_timestamp(col("ES date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("Week_End_Date", to_date(col("Week_End_Date")))
      .withColumn("GA date", to_date(unix_timestamp(col("GA date"), "yyyy-MM-dd").cast("timestamp")))
    //var npdDF = spark.read.option("header","true").option("inferSchema","true").csv("E:\\Scienaptic\\HP\\Pricing\\Data\\April8Run_Inputs\\NPD_weekly.csv")
    var npdDF = spark.read.option("header", "true").option("inferSchema", "true").csv("/etherData/managedSources/NPD/NPD_weekly.csv")
    var npd = renameColumns(npdDF)
    npd.columns.toList.foreach(x => {
      npd = npd.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    npd = npd.withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "MM/dd/yyyy").cast("timestamp"))).cache()
    //writeDF(npd,"npd")
    npd.write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/commercialTemp/CommercialFeatEngg/npd.csv")
    /*================= Brand not Main Brands =======================*/
    val npdChannelBrandFilterNotRetail = npd.where((col("Channel") =!= "Retail") && (col("Brand").isin("Canon", "Epson", "Brother", "Lexmark", "Samsung")))
      .where((col("DOLLARS") > 0) && (col("MSRP__") > 0)).cache()
    //writeDF(npdChannelBrandFilterNotRetail,"npdChannel_Brand_FilterNotRetail")
    val L1Competition = npdChannelBrandFilterNotRetail
      .groupBy("L1_Category", "Week_End_Date", "Brand")
      .agg((sum("DOLLARS") / sum("MSRP__")).as("dolMSRPRatio"))
      .withColumn("L1_competition", lit(1) - col("dolMSRPRatio")).drop("dolMSRPRatio")

    //val generateUUID = udf(() => UUID.randomUUID().toString)
    var L1Comp = L1Competition //.withColumn("uuid",generateUUID())
      .groupBy("L1_Category", "Week_End_Date" /*,"uuid"*/)
      .pivot("Brand").agg(first("L1_competition")).drop("uuid")
    //writeDF(L1Comp,"L1Comp")
    val allBrands = List("Brother", "Canon", "Epson", "Lexmark", "Samsung")
    val L1CompColumns = L1Comp.columns
    allBrands.foreach(x => {
      if (!L1CompColumns.contains(x))
        L1Comp = L1Comp.withColumn(x, lit(null))
    })
    allBrands.foreach(x => {
      L1Comp = L1Comp.withColumn(x, when(col(x).isNull, 0).otherwise(col(x)))
        .withColumnRenamed(x, "L1_competition_" + x)
    })
    //writeDF(L1Comp, "L1Comp")

    val L2Competition = npdChannelBrandFilterNotRetail
      .groupBy("L2_Category", "Week_End_Date", "Brand")
      .agg((sum("DOLLARS") / sum("MSRP__")).as("dolMSRPRatio"))
      .withColumn("L2_competition", lit(1) - col("dolMSRPRatio")).drop("dolMSRPRatio")
    //writeDF(L2Competition,"L2Competition")
    var L2Comp = L2Competition //.withColumn("uuid",generateUUID())
      .groupBy("L2_Category", "Week_End_Date" /*,"uuid"*/)
      .pivot("Brand")
      .agg(first("L2_competition"))
    //.drop("uuid")
    //writeDF(L2Comp,"L2Comp_BEFORE_NULL_IMPUTAITON")
    val L2CompColumns = L2Comp.columns
    allBrands.foreach(x => {
      if (!L2CompColumns.contains(x))
        L2Comp = L2Comp.withColumn(x, lit(null))
    })
    allBrands.foreach(x => {
      L2Comp = L2Comp.withColumn(x, when(col(x).isNull, 0).otherwise(col(x)))
        .withColumnRenamed(x, "L2_competition_" + x)
    })
    //writeDF(L2Comp,"L2Comp")

    commercial = commercial.join(L1Comp, Seq("L1_Category", "Week_End_Date"), "left")
    commercial = commercial
      .join(L2Comp, Seq("L2_Category", "Week_End_Date"), "left")
    allBrands.foreach(x => {
      val l1Name = "L1_competition_" + x
      val l2Name = "L2_competition_" + x
      commercial = commercial.withColumn(l1Name, when((col(l1Name).isNull) || (col(l1Name) < 0), 0).otherwise(col(l1Name)))
        .withColumn(l2Name, when(col(l2Name).isNull || col(l2Name) < 0, 0).otherwise(col(l2Name)))
    })
    //writeDF(commercialWithCompetitionDF,"commercialWithCompetitionDF_AFTER_L1_L2_JOIN")
    commercial = commercial.na.fill(0, Seq("L1_competition_Brother", "L1_competition_Canon", "L1_competition_Epson", "L1_competition_Lexmark", "L1_competition_Samsung"))
      .na.fill(0, Seq("L2_competition_Brother", "L2_competition_Epson", "L2_competition_Canon", "L2_competition_Lexmark", "L2_competition_Samsung"))
      .repartition(500).cache()
    //writeDF(commercialWithCompetitionDF,"commercialWithCompetitionDF_WITH_L1_L2")
    /*====================================== Brand Not HP =================================*/
    val npdChannelNotRetailBrandNotHP = npd.where((col("Channel") =!= "Retail") && (col("Brand") =!= "HP"))
      .where((col("DOLLARS") > 0) && (col("MSRP__") > 0)).cache()
    //writeDF(npdChannelNotRetailBrandNotHP,"npdChannelNotRetailBrandNotHP")
    var L1CompetitionNonHP = npdChannelNotRetailBrandNotHP
      .groupBy("L1_Category", "Week_End_Date")
      .agg((sum("DOLLARS") / sum("MSRP__")).as("dolMSRPRatio"))
    //writeDF(L1CompetitionNonHP,"L1CompetitionNonHP")
    L1CompetitionNonHP = L1CompetitionNonHP
      .withColumn("L1_competition", lit(1) - col("dolMSRPRatio")).drop("dolMSRPRatio")
    //writeDF(L1CompetitionNonHP,"L1CompetitionNonHP")
    val L2CompetitionNonHP = npdChannelNotRetailBrandNotHP
      .groupBy("L2_Category", "Week_End_Date")
      .agg((sum("DOLLARS") / sum("MSRP__")).as("dolMSRPRatio"))
      .withColumn("L2_competition", lit(1) - col("dolMSRPRatio")).drop("dolMSRPRatio")
    //writeDF(L2CompetitionNonHP,"L2CompetitionNonHP_378")
    commercial = commercial.join(L1CompetitionNonHP, Seq("L1_Category", "Week_End_Date"), "left")
      .join(L2CompetitionNonHP, Seq("L2_Category", "Week_End_Date"), "left")
      .withColumn("L1_competition", when((col("L1_competition").isNull) || (col("L1_competition") < 0), 0).otherwise(col("L1_competition")))
      .withColumn("L2_competition", when((col("L2_competition").isNull) || (col("L2_competition") < 0), 0).otherwise(col("L2_competition")))
      .na.fill(0, Seq("L1_competition", "L2_competition"))
    //writeDF(commercialWithCompetitionDF,"commercialWithCompetitionDF_FORNONHP_Competition")

    /*=================================== Brand Not Samsung ===================================*/
    val npdChannelNotRetailBrandNotSamsung = npd.where((col("Channel") =!= "Retail") && (col("Brand") =!= "Samsung"))
      .where((col("DOLLARS") > 0) && (col("MSRP__") > 0)).cache()
    //writeDF(npdChannelNotRetailBrandNotSamsung,"npdChannelNotRetailBrandNotSamsung")
    val L1CompetitionSS = npdChannelNotRetailBrandNotSamsung
      .groupBy("L1_Category", "Week_End_Date")
      .agg((sum("DOLLARS") / sum("MSRP__")).as("dolMSRPRatio"))
      .withColumn("L1_competition_ss", lit(1) - col("dolMSRPRatio")).drop("dolMSRPRatio")
    //writeDF(L1CompetitionSS,"L1CompetitionSS")
    val L2CompetitionSS = npdChannelNotRetailBrandNotSamsung
      .groupBy("L2_Category", "Week_End_Date")
      .agg((sum("DOLLARS") / sum("MSRP__")).as("dolMSRPRatio"))
      .withColumn("L2_competition_ss", lit(1) - col("dolMSRPRatio")).drop("dolMSRPRatio")
    //writeDF(L2CompetitionSS,"L2CompetitionSS_BEFORE_JOIN_L1_AND_L2")

    commercial = commercial.join(L1CompetitionSS, Seq("L1_Category", "Week_End_Date"), "left")
      .join(L2CompetitionSS, Seq("L2_Category", "Week_End_Date"), "left")
      .withColumn("L1_competition_ss", when((col("L1_competition_ss").isNull) || (col("L1_competition_ss") < 0), 0).otherwise(col("L1_competition_ss")))
      .withColumn("L2_competition_ss", when((col("L2_competition_ss").isNull) || (col("L2_competition_ss") < 0), 0).otherwise(col("L2_competition_ss")))
      .na.fill(0, Seq("L1_competition_ss", "L2_competition_ss"))

    commercial = commercial
      .withColumn("L1_competition", when(col("Brand").isin("Samsung"), col("L1_competition_ss")).otherwise(col("L1_competition")))
      .withColumn("L2_competition", when(col("Brand").isin("Samsung"), col("L2_competition_ss")).otherwise(col("L2_competition")))
      .drop("L2_competition_ss", "L1_competition_ss")
    //writeDF(commercialWithCompetitionDF,"commercialWithCompetitionDF_Samsung")*/

    val commercialBrandinHP = commercial.where(col("Brand").isin("HP"))
      .withColumn("Qty_pmax", greatest(col("Qty"), lit(0))).cache()
    //writeDF(commercialBrandinHP,"commercialBrandinHP_WITH_QTY_PMAX")
    val HPComp1 = commercialBrandinHP
      .groupBy("Week_End_Date", "L1_Category")
      .agg(sum("Qty_pmax").as("sum2"), (sum(col("Promo_Pct") * col("Qty_pmax"))).as("sum1"))
      .withColumn("sum1", when(col("sum1") < 0, 0).otherwise(col("sum1")))
      .withColumn("sum2", when(col("sum2") < 0, 0).otherwise(col("sum2")))
      .withColumn("L1_competition_HP_ssmodel", col("sum1") / col("sum2"))
      .drop("sum1", "sum2")
    //writeDF(HPComp1,"HPComp1")
    val HPComp2 = commercialBrandinHP
      .groupBy("Week_End_Date", "L2_Category")
      .agg(sum("Qty_pmax").as("sum2"), (sum(col("Promo_Pct") * col("Qty_pmax"))).as("sum1"))
      .withColumn("sum1", when(col("sum1") < 0, 0).otherwise(col("sum1")))
      .withColumn("sum2", when(col("sum2") < 0, 0).otherwise(col("sum2")))
      .withColumn("L2_competition_HP_ssmodel", col("sum1") / col("sum2"))
      .drop("sum1", "sum2")
    //writeDF(HPComp2,"HPComp2")
    commercial = commercial.join(HPComp1, Seq("Week_End_Date", "L1_Category"), "left")
      .join(HPComp2, Seq("Week_End_Date", "L2_Category"), "left")
      .withColumn("L1_competition_HP_ssmodel", when((col("L1_competition_HP_ssmodel").isNull) || (col("L1_competition_HP_ssmodel") < 0), 0).otherwise(col("L1_competition_HP_ssmodel")))
      .withColumn("L2_competition_HP_ssmodel", when((col("L2_competition_HP_ssmodel").isNull) || (col("L2_competition_HP_ssmodel") < 0), 0).otherwise(col("L2_competition_HP_ssmodel")))
      .na.fill(0, Seq("L1_competition_HP_ssmodel", "L2_competition_HP_ssmodel")).cache()
    //writeDF(commercial,"commercialBeforeCannibalisation")

    commercial.write.option("header", "true").mode(SaveMode.Overwrite).csv("/etherData/commercialTemp/CommercialFeatEngg/commercialBeforeCannibalisation.csv")
  }
}
