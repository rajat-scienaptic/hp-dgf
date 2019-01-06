package com.scienaptic.jobs.core

import com.scienaptic.jobs.ExecutionContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import com.scienaptic.jobs.bean.UnionOperation.doUnion
import java.util.UUID

import com.scienaptic.jobs.utility.CommercialUtility.{addDaystoDateStringUDF, extractWeekFromDateUDF}
import org.apache.spark.sql.catalyst.expressions.aggregate.Percentile
import org.apache.spark.sql.expressions.Window

object CommercialFeatEnggProcessor {
  val Cat_switch=1
  val min_baseline = 2
  val stability_weeks = 4
  val intro_weeks = 6

  val dat2000_01_01 = to_date(unix_timestamp(lit("2000-01-01"),"yyyy-MM-dd").cast("timestamp"))
  val dat9999_12_31 = to_date(unix_timestamp(lit("9999-12-31"),"yyyy-MM-dd").cast("timestamp"))

  val indexerForSpecialPrograms = new StringIndexer().setInputCol("Special_Programs").setOutputCol("Special_Programs_fact")
  val pipelineForSpecialPrograms = new Pipeline().setStages(Array(indexerForSpecialPrograms))

  val indexerForResellerCluster = new StringIndexer().setInputCol("Reseller_Cluster").setOutputCol("Reseller_Cluster_fact")
  val pipelineForResellerCluster= new Pipeline().setStages(Array(indexerForResellerCluster))


  def execute(executionContext: ExecutionContext): Unit = {

    val sparkConf = new SparkConf().setAppName("Test")
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Retail-R")
      .config(sparkConf)
      .getOrCreate

    val baselineThreshold = if (min_baseline/2 > 0) min_baseline/2 else 0

    val commercial = spark.read.option("header","true").option("inferSchema","true").csv("posqty_output_commercial.csv")
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"),"yyyy-MM-dd").cast("timestamp")))
      .where(col("Week_End_Date") >= lit("2014-01-01"))
      .where(col("Week_End_Date") <= lit("2017-01-01"))
      .withColumnRenamed("Street_Price_Org","Street_Price")
      .cache()


    val ifs2 = spark.read.option("header","true").option("inferSchema","true").csv("ifs2_most_recent.csv")
        .withColumn("Valid_Start_Date", to_date(unix_timestamp(col("Start_End_Date"),"yyyy-MM-dd").cast("timestamp")))
      .withColumn("Valid_End_Date", to_date(unix_timestamp(col("Valid_End_Date"),"yyyy-MM-dd").cast("timestamp")))

    val commercialMergeifs2DF = commercial.join(ifs2.dropDuplicates("SKU","Street_Price").select("Street.Price", "Valid.Start.Date", "Valid.End.Date"), Seq("SKU"), "left")
      .withColumn("Valid_Start_Date", when(col("Valid_Start_Date").isNull,dat2000_01_01).otherwise(col("Valid_Start_Date")))
      .withColumn("Valid_End_Date", when(col("Valid_End_Date").isNull,dat9999_12_31).otherwise(col("Valid_End_Date")))
      .withColumn("Street_Price", when(col("Street_Price").isNull, col("Street_Price_Org")).otherwise(col("Street_Price")))
      .where((col("Week_End_Date") >= col("Valid_Start_Date")) && (col("Week_End_Date") < col("Valid_End_Date")))
      .drop("Street_Price_Org","Changed_Street_Price","Valid_Start_Date","Valid_End_Date")


    val commercialWithQtyFilterResellerAndSpProgramsFacDF = commercialMergeifs2DF.withColumn("Qty",col("Non_Big_Deal_Qty"))
      .withColumn("Reseller",when(col("Reseller")==="B & H PHOTO VIDEO CORP", lit("B & H Foto and Electronics Inc")).otherwise(col("Reseller")))
      .withColumn("Special_Programs",lit("None"))
      .withColumn("Special_Programs_LEVELS", col("Special_Programs"))
    //TODO: DOUBT: commercial$Special.Programs <- as.factor(commercial$Special.Programs)   Why to convert it to factor when it has just 1 value.


    val commercialSpclPrgmFactDF = pipelineForSpecialPrograms.fit(commercialWithQtyFilterResellerAndSpProgramsFacDF).transform(commercialWithQtyFilterResellerAndSpProgramsFacDF)
      .drop("Special_Programs").withColumnRenamed("Special_Programs_fact","Special_Programs").drop("Special_Programs_fact")
      .withColumn("Special_Programs", (col("Special_Programs")+lit(1)).cast("int"))


    //**** Get levels mapping back from stringIndexer: https://stackoverflow.com/questions/43575374/retrieve-spark-mllib-stringindexer-column-mapping/43575554

    //Checking if total =0, means division will be infinite
    val stockpiler_dealchaser = commercialSpclPrgmFactDF
      .groupBy("Reseller")
      .agg(sum("Qty").alias("Qty_total"), sum(when(col("IR")>0,"Qty")).alias("Qty_promo"))
      .join(commercialSpclPrgmFactDF, Seq("Reseller"),"right")
      .withColumn("proportion_on_promo", when((col("Qty_total")===0) || (col("proportion_on_promo").isNull), 0).otherwise(col("Qty_promo")/col("Qty_total")))
      .where(col("proportion_on_promo")>0.95)

    val commercialMatResellerStockPilerDF = commercialSpclPrgmFactDF.join(stockpiler_dealchaser.select("Reseller","proportion_on_promo"), Seq("Reseller"), "left")
      .withColumn("Reseller_Cluster", when(col("proportion_on_promo").isNotNull, lit("Stockpiler & Deal Chaser")).otherwise(col("Reseller_Cluster")))
      .withColumn("Reseller_Cluster_LEVELS", col("Reseller_Cluster"))

    val commercialResellerClusterFactDF = pipelineForResellerCluster.fit(commercialMatResellerStockPilerDF).transform(commercialMatResellerStockPilerDF)
      .drop("Reseller_Cluster").withColumnRenamed("Reseller_Cluster_fact","Reseller_Cluster").drop("Reseller_Cluster_fact")
      .withColumn("Reseller_Cluster", (col("Reseller_Cluster")+lit(1)).cast("int"))   //String indexer starts from 0
      .withColumn("Reseller_Cluster", when(col("eTailer")===1, lit("eTailer")+col("Reseller_Cluster"))otherwise(col("Reseller_Cluster").cast("string")))
      .withColumn("eTailer", lit(null))

    val commercialResFacNotPL = commercialResellerClusterFactDF.where(!col("PL").isin("E4","E0","ED"))
      .withColumn("Brand",lit("HP"))
    val commercialHPDF = commercialResFacNotPL
      .groupBy("SKU","SKU_Name","Reseller_Cluster","Week_End_Date","Season","Street_Price","IPSLES","HPS_OPS","Series","Category","Category_Subgroup","Category_1","Category_2","Category_3","Category_Custom","Line","PL","L2_Category","L2_Category","PLC_Status","GA_date","ES_date","Inv_Qty","Special_Programs")
      .agg(sum("QtY").as("Qty"), max("IR").as("IR"), sum("Big_Deal_Qty").as("Big_Deal_Qty"), sum("Non_Big_Deal_Qty").as("Non_Big_Deal_Qty"))
      .join(commercialResFacNotPL, Seq("SKU","SKU_Name","Reseller_Cluster","Week_End_Date","Season","Street_Price","IPSLES","HPS_OPS","Series","Category","Category_Subgroup","Category_1","Category_2","Category_3","Category_Custom","Line","PL","L2_Category","L2_Category","PLC_Status","GA_date","ES_date","Inv_Qty","Special_Programs"),"right")

    val commercialResFacPL = commercialResellerClusterFactDF.where(col("PL").isin("E4","E0","ED"))
    val commercialSSDF = commercialResFacPL
      .groupBy("SKU","SKU_Name","Reseller_Cluster","Week_End_Date","Season","Special_Programs")
      .agg(sum("QtY").as("Qty"), sum("Inv_Qty").as("Inv_Qty"), max("IR").as("IR"), sum("Big_Deal_Qty").as("Big_Deal_Qty"), sum("Non_Big_Deal_Qty").as("Non_Big_Deal_Qty"))
      .join(commercialResFacPL, Seq("SKU","SKU_Name","Reseller_Cluster","Week_End_Date","Season","Special_Programs"), "right")

    val auxTable = spark.read.option("header","true").option("inferSchema","true").csv("sku_hierarchy.csv")

    val commercialSSJoinSKUDF = commercialSSDF.join(auxTable, Seq("SKU"), "left")
      .withColumn("Abbreviated_Name", lit(null))
      .withColumn("Platform_Name", lit(null))
      .withColumnRenamed("HPS/OPS","HPS_OPS")
      .withColumn("Mono/Color", lit(null))
      .withColumn("Added", lit(null))
      .withColumnRenamed("L2:_Use_Case", "L2_Category")
      .withColumn("L3:_IDC_Category", lit(null))
      .withColumn("L0:_Format", lit(null))
      .withColumn("Need_Big_Data", lit(null))
      .withColumn("Need_IFS2?", lit(null))
      .withColumn("Top_SKU", lit(null))
      .withColumn("NPD_Model", lit(null))
      .withColumn("NPD_Product_Company", lit(null))
      .withColumn("Sales_Product_Name", lit(null))

    /*.withColumnRenamed("L2:_Key_functionality", )
    TODO: names(commercial_SS)[names(commercial_SS)=="L2:.Key.functionality"] <- "L2.Category"   #DOUBT why 2 columns with same name
    */

    var commercialSSFactorsDF = commercialSSJoinSKUDF

    val commercialSSFactorColumnsList = List("IPSLES", "HPS_OPS","Series","Category","Category_Subgroup","Category_1","Category_2","Category_3","Line","PL","Category_Custom","L2_Category","PLC_Status","GA_date","ES_date")
    commercialSSFactorColumnsList.foreach(column => {
      val indexer = new StringIndexer().setInputCol(column).setOutputCol(column+"_fact")
      val pipeline = new Pipeline().setStages(Array(indexer))
      commercialSSFactorsDF = commercialSSFactorsDF.withColumn(column+"_LEVELS", lit(column))
      commercialSSFactorsDF = pipeline.fit(commercialSSFactorsDF).transform(commercialSSFactorsDF)
        .drop(column).withColumnRenamed(column+"_fact",column).drop(column+"_fact")
    })

    val commercialHPandSSDF = doUnion(commercialSSFactorsDF, commercialHPDF).get
      .where(col("Street_Price") =!= 0)
      .withColumn("VPA", when(col("Reseller_Cluster").isin("CDW","PC Connection","Zones Inc","Insight Direct","PCM","GovConnection"), 1).otherwise(0))
      .withColumn("Promo_Flag", when(col("IR")>0,1).otherwise(0))
      .withColumn("Promo_Pct", col("IR")/col("Street_Price"))
      .withColumn("Discount_Depth_Category", when(col("Promo_Pct")===0, "No Discount").when(col("Promo_Pct")<=0.2, "Very Low").when(col("Promo_Pct")<=0.3, "Low").when(col("Promo_Pct")===0.4, "Moderate").when(col("Promo_Pct")<="0.5", "Heavy").otherwise(lit("Very Heavy")))
      .withColumn("log_Qty", log(col("Qty")))
      .withColumn("log_Qty", when(col("log_Qty").isNull, 0).otherwise(col("log_Qty")))
      .withColumn("price", lit(1)-col("Promo_Pct"))
      .withColumn("price", when(col("price").isNull, 0).otherwise(col("price")))
      .withColumn("Inv_Qty_log", log(col("Inv_Qty")))
      .withColumn("Inv_Qty_log", when(col("Inv_Qty_log").isNull, 0).otherwise(col("Inv_Qty_log")))
      .na.fill(0, Seq("log_Qty","price","Inv_Qty_log"))

    //TODO: Holidays custom function
    val commercialwithHolidaysDF = commercialHPandSSDF

    val npd = spark.read.option("header","true").option("inferSchema","true").csv("npd_weekly.csv")
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"),"yyyy-MM-dd").cast("timestamp")))

    /*================= Brand not Main Brands =======================*/

    val npdChannelBrandFilterNotRetail = npd.where((col("Channel") =!= "Retail") && (col("Brand").isin("Canon","Epson","Brother","Lexmark","Samsung")))
      .where((col("DOLLARS")>0) && (col("MSRP__")>0))

    val L1Competition =  npdChannelBrandFilterNotRetail  //TODO: Check if column name 'MSRP__' is correct or not
      .groupBy("L1_Category","Week_End_Date","Brand")
      .agg((sum("DOLLARS")/sum("MSRP__")).as("dolMSRPRatio"))
      .withColumn("L1_competition", lit(1)-col("dolMSRPRatio")).drop("dolMSRPRatio")
      .join(npdChannelBrandFilterNotRetail, Seq("L1_Category","Week_End_Date","Brand"), "right")

    val generateUUID = udf(() => UUID.randomUUID().toString)
    var L1Comp = L1Competition.withColumn("uuid",generateUUID()).groupBy("L1_Category","Week_End_Date","uuid").pivot("Brand").agg(first("L1_competition")).drop("uuid")
    val allBrands = List("Brother","Canon","Epson","Lexmark","Samsung")
    val L1CompColumns = L1Comp.columns
    allBrands.foreach(x => {
      if (!L1CompColumns.contains(x))
        L1Comp = L1Comp.withColumn(x, lit(null))
    })
    allBrands.foreach(x => {
      L1Comp = L1Comp.withColumn(x, when(col(x).isNull, 0).otherwise(col(x)))
          .withColumnRenamed(x, "L1_competition_"+x)
    })


    val L2Competition = npdChannelBrandFilterNotRetail
      .groupBy("L2_Category","Week_End_Date","Brand")
      .agg((sum("DOLLARS")/sum("MSRP__")).as("dolMSRPRatio"))
      .withColumn("L2_competition", lit(1)-col("dolMSRPRatio")).drop("dolMSRPRatio")
      .join(npdChannelBrandFilterNotRetail, Seq("L2_Category","Week_End_Date","Brand"), "right")

    var L2Comp = L2Competition.withColumn("uuid",generateUUID())
      .groupBy("L2_Category","Week_End_Date","uuid")
      .pivot("Brand")
      .agg(first("L1_competition")).drop("uuid")

    val L2CompColumns = L2Comp.columns
    allBrands.foreach(x => {
      if (!L2CompColumns.contains(x))
        L2Comp = L2Comp.withColumn(x, lit(null))
    })
    allBrands.foreach(x => {
      L2Comp = L2Comp.withColumn(x, when(col(x).isNull, 0).otherwise(col(x)))
        .withColumnRenamed(x, "L2_competition_"+x)
    })

    var commercialWithCompetitionDF = commercialwithHolidaysDF.join(L1Comp, Seq("L1_Category","Week_End_Date"), "left")
      .join(L2Comp, Seq("L2_Category","Week_End_Date"), "left")
    allBrands.foreach(x => {
      val l1Name = "L1_competition_"+x; val l2Name = "L2_competition_"+x
      commercialWithCompetitionDF = commercialWithCompetitionDF.withColumn(l1Name, when((col(l1Name).isNull) || (col(l1Name)<0), 0).otherwise(col(l1Name)))
          .withColumn(l2Name, when((col(l2Name).isNull) || (col(l2Name)<0), 0).otherwise(col(l2Name)))
    })
    commercialWithCompetitionDF= commercialWithCompetitionDF.na.fill(0, Seq("L1_competition_Brother","L1_competition_Canon","L1_competition_Epson","L1_competition_Lexmark","L1_competition_Samsung"))
        .na.fill(0, Seq("L2_competition_Brother","L2_competition_Epson","L2_competition_Canon","L2_competition_Lexmark","L2_competition_Samsung"))

    /*====================================== Brand Not HP ================================= */

    val npdChannelNotRetailBrandNotHP = npd.where((col("Channel")=!="Retail") && (col("Brand")=!="HP"))
      .where((col("DOLLARS")>0) && (col("MSRP__")>0))

    val L1CompetitionNonHP = npdChannelNotRetailBrandNotHP
      .groupBy("L1_Category","Week_End_Date")
      .agg(sum("DOLLARS")/sum("MSRP__").as("dolMSRPRatio"))
      .withColumn("L1_competition", lit(1)-col("dolMSRPRatio")).drop("dolMSRPRatio")
      .join(npdChannelNotRetailBrandNotHP, Seq("L1_Category","Week_End_Date"), "right")

    val L2CompetitionNonHP = npdChannelNotRetailBrandNotHP
      .groupBy("L2_Category","Week_End_Date")
      .agg(sum("DOLLARS")/sum("MSRP__").as("dolMSRPRatio"))
      .withColumn("L2_competition", lit(1)-col("dolMSRPRatio")).drop("dolMSRPRatio")
      .join(npdChannelNotRetailBrandNotHP, Seq("L2_Category","Week_End_Date"), "right")

    commercialWithCompetitionDF = commercialWithCompetitionDF.join(L1CompetitionNonHP, Seq("L1_Category","Week_End_Date"), "left")
        .join(L2CompetitionNonHP, Seq("L2_Category","Week_End_Date"), "left")
        .withColumn("L1_competition", when((col("L1_competition").isNull) || (col("L1_competition")<0), 0).otherwise(col("L1_competition")))
        .withColumn("L2_competition", when((col("L2_competition").isNull) || (col("L2_competition")<0), 0).otherwise(col("L2_competition")))
        .na.fill(0, Seq("L1_competition","L2_competition"))

    /*=================================== Brand Not Samsung ===================================*/

    val npdChannelNotRetailBrandNotSamsung = npd.where((col("Channel")=!="Retail") && (col("Brand")=!="Samsung"))
      .where((col("DOLLARS")>0) && (col("MSRP__")>0))
    val L1CompetitionSS = npdChannelNotRetailBrandNotSamsung
      .groupBy("L1_Category","Week_End_Date")
      .agg(sum("DOLLARS")/sum("MSRP__").as("dolMSRPRatio"))
      .withColumn("L1_competition_ss", lit(1)-col("dolMSRPRatio")).drop("dolMSRPRatio")
      .join(npdChannelNotRetailBrandNotSamsung, Seq("L1_Category","Week_End_Date"), "right")

    val L2CompetitionSS = npdChannelNotRetailBrandNotSamsung
      .groupBy("L2_Category","Week_End_Date")
      .agg(sum("DOLLARS")/sum("MSRP__").as("dolMSRPRatio"))
      .withColumn("L2_competition_ss", lit(1)-col("dolMSRPRatio")).drop("dolMSRPRatio")
      .join(npdChannelNotRetailBrandNotSamsung, Seq("L2_Category","Week_End_Date"), "right")

    commercialWithCompetitionDF = commercialWithCompetitionDF.join(L1CompetitionSS, Seq("L1_Category","Week_End_Date", "left"))
        .join(L2CompetitionSS, Seq("L2_Category","Week_End_Date"), "left")
      .withColumn("L1_competition_ss", when((col("L1_competition_ss").isNull) || (col("L1_competition_ss")<0), 0).otherwise(col("L1_competition_ss")))
      .withColumn("L2_competition_ss", when((col("L2_competition_ss").isNull) || (col("L2_competition_ss")<0), 0).otherwise(col("L2_competition_ss")))
      .na.fill(0, Seq("L1_competition_ss","L2_competition_ss"))

    commercialWithCompetitionDF = commercialWithCompetitionDF
        .withColumn("L1_competition", when(col("Brand").isin("Samsung"), col("L1_competition_ss")).otherwise(col("L1_competition")))
        .withColumn("L2_competition", when(col("Brand").isin("Samsung"), col("L2_competition_ss")).otherwise(col("L2_competition")))
        .drop("L2_competition_ss","L1_competition_ss")
    /* ========================================================================================== */

    val commercialBrandinHP = commercialWithCompetitionDF.where(col("Brand").isin("HP"))

    val HPComp1 = commercialBrandinHP
      .groupBy("Week_End_Date","L1_Category")
      .agg(max("Qty").as("sum2"), (sum("Promo_Pct")*max("Qty").as("sum1")))
      .withColumn("sum1", when(col("sum1")<0, 0).otherwise(col("sum1")))
      .withColumn("sum2", when(col("sum2")<0, 0).otherwise(col("sum2")))
      .withColumn("L1_competition_HP_ssmodel", col("sum1")/col("sum2"))
      .drop("sum1","sum2")
      .join(commercialBrandinHP, Seq("Week_End_Date","L1_Category"), "right")

    val HPComp2 = commercialBrandinHP
      .groupBy("Week_End_Date","L2_Category")
      .agg(max("Qty").as("sum2"), (sum("Promo_Pct")*max("Qty").as("sum1")))
      .withColumn("sum1", when(col("sum1")<0, 0).otherwise(col("sum1")))
      .withColumn("sum2", when(col("sum2")<0, 0).otherwise(col("sum2")))
      .withColumn("L2_competition_HP_ssmodel", col("sum1")/col("sum2"))
      .drop("sum1","sum2")
      .join(commercialBrandinHP, Seq("Week_End_Date","L2_Category"), "right")

    commercialWithCompetitionDF = commercialWithCompetitionDF.join(HPComp1, Seq("Week_End_Date","L1_Category"), "left")
        .join(HPComp2, Seq("Week_End_Date","L2_Category", "left"))
      .withColumn("L1_competition_HP_ssmodel", when((col("L1_competition_HP_ssmodel").isNull) || (col("L1_competition_HP_ssmodel")<0), 0).otherwise(col("L1_competition_HP_ssmodel")))
      .withColumn("L2_competition_HP_ssmodel", when((col("L2_competition_HP_ssmodel").isNull) || (col("L2_competition_HP_ssmodel")<0), 0).otherwise(col("L2_competition_HP_ssmodel")))
      .na.fill(0, Seq("L1_competition_HP_ssmodel","L2_competition_HP_ssmodel"))


    /* TODO
    * ave2 <- function (x, y, ...) {
        if(missing(...))
          x[] <- sum(x, na.rm = T)
        else {
          g <- interaction(...)
          z <- unsplit(lapply(split(x*y, g), sum, na.rm = T),g)
          w <- unsplit(lapply(split(y, g), sum, na.rm = T),g)
        }
        (z-(x*y))/(w-y)
      }
      commercial$L1_cannibalization <- ave2(commercial$Promo.Pct,ifelse(commercial$Qty<0,0,commercial$Qty),commercial$Week.End.Date,commercial$L1.Category)
      commercial$L2_cannibalization <- ave2(commercial$Promo.Pct,ifelse(commercial$Qty<0,0,commercial$Qty),commercial$Week.End.Date,commercial$L2.Category)
    * */
    var commercialWithCompCannDF = commercialWithCompetitionDF

    val commercialWithAdj = commercialWithCompCannDF.withColumn("Adj_Qty", when(col("Qty")<=0,0).otherwise(col("Qty")))
    val commercialGroupWEDSKU = commercialWithAdj.groupBy("Week_End_Date","SKU")
      .agg(sum(col("Promo_Pct")*col("Adj_Qty")).as("sumSKU1"), sum("Adj_Qty").as("sumSKU2"))
      .join(commercialWithAdj, Seq("Week_End_Date","SKU"), "right")

    val commercialGroupWEDL1 = commercialGroupWEDSKU
      .groupBy("Week_End_Date", "L1_Category")
      .agg(sum(col("Promo_Pct")*col("Adj_Qty")).as("sum1"), sum("Adj_Qty").as("sum2"))
      .withColumn("L1_cannabalization", (col("sum1")-col("sumSKU1"))/(col("sum2")-col("sumSKU2")))
      .join(commercialGroupWEDSKU, Seq("Week_End_Date", "L1_Category"), "right")

    commercialWithCompCannDF = commercialGroupWEDL1
      .groupBy("Week_End_Date", "L2_Category")
      .agg(sum(col("Promo_Pct")*col("Adj_Qty")).as("sum1"), sum("Adj_Qty").as("sum2"))
      .withColumn("L2_cannabalization", (col("sum1")-col("sumSKU1"))/(col("sum2")-col("sumSKU2")))
      .join(commercialGroupWEDL1, Seq("Week_End_Date", "L2_Category"), "right")     //TODO: .ungroup()
      .withColumn("L1_cannibalization", when(col("L1_cannibalization").isNull, 0).otherwise(col("L1_cannibalization")))
      .withColumn("L2_cannibalization", when(col("L2_cannibalization").isNull, 0).otherwise(col("L2_cannibalization")))
      .na.fill(0, Seq("L2_cannibalization","L1_cannibalization"))
      .withColumn("Sale_Price", col("Street_Price")-col("IR"))

    val AverageWeeklySales = commercialWithCompCannDF
      .groupBy("SKU","Reseller_Cluster","Sale_Price","Street_Price")
      .agg(mean(col("Qty")).as("POS_Qty"))
      .join(commercialWithCompCannDF, Seq("SKU","Reseller_Cluster","Sale_Price","Street_Price"), "right")

    var npdChannelNotRetail = npd.where(col("Channel")=!="Retail")
      .withColumn("Year", year(col("Week_End_Date")).cast("string")).withColumn("Year_LEVELS", col("year"))
      //.withColumn("Week", dayofweek(col("Week_End_Date")).cast("string")).withColumn("Week_LEVELS",col("Week"))
      .withColumn("Week", extractWeekFromDateUDF(col("Week_End_Date").cast("string"), lit("yyyy-MM-dd")))

    List("Year","Week").foreach(x => {
      val indexer = new StringIndexer().setInputCol(x).setOutputCol(x+"_fact")
      val pipeline = new Pipeline().setStages(Array(indexer))
      npdChannelNotRetail = pipeline.fit(npdChannelNotRetail).transform(npdChannelNotRetail)
        .drop(x).withColumnRenamed(x+"_fact",x).drop(x+"_fact")
    })

    val npdFilteredL1CategoryDF = npdChannelNotRetail.where(col("L1_Category").isNotNull)
    var seasonalityNPD = npdFilteredL1CategoryDF
      .groupBy("Week","L1_Category")
      .agg(sum("UNITS").as("UNITS"), countDistinct("Year").as("Years"))
      .join(npdFilteredL1CategoryDF, Seq("Week","L1_Category"), "right")
      .withColumn("UNITS_average", col("UNITS")/col("Years"))

    val seasonalityNPDSum = seasonalityNPD
      .groupBy("L1_Category")
      .count().as("number_weeks")
      .agg(sum("UNITS_average").as("UNITS_average"))
      .withColumn("average", col("UNITS_average")/col("number_weeks"))
      .drop("UNITS_average","number_weeks")

    seasonalityNPD = seasonalityNPD.join(seasonalityNPDSum, Seq("L1_Category"), "left")
        .withColumn("seasonality_npd", (col("UNITS_average")/col("average"))-lit(1))
        .drop("UNITS","UNITS_average","average","Years")

    val weekindexer = new StringIndexer().setInputCol("Week").setOutputCol("Week_fact")
    val weekpipeline = new Pipeline().setStages(Array(weekindexer))
    commercialWithCompCannDF = commercialWithCompCannDF.withColumn("Week", extractWeekFromDateUDF(col("Week_End_Date").cast("string"), lit("yyyy-MM-dd"))).withColumn("Week_LEVELS",col("Week"))
    commercialWithCompCannDF = weekpipeline.fit(commercialWithCompCannDF).transform(commercialWithCompCannDF)
        .drop("Week").withColumnRenamed("Week_fact","Week").drop("Week_fact")

    val seasonalityNPDScanner = seasonalityNPD.where(col("L1_Category")==="Office - Personal")
      .withColumn("L1_Category", when(col("L1_Category")==="Office - Personal", "Scanners").otherwise(col("L1_Category")))
    //TODO: levels(seasonality_npd_scanner$L1.Category) <- c(levels(seasonality_npd_scanner$L1.Category), "Scanners")

    seasonalityNPD = doUnion(seasonalityNPD, seasonalityNPDScanner).get

    commercialWithCompCannDF = commercialWithCompCannDF.join(seasonalityNPD, Seq("L1_Category","Week"), "left")
        .drop("Week")
        .withColumn("seasonality_npd2", when((col("USCyberMonday")===1) || (col("USThanksgivingDay")===1),0).otherwise(col("seasonality_npd").cast("int")))
        .join(ifs2.where(col("Account")==="Commercial").select("SKU","Hardware.GM","Supplies.GM","Hardware.Rev","Supplies.Rev", "Changed.Street.Price", "Valid.Start.Date", "Valid.End.Date"), Seq("SKU"), "left")
        .withColumn("Valid_Start_Date", when(col("Valid_Start_Date").isNull, dat2000_01_01).otherwise(col("Valid_Start_Date")))
        .withColumn("Valid_End_Date", when(col("Valid_End_Date").isNull, dat9999_12_31).otherwise(col("Valid_End_Date")))
        .where((col("Week_End_Date")>=col("Valid_Start_Date")) && (col("Week_End_Date")<col("Valid_End_Date")))
        .withColumn("Changed_Street_Price", when(col("Changed_Street_Price").isNull, 0).otherwise(col("Changed_Street_Price")))

    val ifs2FilteredAccount = ifs2.where(col("Account").isin("Best Buy","Office Depot-Max","Staples"))
    val ifs2RetailAvg = ifs2FilteredAccount
      .groupBy("SKU")
      .agg(mean("Hardware_GM").as("Hardware_GM_retail_avg"), mean("Hardware_Rev").as("Hardware_Rev_retail_avg"), mean("Supplies_GM").as("Supplies_GM_retail_avg"), mean("Supplies_Rev").as("Supplies_Rev_retail_avg"))


    commercialWithCompCannDF = commercialWithCompCannDF.join(ifs2RetailAvg.select("SKU","Hardware.GM_retail_avg","Supplies.GM_retail_avg","Hardware.Rev_retail_avg","Supplies.Rev_retail_avg"), Seq("SKU"), "left")
      .withColumn("Hardware_GM_type", when(col("Hardware_GM").isNotNull, "Commercial").otherwise(when((col("Hardware_GM").isNull) && (col("Hardware_GM_retail_avg").isNotNull), "Retail").otherwise(lit(null))))
      .withColumn("Hardware_Rev_type", when(col("Hardware_Rev").isNotNull, "Commercial").otherwise(when((col("Hardware_Rev").isNull) && (col("Hardware_Rev_retail_avg").isNotNull), "Retail").otherwise(lit(null))))
      .withColumn("Supplies_GM_type", when(col("Supplies_GM").isNotNull, "Commercial").otherwise(when((col("Supplies_GM").isNull) && (col("Supplies_GM_retail_avg").isNotNull), "Retail").otherwise(lit(null))))
      .withColumn("Supplies_Rev_type", when(col("Supplies_Rev").isNotNull, "Commercial").otherwise(when((col("Supplies_Rev").isNull) && (col("Supplies_Rev_retail_avg").isNotNull), "Retail").otherwise(lit(null))))
      .withColumn("Hardware_GM", when(col("Hardware_GM").isNull, col("Hardware_GM_retail_avg")).otherwise(col("Hardware_GM")))
      .withColumn("Hardware_Rev", when(col("Hardware_Rev").isNull, col("Hardware_Rev_retail_avg")).otherwise(col("Hardware_Rev")))
      .withColumn("Supplies_GM", when(col("Supplies_GM").isNull, col("Supplies_GM_retail_avg")).otherwise(col("Supplies_GM")))
      .withColumn("Supplies_Rev", when(col("Supplies_Rev").isNull, col("Supplies_Rev_retail_avg")).otherwise(col("Supplies_Rev")))

    val avgDiscountSKUAccountDF = commercialWithCompCannDF
      .groupBy("SKU_Name","Reseller_Cluster")
      .agg((sum(col("Qty")*col("IR"))/sum(col("Qty")*col("Street_Price"))).as("avg_discount_SKU_Account"))

    commercialWithCompCannDF = commercialWithCompCannDF.join(avgDiscountSKUAccountDF, Seq("SKU_Name","Resller_Cluster"), "left")
        .withColumn("avg_discount_SKU_Account", when(col("avg_discount_SKU_Account").isNull, 0).otherwise(col("avg_discount_SKU_Account")))
        .na.fill(0, Seq("avg_discount_SKU_Account"))
        .withColumn("supplies.GM.scaling.factor", lit(-0.3))
        .withColumn("Supplies.GM_unscaled", col("Supplies_GM"))
        .withColumn("Supplies_GM", col("Supplies_GM_unscaled")*(lit(1)+((col("Promo_Pct")-col("avg_discount_SKU_Account"))*col("supplies_GM_scaling_factor"))))
        .withColumn("Supplies_GM_no_promo", col("Supplies_GM_unscaled")*(lit(1)+((lit(0)-col("avg_discount_SKU_Account"))*col("supplies_GM_scaling_factor"))))
        .withColumn("Supplies_Rev_unscaled", col("Supplies_Rev"))
        .withColumn("Supplies_Rev", col("Supplies_Rev_unscaled")*(lit(1)+((col("Promo_Pct")-col("avg_discount_SKU_Account"))*col("supplies_GM_scaling_factor"))))
        .withColumn("Supplies_Rev_no_promo", col("Supplies_Rev_unscaled")*(lit(1)+((lit(0)-col("avg_discount_SKU_Account"))*col("supplies_GM_scaling_factor"))))
        .drop("Hardware_GM_retail_avg","Hardware_Rev_retail_avg","Supplies_GM_retail_avg","Supplies_Rev_retail_avg")
        .withColumn("L1_cannibalization_log", log(lit(1)-col("L1_cannibalization")))
        .withColumn("L2_cannibalization_log", log(lit(1)-col("L2_cannibalization")))
        .withColumn("L1_competition_log", log(lit(1)-col("L1_competition")))
        .withColumn("L2_competition_log", log(lit(1)-col("L1_competition")))
        .withColumn("L1_cannibalization_log", when(col("L1_cannibalization_log").isNull, 0).otherwise(col("L1_cannibalization_log")))
        .withColumn("L2_cannibalization_log", when(col("L2_cannibalization_log").isNull, 0).otherwise(col("L2_cannibalization_log")))
        .withColumn("L1_competition_log", when(col("L1_competition_log").isNull, 0).otherwise(col("L1_competition_log")))
        .withColumn("L2_competition_log", when(col("L2_competition_log").isNull, 0).otherwise(col("L2_competition_log")))
        .withColumn("Big_Deal", when(col("Big_Deal_Qty")>0, 1).otherwise(lit(0)))
        .withColumn("Big_Deal_Qty_log", log(when(col("Big_Deal_Qty")<1,1).otherwise(col("Big_Deal_Qty"))))

    val wind = Window.partitionBy("SKU_Name","Reseller_Cluster").orderBy("Qty")
    //df2 = sqlContext.sql("select agent_id, percentile_approx(payment_amount,0.95) as approxQuantile from df group by agent_id")
    commercialWithCompCannDF.createOrReplaceTempView("commercial")

    val percentil75DF = spark.sql("select SKU_Name, Reseller_Cluster, percentile_approx(Qty,0.75) as percentile_0_75 from commercial group by SKU_Name, Reseller_Cluster")
    commercialWithCompCannDF = commercialWithCompCannDF.join(percentil75DF, Seq("SKU_Name", "Reseller_Cluster"), "left")
    commercialWithCompCannDF.createOrReplaceTempView("commercial")
    val percentile25DF = spark.sql("select SKU_Name, Reseller_Cluster, percentile_approx(Qty,0.25) as percentile_0_25 from commercial group by SKU_Name, Reseller_Cluster")
    commercialWithCompCannDF = commercialWithCompCannDF.join(percentile25DF, Seq("SKU_Name", "Reseller_Cluster"), "left")

    commercialWithCompCannDF = commercialWithCompCannDF.withColumn("IQR", col("percentile_0_75")-col("percentile_0_25"))
        .withColumn("outlier", when(col("Qty")>col("percentile_0_75"), (col("Qty")-col("percentile_0_75"))/col("IQR")).otherwise(when(col("Qty")<col("percentile_0_25"), (col("Qty")-col("percentile_0_25"))/col("IQR")).otherwise(lit(0))))
        .withColumn("spike", when(abs(col("outlier"))<=8, 0).otherwise(1))
        .withColumn("spike", when((col("SKU_Name")==="OJ Pro 8610") && (col("Reseller_Cluster")==="Other - Option B") && (col("Week_End_Date")==="2014-11-01"),1).otherwise(col("spike")))
        .withColumn("spike", when((col("SKU").isin("F6W14A")) && (col("Week_End_Date")==="10/7/2017") && (col("Reseller_Cluster").isin("Other - Option B")), 1).otherwise(col("spike")))
        .withColumn("spike2", when((col("spike")===1) && (col("IR")>0), 0).otherwise(col("spike")))
        .drop("percentile_0_75", "percentile_0_25","IQR")
        .withColumn("Qty", col("Qty").cast("int"))

    val commercialWithHolidayAndQtyFilter = commercialWithCompCannDF.where((col("Promo_Flag")===0) && (col("USThanksgivingDay")===0) && (col("USCyberMonday")===0) && (col("spike")===0))
      .where(col("Qty")>0)
    var npbl = commercialWithHolidayAndQtyFilter
      .groupBy("Reseller_Cluster","SKU_Name")
      .agg(mean("Qty").as("no_promo_avg"), stddev("Qty").as("no_promo_sd"), min("Qty").as("no_promo_min"), max("Qty").as("no_promo_max"))
    npbl.createOrReplaceTempView("npbl")
    npbl = spark.sql("select *,percentile_approx(Qty, 0.5) as no_promo_med from npbl group by Reseller_Cluster,SKU_Name")

    commercialWithCompCannDF = commercialWithCompCannDF.join(npbl.select("SKU_Name", "Reseller_Cluster","no_promo_avg", "no_promo_med"), Seq("SKU_Name","Reseller_Cluster"), "left")
        .withColumn("no_promo_avg", when(col("no_promo_avg").isNull, 0).otherwise(col("no_promo_avg")))
        .withColumn("no_promo_med", when(col("no_promo_med").isNull, 0).otherwise(col("no_promo_med")))
        .withColumn("low_baseline", when(((col("no_promo_avg")>=min_baseline) && (col("no_promo_med")>baselineThreshold)) || ((col("no_promo_med")>=min_baseline) && (col("no_promo_avg")>=baselineThreshold)),0).otherwise(1))
        .withColumn("low_volume", when(col("Qty")>0,0).otherwise(1))
        .withColumn("raw_bl_avg", col("no_promo_avg")*(col("seasonality_npd")+lit(1)))
        .withColumn("raw_bl_med", col("no_promo_med")*(col("seasonality_npd")+lit(1)))


    /*TODO
    * EOL_criterion_commercial <- function (x) {
      Qty <- as.numeric(do.call(rbind, strsplit(x, split=";"))[,1])
      baseline <- as.numeric(do.call(rbind, strsplit(x, split=";"))[,2])
      temp <- NULL
      d <- NULL
      for (i in length(Qty):1){
        if (Qty[i]<baseline[i] | i<=stability_weeks){
          temp[i] <- 0
        }else{
          for (j in 1:stability_weeks) {
            d[j] <- Qty[i-j]>baseline[i]}
          if (length(d[d==TRUE])>=1){
            temp[i] <- 1
          }else{
            temp[i] <-0
          }
        }
      }
      temp
    }
    * */

    var EOLcriterion = commercialWithCompCannDF
      .groupBy("SKU_Name","Reseller_Cluster","Week_End_Date")
      .agg(sum("Qty").as("Qty"), sum("no_promo_med").as("no_promo_med"))
      .join(commercialWithCompCannDF, Seq("SKU_Name","Reseller_Cluster","Week_End_Date"), "right")
      .sort("SKU_Name","Reseller_Cluster","Week_End_Date")
      .withColumn("Qty&no_promo_med", concat_ws(";",col("Qty"), col("no_promo_med")))
        .withColumn("SKU&Reseller", concat(col("SKU_Name"), col("Reseller_Cluster")))
    //TODO: EOL_criterion$EOL_criterion <- ave(Qty&no_promo_med, SKU&Reseller, EOL_criterion_commercial)
    //  Already created 2 columns. It would be -------> ave(Qty&no_promo_med, SKU&Reseller, EOL_criterion_commercial)
    EOLcriterion = EOLcriterion

    val EOLCriterionLast = EOLcriterion.where(col("EOL_criterion")===1)
      .groupBy("SKU_Name","Reseller_Cluster")
      .agg(max("Week_End_Date").as("last_date"))
      .join(EOLcriterion.where(col("EOL_criterion")===1), Seq("SKU_Name","Reseller_Cluster"), "right")

    val EOLCriterionMax = commercialWithCompCannDF
      .groupBy("SKU_Name","Reseller_Cluster")
      .agg(max("Week_End_Date").as("max_date"))
      .join(commercialWithCompCannDF, Seq("SKU_Name","Reseller_Cluster"), "right")

    EOLcriterion = EOLCriterionMax.join(EOLCriterionLast, Seq("SKU_Name","Reseller_Cluster"), "left")
        .where(col("last_date").isNotNull)

    val maxMaxDate = EOLcriterion.agg(max("max_date")).head().getString(0)
    EOLcriterion = EOLcriterion
        .where(!((col("max_date")===col("last_date")) && (col("last_date")===maxMaxDate)))
        .drop("max_date")

    commercialWithCompCannDF = commercialWithCompCannDF.join(EOLcriterion, Seq("SKU_Name","Reseller_Cluster"), "left")
        .withColumn("EOL", when(col("last_date").isNull, 0).otherwise(when(col("Week_End_Date")>col("last_date"),1).otherwise(0)))
        .drop("last_date")
        .withColumn("EOL", when(col("SKU").isin("G3Q47A","M9L75A","F8B04A","B5L24A","L2719A","D3Q19A","F2A70A","CF377A","L2747A","F0V69A","G3Q35A","C5F93A","CZ271A","CF379A","B5L25A","D3Q15A","B5L26A","L2741A","CF378A","L2749A","CF394A"),0).otherwise(col("EOL")))
        .withColumn("EOL", when((col("SKU")==="C5F94A") && (col("Season")=!="STS'17"), 0).otherwise(col("EOL")))

    var BOL = commercialWithCompCannDF.select("SKU","ES_date","GA_date")
      .dropDuplicates()
      .where((col("ES_date").isNotNull) || (col("GA_date").isNotNull))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"),"yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"),"yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date_wday", dayofweek(col("ES_date")).cast("int"))  //As dayofweek returns in range 1-7 we want 0-6
      .withColumn("GA_date_wday", dayofweek(col("GA_date")).cast("int"))
      .withColumn("GA_date", addDaystoDateStringUDF(col("GA_date").cast("timestamp").cast("string"), lit(7)-col("GA_date_wday")))
      .withColumn("ES_date", addDaystoDateStringUDF(col("ES_date").cast("timestamp").cast("string"), lit(7)-col("ES_date_wday")))
      .drop("GA_date_wday","ES_date_wday")

    /*TODO
    * BOL_criterion_v3 <- function (x) {
      temp <- NULL
      for (i in 1:length(x)){
        if (i < intro_weeks){
          temp[i] <- 0
        }else{
          temp[i] <- 1
        }
      }
      temp
    }
    * */

    var BOLCriterion = commercialWithCompCannDF
      .groupBy("SKU","Reseller_Cluster","Week_End_Date")
      .agg(sum("Qty").as("Qty"))
      .sort("SKU","Reseller_Cluster","Week_End_Date")
      /*TODO: .withColumn("BOL_criterion", )
        BOL_criterion$BOL_criterion <- ave(BOL_criterion$Qty, paste0(BOL_criterion$SKU, BOL_criterion$Reseller.Cluster), FUN = BOL_criterion_v3)
       */
      .drop("Qty")

    BOLCriterion = BOLCriterion.join(BOL.select("SKU","GA_date"), Seq("SKU"), "left")
    val minWEDDate = to_date(unix_timestamp(lit(BOLCriterion.agg(min("Week_End_Date")).head().getString(0)),"yyyy-MM-dd").cast("timestamp"))

    BOLCriterion = BOLCriterion.withColumn("GA_date", when(col("GA_date").isNull, minWEDDate).otherwise(col("GA_date")))
        .where(col("Week_End_Date")>=col("GA_date"))

    val BOLCriterionFirst = BOLCriterion.where(col("BOL_criterion")===1)
      .groupBy("SKU","Reseller_Cluster")
      .agg(min("Week_End_Date").as("first_date"))
      .join(BOLCriterion.where(col("BOL_criterion")===1), Seq("SKU","Reseller_Cluster"), "right")

    val BOLCriterionMax = commercialWithCompCannDF
      .groupBy("SKU","Reseller_Cluster")
      .agg(min("Week_End_Date").as("max_date"))
      .join(commercialWithCompCannDF, Seq("SKU","Reseller_Cluster"), "right")

    val BOLCriterionMin = BOLCriterion.where(col("BOL_criterion")===1)
      .groupBy("SKU","Reseller_Cluster")
      .agg(min("Week_End_Date").as("min_date"))
      .join(commercialWithCompCannDF, Seq("SKU","Reseller_Cluster"), "right")

    BOLCriterion =  BOLCriterionMax.join(BOLCriterionFirst, Seq("SKU","Reseller_Cluster"), "left")
        .join(BOLCriterionMin, Seq("SKU","Reseller_Cluster"), "left")

    BOLCriterion = BOLCriterion.withColumn("first_date", when(col("first_date").isNull, col("max_date")).otherwise(col("first_date")))
        .drop("max_date")
    val minMinDateBOL = BOLCriterion.agg(min("min_date")).head().getString(0)
    BOLCriterion = BOLCriterion
        .where(!((col("min_date")===col("first_date")) && (col("first_date")===minMinDateBOL)))
        .withColumn("diff_weeks", ((col("first_date")-col("min_date"))/7)+1)
    /*TODO: BOL_criterion <- BOL_criterion[rep(row.names(BOL_criterion), BOL_criterion$diff_weeks),]
      Repeat 'row_names' that rows corresponding 'diff_weeks' times.
     */

    /*TODO
    * BOL_criterion$add <- t(as.data.frame(strsplit(row.names(BOL_criterion), "\\.")))[,2]
      BOL_criterion$add <- ifelse(grepl("\\.",row.names(BOL_criterion))==FALSE,0,as.numeric(BOL_criterion$add))
      BOL_criterion$add <- BOL_criterion$add*7
      BOL_criterion$Week.End.Date <- BOL_criterion$min_date+BOL_criterion$add
    * */

    BOLCriterion = BOLCriterion.drop("min_date","fist_date","diff_weeks","add")
        .withColumn("BOL_criterion", lit(1))

    commercialWithCompCannDF = commercialWithCompCannDF.join(BOLCriterion, Seq("SKU","Reseller_Cluster","Week_End_Date"), "left")
        .withColumn("BOL_criterion", when(col("BOL_criterion").isNull, 0).otherwise(col("BOL_criterion")))
        .withColumn("BOL", when(col("Week_End_Date")-col("GA_date")<(7*6),1).otherwise(col("BOL")))   //TODO: Check subtraction possible
        .withColumn("BOL", when(col("BOL").isNull, 0).otherwise(col("BOL")))

    val commercialEOLSpikeFilter = commercialWithCompCannDF.where((col("EOL")===0) && (col("spike")===0))
    val opposite = commercialEOLSpikeFilter
      .groupBy("SKU_Name","Reseller_Cluster")
      .count().as("n") //sum(when(col("IR")>0,"Qty"))
      .agg(mean("Qty").as("Qty_total"), mean(when(col("Promo_Flag")===1,"Qty")).as("Qty_promo"), mean(when(col("Promo_Flag")===0, "Qty")).as("Qty_no_promo"))
      .join(commercialEOLSpikeFilter, Seq("SKU_Name","Reseller_Cluster"), "right")
      .withColumn("opposite", when((col("Qty_no_promo")>col("Qty_promo")) || (col("Qty_no_promo")<0), 1).otherwise(0))
      .withColumn("opposite", when(col("opposite").isNull, 0).otherwise(col("opposite")))
      .withColumn("no_promo_sales", when(col("Qty_promo").isNull, 1).otherwise(0))

    commercialWithCompCannDF = commercialWithCompCannDF.join(opposite.select("SKU_Name", "Reseller_Cluster", "opposite","no_promo_sales"), Seq("SKU_Name","Reseller_Cluster"), "left")
        .withColumn("NP_Flag", col("Promo_Flag"))
        .withColumn("NP_IR", col("IR"))
        .withColumn("high_disc_Flag", when(col("Promo_Pct")<=0.55, 0).otherwise(1))
    //TODO: CHECK - ave(commercial$Promo.Flag, commercial$Reseller.Cluster, commercial$SKU.Name, commercial$Season, FUN=mean
    val commercialPromoMean = commercialWithCompCannDF
      .groupBy("Reseller_Cluster","SKU_Name","Season")
      .agg(avg(mean(col("Promo_Flag"))).as("PromoFlagAvg"))

    commercialWithCompCannDF = commercialWithCompCannDF.join(commercialPromoMean, Seq("Reseller_Cluster","SKU_Name","Season"), "left")
        .withColumn("always_promo_Flag", when(col("PromoFlagAvg")===1, 1).otherwise(0)).drop("PromoFlagAvg")
        .withColumn("EOL", when(col("Reseller_Cluster")==="CDW",
          when(col("SKU_Name")==="LJ Pro M402dn", 0).otherwise(col("EOL"))).otherwise(col("EOL")))
        .withColumn("cann_group", lit(null))
        .withColumn("cann_group", when(col("SKU_Name").contains("M20") || col("SKU_Name").contains("M40"),"M201_M203/M402").otherwise(col("cann_group")))
        .withColumn("cann_group", when(col("SKU_Name").contains("M22") || col("SKU_Name").contains("M42"),"M225_M227/M426").otherwise(col("cann_group")))
        .withColumn("cann_group", when(col("SKU_Name").contains("M25") || col("SKU_Name").contains("M45"),"M252_M254/M452").otherwise(col("cann_group")))
        .withColumn("cann_group", when(col("SKU_Name").contains("M27") || col("SKU_Name").contains("M28"),"M277_M281/M477").otherwise(col("cann_group")))
        .withColumn("cann_group", when(col("SKU_Name").contains("8710") || col("SKU_Name").contains("8720") || col("SKU_Name").contains("6968") || col("SKU_Name").contains("6978"),"Weber/Muscatel").otherwise(col("cann_group")))
        .withColumn("cann_group", when(col("SKU_Name").contains("6255") || col("SKU_Name").contains("7155") || col("SKU_Name").contains("7855") || col("SKU_Name").contains("6988") || col("SKU_Name").contains("6978"),"Palermo/Muscatel").otherwise(col("cann_group")))
        .withColumn("cann_receiver", lit(null))
        .withColumn("cann_receiver", when(col("SKU_Name").contains("M40"), "M402").otherwise(col("cann_receiver")))
        .withColumn("cann_receiver", when(col("SKU_Name").contains("M42"), "M426").otherwise(col("cann_receiver")))
        .withColumn("cann_receiver", when(col("SKU_Name").contains("M45"), "M452").otherwise(col("cann_receiver")))
        .withColumn("cann_receiver", when(col("SKU_Name").contains("M47"), "M477").otherwise(col("cann_receiver")))
        .withColumn("cann_receiver", when(col("SKU_Name").contains("8710") || col("SKU_Name").contains("8720"), "Weber").otherwise(col("cann_receiver")))
        .withColumn("cann_receiver", when(col("SKU_Name").contains("6968") || col("SKU_Name").contains("6978"), "Muscatel").otherwise(col("cann_receiver")))
        .withColumn("cann_receiver", when(col("SKU_Name").contains("6255") || col("SKU_Name").contains("7155") || col("SKU_Name").contains("7855"), "Palermo").otherwise(col("cann_receiver")))

        //TODO: Confirm where these columns originate from in R code
    commercialWithCompCannDF = commercialWithCompCannDF
        .withColumn("Direct_Cann_201", lit(null)).withColumn("Direct_Cann_225", lit(null)).withColumn("Direct_Cann_252", lit(null)).withColumn("Direct_Cann_277", lit(null))
        .withColumn("Direct_Cann_Weber", lit(null)).withColumn("Direct_Cann_Muscatel_Weber", lit(null)).withColumn("Direct_Cann_Muscatel_Palermo", lit(null)).withColumn("Direct_Cann_Palermo", lit(null))
        .withColumn("Direct_Cann_201", when(col("SKU_Name").contains("M201") || col("SKU_Name").contains("M203"), commercialWithCompCannDF.where(col("SKU_Name").contains("M40")).agg(mean("IR")).head().getFloat(0)).otherwise(0))
        .withColumn("Direct_Cann_225", when(col("SKU_Name").contains("M225") || col("SKU_Name").contains("M227"), commercialWithCompCannDF.where(col("SKU_Name").contains("M42")).agg(mean("IR")).head().getFloat(0)).otherwise(0))
        .withColumn("Direct_Cann_252", when(col("SKU_Name").contains("M252") || col("SKU_Name").contains("M254"), commercialWithCompCannDF.where(col("SKU_Name").contains("M45")).agg(mean("IR")).head().getFloat(0)).otherwise(0))
        .withColumn("Direct_Cann_277", when(col("SKU_Name").contains("M277") || col("SKU_Name").contains("M281"), commercialWithCompCannDF.where(col("SKU_Name").contains("M47")).agg(mean("IR")).head().getFloat(0)).otherwise(0))
        .withColumn("Direct_Cann_Weber", when(col("cann_receiver")==="Weber", commercialWithCompCannDF.where(col("cann_receiver")==="Muscatel").agg(mean("IR")).head().getFloat(0)).otherwise(0))
        .withColumn("Direct_Cann_Muscatel_Weber", when(col("cann_receiver")==="Muscatel", commercialWithCompCannDF.where(col("cann_receiver")==="Weber").agg(mean("IR")).head().getFloat(0)).otherwise(0))
        .withColumn("Direct_Cann_Muscatel_Palermo", when(col("cann_receiver")==="Muscatel", commercialWithCompCannDF.where(col("cann_receiver")==="Palermo").agg(mean("IR")).head().getFloat(0)).otherwise(0))
        .withColumn("Direct_Cann_Palermo", when(col("cann_receiver")==="Palermo", commercialWithCompCannDF.where(col("cann_receiver")==="Muscatel").agg(mean("IR")).head().getFloat(0)).otherwise(0))
        .withColumn("Direct_Cann_201",when(col("Direct_Cann_201").isNull, 0).otherwise(col("Direct_Cann_201")))
        .withColumn("Direct_Cann_225",when(col("Direct_Cann_225").isNull, 0).otherwise(col("Direct_Cann_225")))
        .withColumn("Direct_Cann_252",when(col("Direct_Cann_252").isNull, 0).otherwise(col("Direct_Cann_252")))
        .withColumn("Direct_Cann_277",when(col("Direct_Cann_277").isNull, 0).otherwise(col("Direct_Cann_277")))
        .withColumn("Direct_Cann_Weber",when(col("Direct_Cann_Weber").isNull, 0).otherwise(col("Direct_Cann_Weber")))
        .withColumn("Direct_Cann_Muscatel_Weber",when(col("Direct_Cann_Muscatel_Weber").isNull, 0).otherwise(col("Direct_Cann_Muscatel_Weber")))
        .withColumn("Direct_Cann_Muscatel_Palermo", when(col("Direct_Cann_Muscatel_Palermo").isNull, 0).otherwise(col("Direct_Cann_Muscatel_Palermo")))
        .withColumn("Direct_Cann_Palermo", when(col("Direct_Cann_Palermo").isNull, 0).otherwise(col("Direct_Cann_Palermo")))
        .withColumn("Hardware_GM", when(col("Category_1")==="Value" && col("Week_End_Date")>="2016-07-01", col("Hardware_GM")+lit(68)).otherwise(col("Hardware_GM")))
        .withColumn("Hardware_GM", when(col("Category_1")==="Value" && col("Week_End_Date")>="2017-05-01", col("Hardware_GM")+lit(8)).otherwise(col("Hardware_GM")))
        .withColumn("Hardware_GM", when(col("Category_Custom")==="A4 SMB" && col("Week_End_Date")>="2017-11-01", col("Hardware_GM")-lit(7.51)).otherwise(col("Hardware_GM")))
        .withColumn("Hardware_GM", when(col("Category_Custom").isin("A4 Value","A3 Value") && col("Week_End_Date")>="2017-11-01", col("Hardware_GM")+lit(33.28)).otherwise(col("Hardware_GM")))
        .withColumn("Supplies_GM", when(col("L1_Category")==="Scanners",0).otherwise(col("Supplies_GM")))

    val excludeCondition1 = when(col("Reseller_Cluster").isin("Other - Option C", "eTailerOther - Option C", "Stockpiler & Deal Chaser", "eTailerStockpiler & Deal Chaser")
      || col("low_volume")===1 || col("low_baseline")===1 || col("spike")===1 || col("opposite")===1 || col("EOL")===1 || col("BOL")===1 || col("no_promo_sales")===1,1).otherwise(0)

    val excludeCondition2 = when(col("Reseller_Cluster").isin("Other - Option C", "eTailerOther - Option C", "Stockpiler & Deal Chaser", "eTailerStockpiler & Deal Chaser")
      || col("low_volume")===1 || /*col("low_baseline")===1 ||*/ col("spike")===1 || col("opposite")===1 || col("EOL")===1 || col("BOL")===1 || col("no_promo_sales")===1,1).otherwise(0)

    commercialWithCompCannDF = commercialWithCompCannDF
        .withColumn("exclude", when(!col("PL").isin("3Y"),excludeCondition1).otherwise(0))
        .withColumn("exclude", when(!col("PL").isin("3Y"),excludeCondition2).otherwise(0))
        .withColumn("exclude", when(col("SKU_Name").contains("Sprocket"), 1).otherwise(col("exclude")))
        .withColumn("AE_NP_IR", col("NP_IR"))
        .withColumn("AE_ASP_IR", lit(0))
        .withColumn("AE_Other_IR", lit(0))
        .withColumn("Street_PriceWhoChange_log", when(col("Changed_Street_Price")===0, 0).otherwise(log(col("Street_Price")*col("Changed_Street_Price"))))
        .withColumn("SKUWhoChange", when(col("Changed_Street_Price")===0, 0).otherwise(col("SKU")))
        .withColumn("PriceChange_HPS_OPS", when(col("Changed_Street_Price")===0, 0).otherwise(col("HPS_OPS")))


    /*TODO:
    * if (length(unique(commercial$Week.End.Date[commercial$Season==unique(commercial$Season[order(commercial$Week.End.Date)])
      [length(unique(commercial$Season))]]))<13){
        commercial$Season_most_recent <- ifelse(commercial$Season==unique(commercial$Season
        [order(commercial$Week.End.Date)])[length(unique(commercial$Season))], as.character(unique(commercial$Season[order
        (commercial$Week.End.Date)])[length(unique(commercial$Season))-1]), as.character(commercial$Season))
      } else {
        commercial$Season_most_recent <- commercial$Season
      }
    * */
    if (/*Above condition*/ true) {
      commercialWithCompCannDF = commercialWithCompCannDF
      //.withColumn("Season_most_recent", )
      //TODO:ifelse(commercial$Season eq unique(commercial$Season(order(commercial$Week.End.Date)))(length(unique(commercial$Season))), as.character(unique(commercial$Season(order(commercial$Week.End.Date)))(length(unique(commercial$Season)) - 1)), as.character(commercial$Season))
    }else {
      commercialWithCompCannDF = commercialWithCompCannDF
          .withColumn("Season_most_recent", col("Season"))
    }

    List("cann_group","SKUWhoChange").foreach(x => {
      val indexer = new StringIndexer().setInputCol(x).setOutputCol(x+"_fact")
      val pipeline = new Pipeline().setStages(Array(indexer))
      commercialWithCompCannDF = pipeline.fit(commercialWithCompCannDF).transform(commercialWithCompCannDF)
        .drop(x).withColumnRenamed(x+"_fact",x).drop(x+"_fact")

    })

    commercialWithCompCannDF.show(100)
  }
}
