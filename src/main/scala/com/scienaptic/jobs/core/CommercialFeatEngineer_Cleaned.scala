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

import org.apache.spark.sql.functions.rank
import com.scienaptic.jobs.utility.Utils.renameColumns
import com.scienaptic.jobs.utility.CommercialUtility.{addDaystoDateStringUDF, checkPrevQtsGTBaseline, concatenateRank, createlist, extractWeekFromDateUDF}
import org.apache.spark.sql.expressions.Window
import com.scienaptic.jobs.utility.CommercialUtility._

object CommercialFeatEnggProcessorCleaned {
  val Cat_switch=1
  val min_baseline = 2
  val stability_weeks = 4
  val intro_weeks = 6

  val dat2000_01_01 = to_date(unix_timestamp(lit("2000-01-01"),"yyyy-MM-dd").cast("timestamp"))
  val dat9999_12_31 = to_date(unix_timestamp(lit("9999-12-31"),"yyyy-MM-dd").cast("timestamp"))

  val indexerForSpecialPrograms = new StringIndexer().setInputCol("Special_Programs").setOutputCol("Special_Programs_fact").setHandleInvalid("keep")
  val pipelineForSpecialPrograms = new Pipeline().setStages(Array(indexerForSpecialPrograms))

  val indexerForResellerCluster = new StringIndexer().setInputCol("Reseller_Cluster").setOutputCol("Reseller_Cluster_fact").setHandleInvalid("keep")
  val pipelineForResellerCluster= new Pipeline().setStages(Array(indexerForResellerCluster))

  def execute(executionContext: ExecutionContext): Unit = {

    val sparkConf = new SparkConf().setAppName("Test")
    val spark = SparkSession.builder
      .master("local[*]")
      //.master("yarn-client")
      .appName("Commercial-R")
      .config(sparkConf)
      .getOrCreate

    val baselineThreshold = if (min_baseline/2 > 0) min_baseline/2 else 0

    import spark.implicits._

    //val commercialDF = spark.read.option("header","true").option("inferSchema","true").csv("C:\\Users\\avika\\Downloads\\JarCode\\R Code Inputs\\posqty_output_commercial.csv")
    val commercialDF = spark.read.option("header","true").option("inferSchema","true").csv("/etherData/Pricing/Output/POS_Commercial/posqty_output_commercial.csv")

    var commercial = renameColumns(commercialDF).cache()
    commercial.columns.toList.foreach(x => {
      commercial = commercial.withColumn(x, when(col(x).cast("string") === "NA" || col(x).cast("string") === "", null).otherwise(col(x)))
    })
    commercial = commercial
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"),"YYYY-MM-dd").cast("timestamp")))
      .where(col("Week_End_Date") >= lit("2014-01-01"))
      .where(col("Week_End_Date") <= lit("2018-12-29"))
      .withColumn("ES date",to_date(unix_timestamp(col("ES date"),"YYYY-MM-dd").cast("timestamp")))
      .withColumn("GA date",to_date(unix_timestamp(col("GA date"),"YYYY-MM-dd").cast("timestamp")))

    //val ifs2DF = spark.read.option("header","true").option("inferSchema","true").csv("C:\\Users\\avika\\Downloads\\JarCode\\R Code Inputs\\IFS2_most_recent.csv")
    val ifs2DF = spark.read.option("header","true").option("inferSchema","true").csv("/etherData/ManagedSources/IFS2/IFS2_most_recent.csv")
    var ifs2 = renameColumns(ifs2DF)
    ifs2.columns.toList.foreach(x => {
      ifs2 = ifs2.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    ifs2 = ifs2
      .withColumn("Valid_Start_Date", to_date(unix_timestamp(col("Valid_Start_Date"),"MM/dd/yyyy").cast("timestamp")))
      .withColumn("Valid_End_Date", to_date(unix_timestamp(col("Valid_End_Date"),"MM/dd/yyyy").cast("timestamp"))).cache()
    //writeDF(ifs2,"ifs2")
    //writeDF(commercial,"commercial")

    val commercialMergeifs2DF = commercial.withColumnRenamed("Reseller Cluster","Reseller_Cluster").withColumnRenamed("Street Price","Street_Price_Org")
      .join(ifs2.dropDuplicates("SKU","Street_Price").select("SKU","Changed_Street_Price","Street_Price", "Valid_Start_Date", "Valid_End_Date"), Seq("SKU"), "left")
      .withColumn("Valid_Start_Date", when(col("Valid_Start_Date").isNull,dat2000_01_01).otherwise(col("Valid_Start_Date")))
      .withColumn("Valid_End_Date", when(col("Valid_End_Date").isNull,dat9999_12_31).otherwise(col("Valid_End_Date")))
      .withColumn("Street_Price", when(col("Street_Price").isNull, col("Street_Price_Org")).otherwise(col("Street_Price")))
      .where((col("Week_End_Date") >= col("Valid_Start_Date")) && (col("Week_End_Date") < col("Valid_End_Date")))
      .drop("Street_Price_Org","Changed_Street_Price","Valid_Start_Date","Valid_End_Date")
    //writeDF(commercialMergeifs2DF,"commercialMergeifs2DF")

    
    var commercialWithQtyFilterResellerAndSpProgramsFacDF = commercialMergeifs2DF.withColumn("Qty",col("Non_Big_Deal_Qty"))
      .withColumn("Special_Programs",lit("None"))
      .withColumn("Special_Programs_LEVELS", col("Special_Programs"))
    val indexer = new StringIndexer().setInputCol("Special_Programs").setOutputCol("Special_Programs_fact").setHandleInvalid("keep")
    val pipeline = new Pipeline().setStages(Array(indexer))
    commercialWithQtyFilterResellerAndSpProgramsFacDF = pipeline.fit(commercialWithQtyFilterResellerAndSpProgramsFacDF).transform(commercialWithQtyFilterResellerAndSpProgramsFacDF)
      .drop("Special_Programs").withColumnRenamed("Special_Programs_fact","Special_Programs").drop("Special_Programs_fact")


    val commercialSpclPrgmFactDF = pipelineForSpecialPrograms.fit(commercialWithQtyFilterResellerAndSpProgramsFacDF).transform(commercialWithQtyFilterResellerAndSpProgramsFacDF)
      .drop("Special_Programs").withColumnRenamed("Special_Programs_fact","Special_Programs").drop("Special_Programs_fact")
      .withColumn("Special_Programs", (col("Special_Programs")+lit(1)).cast("int"))
    var stockpiler_dealchaser = commercialSpclPrgmFactDF
      .groupBy("Reseller_Cluster")
      .agg(sum("Qty").alias("Qty_total"))
    //writeDF(stockpiler_dealchaser,"stockpiler_dealchaser")
    val stockpiler_dealchaserPromo = commercialSpclPrgmFactDF.where(col("IR")>0)
      .groupBy("Reseller_Cluster")
      .agg(sum("Qty").as("Qty_promo"))
    stockpiler_dealchaser = stockpiler_dealchaser.join(stockpiler_dealchaserPromo, Seq("Reseller_Cluster"), "left")
      .withColumn("Qty_promo", when(col("Qty_promo").isNull, 0).otherwise(col("Qty_promo")))
    //writeDF(stockpiler_dealchaserPromo,"stockpiler_dealchaserPromo")
    
    stockpiler_dealchaser = stockpiler_dealchaser
      .withColumn("proportion_on_promo", col("Qty_promo")/col("Qty_total"))
      .withColumn("proportion_on_promo", when(col("proportion_on_promo").isNull, 0).otherwise(col("proportion_on_promo")))
      .where(col("proportion_on_promo")>0.95)
    //writeDF(commercialSpclPrgmFactDF,"commercialSpclPrgmFactDF")
    //writeDF(stockpiler_dealchaser,"stockpiler_dealchaser")

    val stockpilerResellerList = stockpiler_dealchaser.select("Reseller_Cluster").distinct().collect().map(_ (0).asInstanceOf[String]).toSet
    val commercialMatResellerStockPilerDF = commercialSpclPrgmFactDF
      .withColumn("Reseller_Cluster", when(col("Reseller_Cluster").isin(stockpilerResellerList.toSeq: _*), lit("Stockpiler & Deal Chaser")).otherwise(col("Reseller_Cluster")))
      .withColumn("Reseller_Cluster", when(col("eTailer")===lit(1), concat(lit("eTailer"),col("Reseller_Cluster"))).otherwise(col("Reseller_Cluster")))
      .withColumn("Reseller_Cluster_LEVELS", col("Reseller_Cluster"))

    val commercialResellerClusterFactDF = pipelineForResellerCluster.fit(commercialMatResellerStockPilerDF).transform(commercialMatResellerStockPilerDF)
      .drop("Reseller_Cluster").withColumnRenamed("Reseller_Cluster_fact","Reseller_Cluster").drop("Reseller_Cluster_fact")
      .withColumn("Reseller_Cluster", (col("Reseller_Cluster")+lit(1)).cast("int"))   //String indexer starts from 0
      .drop("eTailer")
    //writeDF(commercialResellerClusterFactDF,"commercialResellerClusterFactDF_BEFORE_HP_AND_SS")

    val commercialResFacNotPL = commercialResellerClusterFactDF.where(!col("PL").isin("E4","E0","ED"))
      .withColumn("Brand",lit("HP"))
        .withColumn("GA date",to_date(col("GA date")))
        .withColumn("ES date",to_date(col("ES date")))
    ////writeDF(commercialResFacNotPL,"commercialHPBeforeGroup")
    var commercialHPDF = commercialResFacNotPL
      .groupBy("SKU","SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS","Week_End_Date","Season","Street_Price","IPSLES","HPS/OPS","Series","Category","Category Subgroup","Category_1","Category_2","Category_3","Category Custom","Line","PL","L1_Category","L2_Category","PLC Status","GA date","ES date","Inv_Qty","Special_Programs")
      .agg(sum("Qty").as("Qty"), max("IR").as("IR"), sum("Big_Deal_Qty").as("Big_Deal_Qty"), sum("Non_Big_Deal_Qty").as("Non_Big_Deal_Qty"))
      .withColumnRenamed("HPS/OPS","HPS_OPS")
    //writeDF(commercialHPDF,"commercialHPAfterGroup")
    
    val commercialResFacPL = commercialResellerClusterFactDF.where(col("PL").isin("E4","E0","ED"))
    //writeDF(commercialResFacPL,"commercialSSBeforeGroup")
    val commercialSSDF = commercialResFacPL
      .groupBy("SKU","SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS","Week_End_Date","Season","Special_Programs")
      .agg(sum("Qty").as("Qty"), sum("Inv_Qty").as("Inv_Qty"), max("IR").as("IR"), sum("Big_Deal_Qty").as("Big_Deal_Qty"), sum("Non_Big_Deal_Qty").as("Non_Big_Deal_Qty"))
    //writeDF(commercialSSDF,"commercialSSAfterGroup")*/

    val auxTableDF = spark.read.option("header","true").option("inferSchema","true").csv("C:\\Users\\avika\\Downloads\\JarCode\\R Code Inputs\\AUX_sku_hierarchy.csv")
    //val auxTableDF = spark.read.option("header","true").option("inferSchema","true").csv("/etherData/ManagedSources/AUX/AUX_sku_hierarchy.csv")
    var auxTable = renameColumns(auxTableDF).cache()
    auxTable.columns.toList.foreach(x => {
      auxTable = auxTable.withColumn(x, when(col(x).cast("string") === "NA" || col(x).cast("string") === "", null).otherwise(col(x)))
    })

    val commercialSSJoinSKUDF = commercialSSDF.join(auxTable, Seq("SKU"), "left")
      .withColumnRenamed("HPS/OPS","HPS_OPS")
      .withColumn("L1_Category",col("L1: Use Case"))
      .withColumn("L2_Category",col("L2: Key functionality"))
      .drop("L1: Use Case","L2: Key functionality","Abbreviated_Name","Platform_Name","Mono/Color","Added","L3: IDC Category","L0: Format","Need_Big_Data","Need_IFS2?","Top_SKU","NPD_Model","NPD_Product_Company","Sales_Product_Name")

    var commercialSSFactorsDF = commercialSSJoinSKUDF
        .drop("L3: IDC Category","L0: Format","Need Big Data","Need IFS2?","Top SKU","NPD Model","NPD_Product_Company","Sales_Product_Name")
    val commercialSSFactorColumnsList = List("IPSLES","HPS_OPS","Series","Category","Category Subgroup","Category_1","Category_2","Category_3","Line","PL","Category Custom","L1_Category","L2_Category","PLC Status","GA date","ES date")
    commercialSSFactorColumnsList.foreach(column => {
      val indexer = new StringIndexer().setInputCol(column).setOutputCol(column+"_fact").setHandleInvalid("keep")
      val pipeline = new Pipeline().setStages(Array(indexer))
      commercialSSFactorsDF = commercialSSFactorsDF.withColumn(column+"_LEVELS", col(column))
      commercialSSFactorsDF = pipeline.fit(commercialSSFactorsDF).transform(commercialSSFactorsDF)
        .drop(column).withColumnRenamed(column+"_fact",column).drop(column+"_fact")
    })
    commercialHPDF = commercialHPDF.withColumn("Brand",lit("HP")).withColumn("GA date",col("GA date").cast("string")).withColumn("ES date",col("ES date").cast("string"))
    List("IPSLES","HPS_OPS","Series","Category","Category Subgroup","Category_1","Category_2","Category_3","Line","PL","Category Custom","L1_Category","L2_Category","PLC Status","GA date","ES date").foreach(column => {
      val indexer = new StringIndexer().setInputCol(column).setOutputCol(column+"_fact").setHandleInvalid("keep")
      val pipeline = new Pipeline().setStages(Array(indexer))
      commercialHPDF = commercialHPDF.withColumn(column+"_LEVELS", col(column))   //TODO: Change this to col(column)
      commercialHPDF = pipeline.fit(commercialHPDF).transform(commercialHPDF)
        .drop(column).withColumnRenamed(column+"_fact",column).drop(column+"_fact")
    })
    //writeDF(commercialHPDF,"commercialHPDF_BEFORE_UNION")
    //writeDF(commercialSSFactorsDF,"commercialSSFactorsDF_BEFORE_UNION")*/

    //val commercialHPandSSDF = /*commercialHPDF.withColumnRenamed("Street_Price","Street Price")*/doUnion(commercialSSFactorsDF, commercialHPDF.withColumnRenamed("Street_Price","Street Price")).ge
    var commercialHPandSSDF = doUnion(commercialSSFactorsDF, commercialHPDF.withColumnRenamed("Street_Price","Street Price")).get
      .withColumn("Street Price", (regexp_replace(col("Street Price"),lit("\\$"),lit(""))))
      .withColumn("Street Price", (regexp_replace(col("Street Price"),lit(","),lit(""))).cast("double"))
      .where(col("Street Price") =!= lit(0))
    //writeDF(commercialHPandSSDF,"commercialHPandSSDF_FILTERED_STREET_PRICE")
    commercialHPandSSDF = commercialHPandSSDF
      .withColumn("VPA", when(col("Reseller_Cluster_LEVELS").isin("CDW","PC Connection","Zones Inc","Insight Direct","PCM","GovConnection"), 1).otherwise(0))
      .withColumn("Promo_Flag", when(col("IR")>0,1).otherwise(0))
      .withColumn("Promo_Pct", when(col("Street Price").isNull,null).otherwise(col("IR")/col("Street Price").cast("double")))
      .withColumn("Discount_Depth_Category", when(col("Promo_Pct")===0, "No Discount").when(col("Promo_Pct")<=0.2, "Very Low").when(col("Promo_Pct")<=0.3, "Low").when(col("Promo_Pct")===0.4, "Moderate").when(col("Promo_Pct")<="0.5", "Heavy").otherwise(lit("Very Heavy")))
      .withColumn("log_Qty", log(col("Qty")))
      .withColumn("log_Qty", when(col("log_Qty").isNull, 0).otherwise(col("log_Qty")))
      .withColumn("price", lit(1)-col("Promo_Pct"))
      .withColumn("price", when(col("price").isNull, 0).otherwise(col("price")))
      .withColumn("Inv_Qty_log", log(col("Inv_Qty")))
      .withColumn("Inv_Qty_log", when(col("Inv_Qty_log").isNull, 0).otherwise(col("Inv_Qty_log")))
      .na.fill(0, Seq("log_Qty","price","Inv_Qty_log"))
    //writeDF(commercialHPandSSDF,"commercialHPandSSDF")
    //writeDF(doUnion(commercialSSFactorsDF, commercialHPDF.withColumnRenamed("Street_Price","Street Price")).get,"Commercial_HP_SS_Bind_Rows")

    val christmasDF = Seq(("2014-12-27",1),("2015-12-26",1),("2016-12-31",1),("2017-12-30",1),("2018-12-29",1),("2019-12-28",1)).toDF("Week_End_Date","USChristmasDay")
    val columbusDF = Seq(("2014-10-18",1),("2015-10-17",1),("2016-10-15",1),("2017-10-14",1),("2018-10-13",1),("2019-10-19",1)).toDF("Week_End_Date","USColumbusDay")
    val independenceDF = Seq(("2014-07-05",1),("2015-07-04",1),("2016-07-09",1),("2017-07-08",1),("2018-07-07",1),("2019-07-06",1)).toDF("Week_End_Date","USIndependenceDay")
    val laborDF = Seq(("2014-09-06",1),("2015-09-12",1),("2016-09-10",1),("2017-09-09",1),("2018-09-08",1),("2019-09-07",1)).toDF("Week_End_Date","USLaborDay")
    val linconsBdyDF = Seq(("2014-02-15",1),("2015-02-14",1),("2016-02-13",1),("2017-02-18",1),("2018-02-17",1),("2019-02-16",1)).toDF("Week_End_Date","USLincolnsBirthday")
    val memorialDF = Seq(("2014-05-31",1),("2015-05-30",1),("2016-06-04",1),("2017-06-03",1),("2018-06-02",1),("2019-06-01",1)).toDF("Week_End_Date","USMemorialDay")
    val MLKingsDF = Seq(("2014-01-25",1),("2015-01-24",1),("2016-01-23",1),("2017-01-21",1),("2018-01-20",1),("2019-01-26",1)).toDF("Week_End_Date","USMLKingsBirthday")
    val newYearDF = Seq(("2014-01-04",1),("2015-01-03",1),("2016-01-02",1),("2017-01-07",1),("2018-01-06",1),("2019-01-05",1)).toDF("Week_End_Date","USNewYearsDay")
    val presidentsDayDF = Seq(("2014-02-22",1),("2015-02-21",1),("2016-02-20",1),("2017-02-25",1),("2018-02-24",1),("2019-02-23",1)).toDF("Week_End_Date","USPresidentsDay")
    val veteransDayDF = Seq(("2014-11-15",1),("2015-11-14",1),("2016-11-12",1),("2017-11-11",1),("2018-11-17",1),("2019-11-16",1)).toDF("Week_End_Date","USVeteransDay")
    val washingtonBdyDF = Seq(("2014-02-22",1),("2015-02-28",1),("2016-02-27",1),("2017-02-25",1),("2018-02-24",1),("2019-02-23",1)).toDF("Week_End_Date","USWashingtonsBirthday")
    val thanksgngDF = Seq(("2014-11-29",1),("2015-11-28",1),("2016-11-26",1),("2017-11-25",1),("2018-11-24",1),("2019-11-30",1)).toDF("Week_End_Date","USThanksgivingDay")

    val usCyberMonday = thanksgngDF.withColumn("Week_End_Date", date_add(col("Week_End_Date").cast("timestamp"), 7)).withColumnRenamed("USThanksgivingDay","USCyberMonday")
    var commercialwithHolidaysDF = commercialHPandSSDF
      .join(christmasDF, Seq("Week_End_Date"), "left")
      .join(columbusDF, Seq("Week_End_Date"), "left")
      .join(independenceDF, Seq("Week_End_Date"), "left")
      .join(laborDF, Seq("Week_End_Date"), "left")
      .join(linconsBdyDF, Seq("Week_End_Date"), "left")
      .join(memorialDF, Seq("Week_End_Date"), "left")
      .join(MLKingsDF, Seq("Week_End_Date"), "left")
      .join(newYearDF, Seq("Week_End_Date"), "left")
      .join(presidentsDayDF, Seq("Week_End_Date"), "left")
      .join(veteransDayDF, Seq("Week_End_Date"), "left")
      .join(washingtonBdyDF, Seq("Week_End_Date"), "left")
      .join(thanksgngDF, Seq("Week_End_Date"), "left")
      .join(usCyberMonday, Seq("Week_End_Date"), "left")
      List("USChristmasDay","USColumbusDay","USIndependenceDay","USLaborDay","USLincolnsBirthday","USMemorialDay","USMLKingsBirthday","USNewYearsDay","USPresidentsDay","USThanksgivingDay","USVeteransDay","USWashingtonsBirthday","USCyberMonday")
      .foreach(x=> {
           commercialwithHolidaysDF = commercialwithHolidaysDF.withColumn(x, when(col(x).isNull, 0).otherwise(col(x)))
      })
    //writeDF(commercialwithHolidaysDF,"Commercial_With_Holidays")*/

    var npdDF = spark.read.option("header","true").option("inferSchema","true").csv("C:\\Users\\avika\\Downloads\\JarCode\\R Code Inputs\\NPD_weekly.csv")
    //var npdDF = spark.read.option("header","true").option("inferSchema","true").csv("/etherData/ManagedSources/NPD/NPD_weekly.csv")
    var npd = renameColumns(npdDF)
    npd.columns.toList.foreach(x => {
      npd = npd.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    npd = npd.withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"),"MM/dd/yyyy").cast("timestamp"))).cache()
    //writeDF(npd,"npd")

    /*================= Brand not Main Brands =======================*/
    val npdChannelBrandFilterNotRetail = npd.where((col("Channel") =!= "Retail") && (col("Brand").isin("Canon","Epson","Brother","Lexmark","Samsung")))
      .where((col("DOLLARS")>0) && (col("MSRP__")>0))
    //writeDF(npdChannelBrandFilterNotRetail,"npdChannel_Brand_FilterNotRetail")
    val L1Competition =  npdChannelBrandFilterNotRetail
      .groupBy("L1_Category","Week_End_Date","Brand")
      .agg((sum("DOLLARS")/sum("MSRP__")).as("dolMSRPRatio"))
      .withColumn("L1_competition", lit(1)-col("dolMSRPRatio")).drop("dolMSRPRatio")

    //val generateUUID = udf(() => UUID.randomUUID().toString)
    var L1Comp = L1Competition//.withColumn("uuid",generateUUID())
      .groupBy("L1_Category","Week_End_Date"/*,"uuid"*/)
      .pivot("Brand").agg(first("L1_competition")).drop("uuid")
    ////writeDF(L1Comp,"L1Comp")
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
    //writeDF(L1Comp, "L1Comp")

    val L2Competition = npdChannelBrandFilterNotRetail
      .groupBy("L2_Category","Week_End_Date","Brand")
      .agg((sum("DOLLARS")/sum("MSRP__")).as("dolMSRPRatio"))
      .withColumn("L2_competition", lit(1)-col("dolMSRPRatio")).drop("dolMSRPRatio")
    //writeDF(L2Competition,"L2Competition")
    var L2Comp = L2Competition//.withColumn("uuid",generateUUID())
      .groupBy("L2_Category","Week_End_Date"/*,"uuid"*/)
      .pivot("Brand")
      .agg(first("L2_competition"))//.drop("uuid")
    //writeDF(L2Comp,"L2Comp_BEFORE_NULL_IMPUTAITON")
    val L2CompColumns = L2Comp.columns
    allBrands.foreach(x => {
      if (!L2CompColumns.contains(x))
        L2Comp = L2Comp.withColumn(x, lit(null))
    })
    allBrands.foreach(x => {
      L2Comp = L2Comp.withColumn(x, when(col(x).isNull, 0).otherwise(col(x)))
        .withColumnRenamed(x, "L2_competition_"+x)
    })
    //writeDF(L2Comp,"L2Comp")
    commercialwithHolidaysDF = commercialwithHolidaysDF
      .withColumnRenamed("L1_Category","L1_Category_fact")
      .withColumnRenamed("L1_Category_LEVELS","L1_Category")
      .withColumnRenamed("L2_Category","L2_Category_fact")
      .withColumnRenamed("L2_Category_LEVELS","L2_Category")

    var commercialWithCompetitionDF = commercialwithHolidaysDF.join(L1Comp, Seq("L1_Category","Week_End_Date"), "left")
    commercialWithCompetitionDF = commercialWithCompetitionDF
      .join(L2Comp, Seq("L2_Category","Week_End_Date"), "left")
    allBrands.foreach(x => {
      val l1Name = "L1_competition_"+x
      val l2Name = "L2_competition_"+x
      commercialWithCompetitionDF = commercialWithCompetitionDF.withColumn(l1Name, when((col(l1Name).isNull) || (col(l1Name)<0), 0).otherwise(col(l1Name)))
          .withColumn(l2Name, when(col(l2Name).isNull || col(l2Name)<0, 0).otherwise(col(l2Name)))
    })
    //writeDF(commercialWithCompetitionDF,"commercialWithCompetitionDF_AFTER_L1_L2_JOIN")
    commercialWithCompetitionDF= commercialWithCompetitionDF.na.fill(0, Seq("L1_competition_Brother","L1_competition_Canon","L1_competition_Epson","L1_competition_Lexmark","L1_competition_Samsung"))
        .na.fill(0, Seq("L2_competition_Brother","L2_competition_Epson","L2_competition_Canon","L2_competition_Lexmark","L2_competition_Samsung"))
    //writeDF(commercialWithCompetitionDF,"commercialWithCompetitionDF_WITH_L1_L2")
    /*====================================== Brand Not HP =================================*/
    val npdChannelNotRetailBrandNotHP = npd.where((col("Channel")=!="Retail") && (col("Brand")=!="HP"))
      .where((col("DOLLARS")>0) && (col("MSRP__")>0))
    //writeDF(npdChannelNotRetailBrandNotHP,"npdChannelNotRetailBrandNotHP")
    var L1CompetitionNonHP = npdChannelNotRetailBrandNotHP
      .groupBy("L1_Category","Week_End_Date")
      .agg((sum("DOLLARS")/sum("MSRP__")).as("dolMSRPRatio"))
    //writeDF(L1CompetitionNonHP,"L1CompetitionNonHP")
    L1CompetitionNonHP = L1CompetitionNonHP
      .withColumn("L1_competition", lit(1)-col("dolMSRPRatio")).drop("dolMSRPRatio")
    //writeDF(L1CompetitionNonHP,"L1CompetitionNonHP")
    val L2CompetitionNonHP = npdChannelNotRetailBrandNotHP
      .groupBy("L2_Category","Week_End_Date")
      .agg((sum("DOLLARS")/sum("MSRP__")).as("dolMSRPRatio"))
      .withColumn("L2_competition", lit(1)-col("dolMSRPRatio")).drop("dolMSRPRatio")
    //writeDF(L2CompetitionNonHP,"L2CompetitionNonHP_378")
    commercialWithCompetitionDF = commercialWithCompetitionDF.join(L1CompetitionNonHP, Seq("L1_Category","Week_End_Date"), "left")
        .join(L2CompetitionNonHP, Seq("L2_Category","Week_End_Date"), "left")
        .withColumn("L1_competition", when((col("L1_competition").isNull) || (col("L1_competition")<0), 0).otherwise(col("L1_competition")))
        .withColumn("L2_competition", when((col("L2_competition").isNull) || (col("L2_competition")<0), 0).otherwise(col("L2_competition")))
        .na.fill(0, Seq("L1_competition","L2_competition"))
    //writeDF(commercialWithCompetitionDF,"commercialWithCompetitionDF_FORNONHP_Competition")
    
    /*=================================== Brand Not Samsung ===================================*/
    val npdChannelNotRetailBrandNotSamsung = npd.where((col("Channel")=!="Retail") && (col("Brand")=!="Samsung"))
      .where((col("DOLLARS")>0) && (col("MSRP__")>0))
    //writeDF(npdChannelNotRetailBrandNotSamsung,"npdChannelNotRetailBrandNotSamsung")
    val L1CompetitionSS = npdChannelNotRetailBrandNotSamsung
      .groupBy("L1_Category","Week_End_Date")
      .agg((sum("DOLLARS")/sum("MSRP__")).as("dolMSRPRatio"))
      .withColumn("L1_competition_ss", lit(1)-col("dolMSRPRatio")).drop("dolMSRPRatio")
    //writeDF(L1CompetitionSS,"L1CompetitionSS")
    val L2CompetitionSS = npdChannelNotRetailBrandNotSamsung
      .groupBy("L2_Category","Week_End_Date")
      .agg((sum("DOLLARS")/sum("MSRP__")).as("dolMSRPRatio"))
      .withColumn("L2_competition_ss", lit(1)-col("dolMSRPRatio")).drop("dolMSRPRatio")
    //writeDF(L2CompetitionSS,"L2CompetitionSS_BEFORE_JOIN_L1_AND_L2")

    commercialWithCompetitionDF = commercialWithCompetitionDF.join(L1CompetitionSS, Seq("L1_Category","Week_End_Date"), "left")
        .join(L2CompetitionSS, Seq("L2_Category","Week_End_Date"), "left")
      .withColumn("L1_competition_ss", when((col("L1_competition_ss").isNull) || (col("L1_competition_ss")<0), 0).otherwise(col("L1_competition_ss")))
      .withColumn("L2_competition_ss", when((col("L2_competition_ss").isNull) || (col("L2_competition_ss")<0), 0).otherwise(col("L2_competition_ss")))
      .na.fill(0, Seq("L1_competition_ss","L2_competition_ss"))

    commercialWithCompetitionDF = commercialWithCompetitionDF
        .withColumn("L1_competition", when(col("Brand").isin("Samsung"), col("L1_competition_ss")).otherwise(col("L1_competition")))
        .withColumn("L2_competition", when(col("Brand").isin("Samsung"), col("L2_competition_ss")).otherwise(col("L2_competition")))
        .drop("L2_competition_ss","L1_competition_ss")
    //writeDF(commercialWithCompetitionDF,"commercialWithCompetitionDF_Samsung")*/
    
    /* ========================================================================================== */
    //Jan 30 - 10:38 PM -- Matching till here
    val commercialBrandinHP = commercialWithCompetitionDF.where(col("Brand").isin("HP"))
        .withColumn("Qty_pmax",greatest(col("Qty"),lit(0)))
    //writeDF(commercialBrandinHP,"commercialBrandinHP_WITH_QTY_PMAX")
    val HPComp1 = commercialBrandinHP
      .groupBy("Week_End_Date","L1_Category")
      .agg(sum("Qty_pmax").as("sum2"), (sum(col("Promo_Pct")*col("Qty_pmax"))).as("sum1"))
      .withColumn("sum1", when(col("sum1")<0, 0).otherwise(col("sum1")))
      .withColumn("sum2", when(col("sum2")<0, 0).otherwise(col("sum2")))
      .withColumn("L1_competition_HP_ssmodel", col("sum1")/col("sum2"))
      .drop("sum1","sum2")
      //writeDF(HPComp1,"HPComp1")
    val HPComp2 = commercialBrandinHP
      .groupBy("Week_End_Date","L2_Category")
      .agg(sum("Qty_pmax").as("sum2"), (sum(col("Promo_Pct")*col("Qty_pmax"))).as("sum1"))
      .withColumn("sum1", when(col("sum1")<0, 0).otherwise(col("sum1")))
      .withColumn("sum2", when(col("sum2")<0, 0).otherwise(col("sum2")))
      .withColumn("L2_competition_HP_ssmodel", col("sum1")/col("sum2"))
      .drop("sum1","sum2")
    //writeDF(HPComp2,"HPComp2")
    commercialWithCompetitionDF = commercialWithCompetitionDF.join(HPComp1, Seq("Week_End_Date","L1_Category"), "left")
        .join(HPComp2, Seq("Week_End_Date","L2_Category"), "left")
      .withColumn("L1_competition_HP_ssmodel", when((col("L1_competition_HP_ssmodel").isNull) || (col("L1_competition_HP_ssmodel")<0), 0).otherwise(col("L1_competition_HP_ssmodel")))
      .withColumn("L2_competition_HP_ssmodel", when((col("L2_competition_HP_ssmodel").isNull) || (col("L2_competition_HP_ssmodel")<0), 0).otherwise(col("L2_competition_HP_ssmodel")))
      .na.fill(0, Seq("L1_competition_HP_ssmodel","L2_competition_HP_ssmodel"))
    //writeDF(commercialWithCompetitionDF,"commercialWithCompetitionDF_L1_L2_Competition_SS_Feat")*/

    commercialWithCompetitionDF = commercialWithCompetitionDF
        .withColumn("wed_cat", concat_ws(".",col("Week_End_Date"), col("L1_Category")))
    
    val commercialWithCompetitionDFTemp1 = commercialWithCompetitionDF
        .groupBy("wed_cat")
        .agg(sum(col("Promo_Pct")*col("Qty")).as("z"), sum(col("Qty")).as("w"))

    commercialWithCompetitionDF = commercialWithCompetitionDF.join(commercialWithCompetitionDFTemp1, Seq("wed_cat"), "left")
        .withColumn("L1_cannibalization", (col("z")-(col("Promo_Pct")*col("Qty")))/(col("w")-col("Qty")))
        .drop("z","w","wed_cat")
        .withColumn("wed_cat", concat_ws(".", col("Week_End_Date"), col("L2_Category")))

    val commercialWithCompetitionDFTemp2 = commercialWithCompetitionDF
        .groupBy("wed_cat")
        .agg(sum(col("Promo_Pct")*col("Qty")).as("z"), sum(col("Qty")).as("w"))

    commercialWithCompetitionDF = commercialWithCompetitionDF.join(commercialWithCompetitionDFTemp2, Seq("wed_cat"), "left")
        .withColumn("L2_cannibalization", (col("z")-(col("Promo_Pct")*col("Qty")))/(col("w")-col("Qty")))
        .drop("z","w","wed_cat")


    var commercialWithCompCannDF = commercialWithCompetitionDF

    val commercialWithAdj = commercialWithCompCannDF.withColumn("Adj_Qty", when(col("Qty")<=0,0).otherwise(col("Qty")))
    val commercialGroupWEDSKU = commercialWithAdj.groupBy("Week_End_Date","SKU")
      .agg(sum(col("Promo_Pct")*col("Adj_Qty")).as("sumSKU1"), sum("Adj_Qty").as("sumSKU2"))
      .join(commercialWithAdj, Seq("Week_End_Date","SKU"), "right")
    //writeDF(commercialGroupWEDSKU,"commercialGroupWEDSKU")
    val commercialGroupWEDL1Temp = commercialGroupWEDSKU
      .groupBy("Week_End_Date","Brand", "L1_Category")
      .agg(sum(col("Promo_Pct")*col("Adj_Qty")).as("sum1"), sum("Adj_Qty").as("sum2"))
    //writeDF(commercialGroupWEDL1Temp,"commercialGroupWEDL1Temp")

    val commercialGroupWEDL1 = commercialGroupWEDSKU.withColumn("L1_Category",col("L1_Category"))
      .join(commercialGroupWEDL1Temp.withColumn("L1_Category",col("L1_Category")), Seq("Week_End_Date","Brand", "L1_Category"), "left")
      .withColumn("L1_cannibalization", (col("sum1")-col("sumSKU1"))/(col("sum2")-col("sumSKU2")))
      .drop("sum1","sum2")
    //writeDF(commercialGroupWEDL1,"commercialGroupWEDL1_Cann")
    val commercialGroupWEDL1Temp2 = commercialGroupWEDL1
      .groupBy("Week_End_Date","Brand", "L2_Category")
      .agg(sum(col("Promo_Pct")*col("Adj_Qty")).as("sum1"), sum("Adj_Qty").as("sum2"))
    //writeDF(commercialGroupWEDL1Temp2,"commercialGroupWEDL1Temp2")

    commercialWithCompCannDF = commercialGroupWEDL1.withColumn("L2_Category",col("L2_Category")).join(commercialGroupWEDL1Temp2.withColumn("L2_Category",col("L2_Category")), Seq("Week_End_Date","Brand", "L2_Category"), "left")
      .withColumn("L2_cannibalization", (col("sum1")-col("sumSKU1"))/(col("sum2")-col("sumSKU2")))
      .drop("sum1","sum2","sumSKU1","sumSKU2","Adj_Qty")
      .withColumn("L1_cannibalization", when(col("L1_cannibalization").isNull, 0).otherwise(col("L1_cannibalization")))
      .withColumn("L2_cannibalization", when(col("L2_cannibalization").isNull, 0).otherwise(col("L2_cannibalization")))
      .na.fill(0, Seq("L2_cannibalization","L1_cannibalization"))
      .withColumn("Sale_Price", col("Street Price")-col("IR"))
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_CANNABILIZATION")

    val AverageWeeklySales = commercialWithCompCannDF
      .groupBy("SKU","Reseller_Cluster","Sale_Price","Street Price")
      .agg(mean(col("Qty")).as("POS_Qty"))

    var npdChannelNotRetail = npd.where(col("Channel")=!="Retail")
      .withColumn("Year", year(col("Week_End_Date")).cast("string")).withColumn("Year_LEVELS", col("year"))
    npdChannelNotRetail = npdChannelNotRetail
      .withColumn("Week", when(col("Week_End_Date").isNull, null).otherwise(extractWeekFromDateUDF(col("Week_End_Date").cast("string"), lit("yyyy-MM-dd"))))

    val npdFilteredL1CategoryDF = npdChannelNotRetail.where(col("L1_Category").isNotNull)
    var seasonalityNPD = npdFilteredL1CategoryDF
      .groupBy("Week","L1_Category")
      .agg(sum("UNITS").as("UNITS"), countDistinct("Year").as("Years"))
      .withColumn("UNITS_average", col("UNITS")/col("Years"))
    ////writeDF(seasonalityNPD,"seasonalityNPD_Start")
    val seasonalityNPDSum = seasonalityNPD
      .groupBy("L1_Category")
      .agg(count("UNITS_average").as("number_weeks"),sum("UNITS_average").as("UNITS_average"))
      .withColumn("average", col("UNITS_average")/col("number_weeks"))
      .drop("UNITS_average","number_weeks")
    ////writeDF(seasonalityNPDSum,"seasonalityNPDSum")
    ////writeDF(seasonalityNPD.join(seasonalityNPDSum, Seq("L1_Category"), "left"),"SEASONALITYNPD_JOIN_NPDSUM")
    seasonalityNPD = seasonalityNPD.join(seasonalityNPDSum, Seq("L1_Category"), "left")
        .withColumn("seasonality_npd", (col("UNITS_average")/col("average"))-lit(1))
        .drop("UNITS","UNITS_average","average","Years")

    commercialWithCompCannDF = commercialWithCompCannDF
      .withColumn("Week", extractWeekFromDateUDF(col("Week_End_Date").cast("string"), lit("yyyy-MM-dd"))).withColumn("Week_LEVELS",col("Week"))
    ////writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_With_WEEK")

    var seasonalityNPDScanner = seasonalityNPD.where(col("L1_Category")==="Office - Personal")
      .withColumn("L1_Category", when(col("L1_Category")==="Office - Personal", "Scanners").otherwise(col("L1_Category")))
    //writeDF(seasonalityNPDScanner,"seasonalityNPDScanner")
    seasonalityNPD = doUnion(seasonalityNPD, seasonalityNPDScanner).get
    //writeDF(seasonalityNPD,"seasonalityNPD")
    commercialWithCompCannDF = commercialWithCompCannDF.join(seasonalityNPD, Seq("L1_Category","Week"), "left")
        .drop("Week")
        .withColumn("seasonality_npd2", when((col("USCyberMonday")===lit(1)) || (col("USThanksgivingDay")===lit(1)),0).otherwise(col("seasonality_npd").cast("int")))
        .join(ifs2.where(col("Account")==="Commercial").select("SKU","Hardware_GM","Supplies_GM","Hardware_Rev","Supplies_Rev", "Changed_Street_Price", "Valid_Start_Date", "Valid_End_Date"), Seq("SKU"), "left")
        .withColumn("Valid_Start_Date", when(col("Valid_Start_Date").isNull, dat2000_01_01).otherwise(col("Valid_Start_Date")))
        .withColumn("Valid_End_Date", when(col("Valid_End_Date").isNull, dat9999_12_31).otherwise(col("Valid_End_Date")))
        .where((col("Week_End_Date")>=col("Valid_Start_Date")) && (col("Week_End_Date")<col("Valid_End_Date")))
        .withColumn("Changed_Street_Price", when(col("Changed_Street_Price").isNull, 0).otherwise(col("Changed_Street_Price")))
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_Seasonality")

    val ifs2FilteredAccount = ifs2.where(col("Account").isin("Best Buy","Office Depot-Max","Staples"))
    val ifs2RetailAvg = ifs2FilteredAccount
      .groupBy("SKU")
      .agg(mean("Hardware_GM").as("Hardware_GM_retail_avg"), mean("Hardware_Rev").as("Hardware_Rev_retail_avg"), mean("Supplies_GM").as("Supplies_GM_retail_avg"), mean("Supplies_Rev").as("Supplies_Rev_retail_avg")).cache()
    //writeDF(ifs2RetailAvg,"ifs2RetailAvg_WITH_HARDWARE_SUPPLIES")
    commercialWithCompCannDF = commercialWithCompCannDF.drop("Hardware_GM_retail_avg","Supplies_GM_retail_avg","Hardware_Rev_retail_avg","Supplies_Rev_retail_avg")
      .join(ifs2RetailAvg.select("SKU","Hardware_GM_retail_avg","Supplies_GM_retail_avg","Hardware_Rev_retail_avg","Supplies_Rev_retail_avg"), Seq("SKU"), "left")
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_BEFORE_IFS2_JOIN")
    commercialWithCompCannDF = commercialWithCompCannDF
      .withColumn("Hardware_GM_type", when(col("Hardware_GM").isNotNull, "Commercial").otherwise(when((col("Hardware_GM").isNull) && (col("Hardware_GM_retail_avg").isNotNull), "Retail").otherwise(lit(null))))
      .withColumn("Hardware_Rev_type", when(col("Hardware_Rev").isNotNull, "Commercial").otherwise(when((col("Hardware_Rev").isNull) && (col("Hardware_Rev_retail_avg").isNotNull), "Retail").otherwise(lit(null))))
      .withColumn("Supplies_GM_type", when(col("Supplies_GM").isNotNull, "Commercial").otherwise(when((col("Supplies_GM").isNull) && (col("Supplies_GM_retail_avg").isNotNull), "Retail").otherwise(lit(null))))
      .withColumn("Supplies_Rev_type", when(col("Supplies_Rev").isNotNull, "Commercial").otherwise(when((col("Supplies_Rev").isNull) && (col("Supplies_Rev_retail_avg").isNotNull), "Retail").otherwise(lit(null))))
      .withColumn("Hardware_GM", when(col("Hardware_GM").isNull, col("Hardware_GM_retail_avg")).otherwise(col("Hardware_GM")))
      .withColumn("Hardware_Rev", when(col("Hardware_Rev").isNull, col("Hardware_Rev_retail_avg")).otherwise(col("Hardware_Rev")))
      .withColumn("Supplies_GM", when(col("Supplies_GM").isNull, col("Supplies_GM_retail_avg")).otherwise(col("Supplies_GM")))
      .withColumn("Supplies_Rev", when(col("Supplies_Rev").isNull, col("Supplies_Rev_retail_avg")).otherwise(col("Supplies_Rev")))
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_WITH_HARDWARE_SUPPLIES_FEAT")*/

    val avgDiscountSKUAccountDF = commercialWithCompCannDF
      .groupBy("SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS")
      .agg(sum(col("Qty")*col("IR")).as("Qty_IR"),sum(col("Qty")*col("Street Price").cast("double")).as("QTY_SP"))
      .withColumn("avg_discount_SKU_Account",col("Qty_IR")/col("QTY_SP"))
    //writeDF(avgDiscountSKUAccountDF,"avgDiscountSKUAccountDF")

    commercialWithCompCannDF = commercialWithCompCannDF.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      .join(avgDiscountSKUAccountDF.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS"), "left")
        .withColumn("avg_discount_SKU_Account", when(col("avg_discount_SKU_Account").isNull, 0).otherwise(col("avg_discount_SKU_Account")))
        .na.fill(0, Seq("avg_discount_SKU_Account"))
        .withColumn("supplies_GM_scaling_factor", lit(-0.3))
        .withColumn("Supplies_GM_unscaled", col("Supplies_GM"))
        .withColumn("Supplies_GM", col("Supplies_GM_unscaled")*(lit(1)+((col("Promo_Pct")-col("avg_discount_SKU_Account"))*col("supplies_GM_scaling_factor"))))
        .withColumn("Supplies_GM_no_promo", col("Supplies_GM_unscaled")*(lit(1)+((lit(0)-col("avg_discount_SKU_Account"))*col("supplies_GM_scaling_factor"))))
        .withColumn("Supplies_Rev_unscaled", col("Supplies_Rev"))
        .withColumn("Supplies_Rev", col("Supplies_Rev_unscaled")*(lit(1)+((col("Promo_Pct")-col("avg_discount_SKU_Account"))*col("supplies_GM_scaling_factor"))))
        .withColumn("Supplies_Rev_no_promo", col("Supplies_Rev_unscaled")*(lit(1)+((lit(0)-col("avg_discount_SKU_Account"))*col("supplies_GM_scaling_factor"))))
        .drop("Hardware_GM_retail_avg","Hardware_Rev_retail_avg","Supplies_GM_retail_avg","Supplies_Rev_retail_avg")
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_JOIN_AvgDISCOUNT_SKUACCOUNT")
    commercialWithCompCannDF = commercialWithCompCannDF
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
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_WITH_LOGS")*/

    val wind = Window.partitionBy("SKU_Name","Reseller_Cluster").orderBy("Qty")
    commercialWithCompCannDF.createOrReplaceTempView("commercial")

    val percentil75DF = spark.sql("select SKU_Name, Reseller_Cluster, Reseller_Cluster_LEVELS, PERCENTILE(Qty, 0.75) OVER (PARTITION BY SKU_Name, Reseller_Cluster, Reseller_Cluster_LEVELS) as percentile_0_75 from commercial")
        .dropDuplicates("SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS","percentile_0_75")
    //writeDF(percentil75DF,"percentil75DF")
    commercialWithCompCannDF = commercialWithCompCannDF.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      .join(percentil75DF.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("SKU_Name", "Reseller_Cluster","Reseller_Cluster_LEVELS"), "left")
    commercialWithCompCannDF.createOrReplaceTempView("commercial")
    val percentile25DF = spark.sql("select SKU_Name, Reseller_Cluster, Reseller_Cluster_LEVELS, PERCENTILE(Qty, 0.25) OVER (PARTITION BY SKU_Name, Reseller_Cluster, Reseller_Cluster_LEVELS) as percentile_0_25 from commercial")
      .dropDuplicates("SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS","percentile_0_25")
    //writeDF(percentile25DF,"percentile25DF")
    commercialWithCompCannDF = commercialWithCompCannDF.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      .join(percentile25DF.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("SKU_Name", "Reseller_Cluster","Reseller_Cluster_LEVELS"), "left")
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_WITH_75_25_QUANTILE")
    commercialWithCompCannDF = commercialWithCompCannDF.withColumn("IQR", col("percentile_0_75")-col("percentile_0_25"))
        .withColumn("outlier", when(col("Qty")>col("percentile_0_75"), (col("Qty")-col("percentile_0_75"))/col("IQR")).otherwise(when(col("Qty")<col("percentile_0_25"), (col("Qty")-col("percentile_0_25"))/col("IQR")).otherwise(lit(0))))
        .withColumn("spike", when(abs(col("outlier"))<=8, 0).otherwise(1))
        .withColumn("spike", when((col("SKU_Name")==="OJ Pro 8610") && (col("Reseller_Cluster")==="Other - Option B") && (col("Week_End_Date")==="2014-11-01"),1).otherwise(col("spike")))
        .withColumn("spike", when((col("SKU").isin("F6W14A")) && (col("Week_End_Date")==="2017-07-10") && (col("Reseller_Cluster").isin("Other - Option B")), 1).otherwise(col("spike")))
        .withColumn("spike2", when((col("spike")===1) && (col("IR")>0), 0).otherwise(col("spike")))
        .drop("percentile_0_75", "percentile_0_25","IQR")
        .withColumn("Qty", col("Qty").cast("int"))
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_Spike")*/

    commercialWithCompCannDF = commercialWithCompCannDF.withColumn("Qty",col("Qty").cast("int"))
    val commercialWithHolidayAndQtyFilter = commercialWithCompCannDF.withColumn("Qty",col("Qty").cast("int"))
      .where((col("Promo_Flag")===0) && (col("USThanksgivingDay")===0) && (col("USCyberMonday")===0) && (col("spike")===0))
      .where(col("Qty")>0)
    ////writeDF(commercialWithHolidayAndQtyFilter,"commercialWithHolidayAndQtyFilter_FILTER_FOR_NPBL")
    var npbl = commercialWithHolidayAndQtyFilter
      .groupBy("Reseller_Cluster","Reseller_Cluster_LEVELS","SKU_Name")
      .agg(mean("Qty").as("no_promo_avg"),
        stddev("Qty").as("no_promo_sd"),
        min("Qty").as("no_promo_min"),
        max("Qty").as("no_promo_max"))
    ////writeDF(npbl,"npbl_BEFORE_MEDIAN")
    commercialWithHolidayAndQtyFilter.createOrReplaceTempView("npbl")
    val npblTemp = spark.sql("select SKU_Name, Reseller_Cluster, Reseller_Cluster_LEVELS, PERCENTILE(Qty, 0.50) OVER (PARTITION BY SKU_Name, Reseller_Cluster, Reseller_Cluster_LEVELS) as no_promo_med from npbl")
        .dropDuplicates("SKU_Name", "Reseller_Cluster", "Reseller_Cluster_LEVELS","no_promo_med")
    ////writeDF(npblTemp,"npblTemp")
    npbl = npbl.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      .join(npblTemp.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("Reseller_Cluster","SKU_Name","Reseller_Cluster_LEVELS"), "inner")
    ////writeDF(npbl,"npbl")

    commercialWithCompCannDF = commercialWithCompCannDF.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      .join(npbl.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster")).select("SKU_Name", "Reseller_Cluster","Reseller_Cluster_LEVELS","no_promo_avg", "no_promo_med"), Seq("SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS"), "left")
        .withColumn("no_promo_avg", when(col("no_promo_avg").isNull, 0).otherwise(col("no_promo_avg")))
        .withColumn("no_promo_med", when(col("no_promo_med").isNull, 0).otherwise(col("no_promo_med")))
        .withColumn("low_baseline", when(((col("no_promo_avg")>=min_baseline) && (col("no_promo_med")>=baselineThreshold)) || ((col("no_promo_med")>=min_baseline) && (col("no_promo_avg")>=baselineThreshold)),0).otherwise(1))
        .withColumn("low_volume", when(col("Qty")>0,0).otherwise(1))
        .withColumn("raw_bl_avg", col("no_promo_avg")*(col("seasonality_npd")+lit(1)))
        .withColumn("raw_bl_med", col("no_promo_med")*(col("seasonality_npd")+lit(1)))
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_AFTER_NPBL")*/

    val windForSKUAndReseller = Window.partitionBy("SKU&Reseller")
      .orderBy(/*"SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS",*/"Week_End_Date")

    var EOLcriterion = commercialWithCompCannDF
      .groupBy("SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS","Week_End_Date")
      .agg(sum("Qty").as("Qty"), sum("no_promo_med").as("no_promo_med"))
      .sort("SKU_Name","Reseller_Cluster_LEVELS","Week_End_Date")
      .withColumn("Qty&no_promo_med", concat_ws(";",col("Qty"), col("no_promo_med")))
      .withColumn("SKU&Reseller", concat(col("SKU_Name"), col("Reseller_Cluster_LEVELS")))
    //writeDF(EOLcriterion,"EOLcriterion_FIRST")

    EOLcriterion = EOLcriterion.orderBy("SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS","Week_End_Date")
      .withColumn("rank", row_number().over(windForSKUAndReseller))
    //writeDF(EOLcriterion,"EOLcriterion_WITH_RANK")

    var EOLWithCriterion1 = EOLcriterion
      .groupBy("SKU&Reseller").agg((collect_list(concat_ws("_",col("rank"),col("Qty")).cast("string"))).as("QtyArray"))

    EOLWithCriterion1 = EOLWithCriterion1
      .withColumn("QtyArray", when(col("QtyArray").isNull, null).otherwise(concatenateRank(col("QtyArray"))))
    ////writeDF(EOLWithCriterion1,"EOLWithCriterion1_WITH_QTYARRAY")

    EOLcriterion = EOLcriterion.join(EOLWithCriterion1, Seq("SKU&Reseller"), "left")
      .withColumn("EOL_criterion", when(col("rank")<=stability_weeks || col("Qty")<col("no_promo_med"), 0).otherwise(checkPrevQtsGTBaseline(col("QtyArray"), col("rank"), col("no_promo_med"), lit(stability_weeks))))
      .drop("rank","QtyArray","SKU&Reseller","Qty&no_promo_med")
    //writeDF(EOLcriterion,"EOLcriterion_BEFORE_LAST_MIN_MAX")
    
    val EOLCriterionLast = EOLcriterion.where(col("EOL_criterion")===lit(1))
      .groupBy("SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS")
      .agg(max("Week_End_Date").as("last_date"))
    //writeDF(EOLCriterionLast,"EOLCriterionLast")

    val EOLCriterionMax = commercialWithCompCannDF
      .groupBy("SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS")
      .agg(max("Week_End_Date").as("max_date"))
    //writeDF(EOLCriterionMax,"EOLCriterionMax")

    EOLcriterion = EOLCriterionMax.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      .withColumn("Reseller_Cluster_LEVELS",col("Reseller_Cluster_LEVELS"))
    .join(EOLCriterionLast.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster")).withColumn("Reseller_Cluster_LEVELS",col("Reseller_Cluster_LEVELS")), Seq("SKU_Name","Reseller_Cluster_LEVELS","Reseller_Cluster"), "left")
    .where(col("last_date").isNotNull)
    //writeDF(EOLcriterion,"EOLcriterion_BEFORE_MAXMAXDAte")

    val maxMaxDate = EOLcriterion.agg(max("max_date")).head().getDate(0)
    EOLcriterion = EOLcriterion
      .where((col("max_date")=!=col("last_date")) || (col("last_date")=!=maxMaxDate))
      .drop("max_date")
    //writeDF(EOLcriterion,"EOLcriterion_AFTER_MAXMAX")

    commercialWithCompCannDF = commercialWithCompCannDF.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      .join(EOLcriterion.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS"), "left")
      .withColumn("EOL", when(col("last_date").isNull, 0).otherwise(when(col("Week_End_Date")>col("last_date"),1).otherwise(0)))
      .drop("last_date")
      //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_BEFORE_SKU_COMP_IN_EOL")
    commercialWithCompCannDF = commercialWithCompCannDF
      .withColumn("EOL", when(col("SKU").isin("G3Q47A","M9L75A","F8B04A","B5L24A","L2719A","D3Q19A","F2A70A","CF377A","L2747A","F0V69A","G3Q35A","C5F93A","CZ271A","CF379A","B5L25A","D3Q15A","B5L26A","L2741A","CF378A","L2749A","CF394A"),0).otherwise(col("EOL")))
      .withColumn("EOL", when((col("SKU")==="C5F94A") && (col("Season")=!="STS'17"), 0).otherwise(col("EOL")))
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_WITH_EOL")*/
    
    var BOL = commercialWithCompCannDF.select("SKU","ES date_LEVELS","GA date_LEVELS")
      .withColumnRenamed("ES date_LEVELS","ES date").withColumnRenamed("GA date_LEVELS","GA date").dropDuplicates()
    //writeDF(BOL,"BOL_WITH_DROP_DUPLICATES")
    
    BOL = BOL.where((col("ES date").isNotNull) || (col("GA date").isNotNull))
    //writeDF(BOL,"BOL_WITH_ES_GA_DATE_CHECK")
    
    BOL = BOL
      .withColumn("ES date_wday", dayofweek(col("ES date")).cast("int"))  //As dayofweek returns in range 1-7 we want 0-6
      .withColumn("GA date_wday", dayofweek(col("GA date")).cast("int"))
      .withColumn("ES date_wday_sub", lit(7)-col("ES date_wday"))
      .withColumn("GA date_wday_sub", lit(7)-col("GA date_wday"))
      .withColumn("ES date_wday_sub", when(col("ES date_wday_sub").isNull,0).otherwise(col("ES date_wday_sub")))
      .withColumn("GA date_wday_sub", when(col("GA date_wday_sub").isNull,0).otherwise(col("GA date_wday_sub")))
    //writeDF(BOL,"BOL_WITH_WDAYs")

    BOL = BOL
      .withColumnRenamed("GA date","GA_date").withColumnRenamed("ES date","ES_date")
      .withColumnRenamed("GA date_wday_sub","GA_date_wday_sub").withColumnRenamed("ES date_wday_sub","ES_date_wday_sub")
      .withColumn("GA_date", expr("date_add(GA_date,GA_date_wday_sub)"))
      .withColumn("ES_date", expr("date_add(ES_date,ES_date_wday_sub)"))
      .withColumnRenamed("GA_date","GA date").withColumnRenamed("ES_date","ES date")
      .withColumnRenamed("GA_date_wday_sub","GA date_wday_sub").withColumnRenamed("ES_date_wday_sub","ES date_wday_sub")
      .drop("GA date_wday","ES date_wday","GA date_wday_sub","ES date_wday_sub")
    //writeDF(BOL,"BOL_WITH_WDAY_MODIFIED")

    val windForSKUnReseller = Window.partitionBy("SKU$Reseller").orderBy(/*"SKU","Reseller_Cluster","Reseller_Cluster_LEVELS",*/"Week_End_Date")
    var BOLCriterion = commercialWithCompCannDF
      .groupBy("SKU","Reseller_Cluster","Reseller_Cluster_LEVELS","Week_End_Date")
      .agg(sum("Qty").as("Qty"))
      .sort("SKU","Reseller_Cluster","Reseller_Cluster_LEVELS","Week_End_Date")
      .withColumn("SKU$Reseller", concat(col("SKU"),col("Reseller_Cluster_LEVELS")))
      .withColumn("rank", row_number().over(windForSKUnReseller))
      .withColumn("BOL_criterion", when(col("rank")<intro_weeks, 0).otherwise(1))
      .drop("rank","Qty")
    //writeDF(BOLCriterion,"BOLCriterion_Before_MinWedDATE")
    
    BOLCriterion = BOLCriterion
      .join(BOL.select("SKU","GA date"), Seq("SKU"), "left")
    val minWEDDate = to_date(unix_timestamp(lit(BOLCriterion.agg(min("Week_End_Date")).head().getDate(0)),"yyyy-MM-dd").cast("timestamp"))
    BOLCriterion = BOLCriterion.withColumn("GA date", when(col("GA date").isNull, minWEDDate).otherwise(col("GA date")))
      .where(col("Week_End_Date")>=col("GA date"))
    //writeDF(BOLCriterion,"BOLCriterion_WITH_WEEK_END_DATE_FILTER_AGAINST_GA")
    val BOLCriterionFirst = BOLCriterion.where(col("BOL_criterion")===1)
      .groupBy("SKU","Reseller_Cluster","Reseller_Cluster_LEVELS")
      .agg(min("Week_End_Date").as("first_date"))
    //writeDF(BOLCriterionFirst,"BOLCriterionFirst")
    val BOLCriterionMax = commercialWithCompCannDF
      .groupBy("SKU","Reseller_Cluster","Reseller_Cluster_LEVELS")
      .agg(max("Week_End_Date").as("max_date"))
    //writeDF(BOLCriterionMax,"BOLCriterionMax")
    val BOLCriterionMin = commercialWithCompCannDF//.where(col("BOL_criterion")===1)
      .groupBy("SKU","Reseller_Cluster","Reseller_Cluster_LEVELS")
      .agg(min("Week_End_Date").as("min_date"))
    //writeDF(BOLCriterionMin,"BOLCriterionMin")

    BOLCriterion =  BOLCriterionMax.withColumn("SKU",col("SKU")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      .join(BOLCriterionFirst.withColumn("SKU",col("SKU")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("SKU","Reseller_Cluster","Reseller_Cluster_LEVELS"), "left")
      .join(BOLCriterionMin.withColumn("SKU",col("SKU")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("SKU","Reseller_Cluster","Reseller_Cluster_LEVELS"), "left")
    //writeDF(BOLCriterion,"BOLCriterion_AFTER_FIRST_MIN_MAX_JOIN")*/
    
    BOLCriterion = BOLCriterion.withColumn("first_date", when(col("first_date").isNull, col("max_date")).otherwise(col("first_date")))
      .drop("max_date")
    //writeDF(BOLCriterion,"BOLCriterion_WITH_FIRST_DATE")
    val minMinDateBOL = BOLCriterion.agg(min("min_date")).head().getDate(0)
    BOLCriterion = BOLCriterion
      .where(!((col("min_date")===col("first_date")) && (col("first_date")===minMinDateBOL)))
      .withColumn("diff_weeks", ((datediff(to_date(col("first_date")),to_date(col("min_date"))))/7)+1)

    BOLCriterion = BOLCriterion.withColumn("diff_weeks", when(col("diff_weeks").isNull || col("diff_weeks")<=0, 0).otherwise(col("diff_weeks")))
    BOLCriterion = BOLCriterion.withColumn("diff_weeks", col("diff_weeks").cast("int"))
    //writeDF(BOLCriterion,"BOLCriterion_WITH_DIFF_WEEKS")*/
    
    BOLCriterion = BOLCriterion
      .withColumn("repList", createlist(col("diff_weeks").cast("int")))
      .withColumn("add", explode(col("repList"))).drop("repList")
      .withColumn("add", col("add")/*+lit(1)*/)
    //writeDF(BOLCriterion,"BOLCriterion_AFTER_EXPLODE")
    BOLCriterion = BOLCriterion
      .withColumn("add", col("add")*lit(7))
      .withColumn("Week_End_Date", expr("date_add(min_date,add)"))
    //.withColumn("Week_End_Date", addDaystoDateStringUDF(col("min_date"), col("add")))   //CHECK: check if min_date is in timestamp format!

    BOLCriterion = BOLCriterion.drop("min_date","fist_date","diff_weeks","add")
      .withColumn("BOL_criterion", lit(1))
    //writeDF(BOLCriterion, "BOLCriterion_FINAL_DF")

    commercialWithCompCannDF = commercialWithCompCannDF.drop("GA date").withColumn("GA date",col("GA date_LEVELS"))//TODO Remove this
      .withColumn("SKU",col("SKU")).withColumn("Reseller_Cluster",col("Reseller_Cluster")).withColumn("Week_End_Date",col("Week_End_Date"))
      .join(BOLCriterion.withColumn("SKU",col("SKU")).withColumn("Reseller_Cluster",col("Reseller_Cluster")).withColumn("Week_End_Date",col("Week_End_Date")), Seq("SKU","Reseller_Cluster","Reseller_Cluster_LEVELS","Week_End_Date"), "left")
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_WITH_BOL_JOINED_BEFORE_NULL_IMPUTATION")*/
    
    commercialWithCompCannDF = commercialWithCompCannDF
      .withColumn("BOL", when(col("EOL")===1,0).otherwise(col("BOL_criterion")))
      .withColumn("BOL", when(datediff(col("Week_End_Date"),col("GA date_LEVELS"))<(7*6),1).otherwise(col("BOL")))
      .withColumn("BOL", when(col("GA date_LEVELS").isNull, 0).otherwise(col("BOL")))
      .withColumn("BOL", when(col("BOL").isNull, 0).otherwise(col("BOL"))).cache()
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_Join_BOLCRITERIA")
    
    val commercialEOLSpikeFilter = commercialWithCompCannDF.where((col("EOL")===0) && (col("spike")===0))
    var opposite = commercialEOLSpikeFilter
      .groupBy("SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS")
      .agg(count("SKU_Name").as("n"),
        mean("Qty").as("Qty_total"))

    var opposite_Promo_flag = commercialEOLSpikeFilter.where(col("Promo_Flag")===1)
      .groupBy("SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS")
    .agg(mean("Qty").as("Qty_promo"))

    var opposite_Promo_flag_ZERO = commercialEOLSpikeFilter.where(col("Promo_Flag")===0)
      .groupBy("SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS")
      .agg(mean("Qty").as("Qty_no_promo"))

    opposite = opposite.join(opposite_Promo_flag, Seq("SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS"), "left")
      .join(opposite_Promo_flag_ZERO, Seq("SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS"), "left")

    //writeDF(opposite,"opposite_BEFORE_MERGE")
    opposite = opposite
      .withColumn("opposite", when((col("Qty_no_promo")>col("Qty_promo")) || (col("Qty_no_promo")<0), 1).otherwise(0))
      .withColumn("opposite", when(col("opposite").isNull, 0).otherwise(col("opposite")))
      .withColumn("no_promo_sales", when(col("Qty_promo").isNull, 1).otherwise(0))
    //writeDF(opposite,"opposite")
    commercialWithCompCannDF = commercialWithCompCannDF.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      .join(opposite.select("SKU_Name", "Reseller_Cluster","Reseller_Cluster_LEVELS", "opposite","no_promo_sales").withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS"), "left")
        .withColumn("NP_Flag", col("Promo_Flag"))
        .withColumn("NP_IR", col("IR"))
        .withColumn("high_disc_Flag", when(col("Promo_Pct")<=0.55, 0).otherwise(1))
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_WITH_OPPOSITE")

    val commercialPromoMean = commercialWithCompCannDF
      .groupBy("Reseller_Cluster_LEVELS","Reseller_Cluster","SKU_Name","Season")
      .agg(mean(col("Promo_Flag")).as("PromoFlagAvg"))
    //writeDF(commercialPromoMean,"commercialPromoMean_WITH_PROMO_FLAG_Avg")*/
    
    commercialWithCompCannDF = commercialWithCompCannDF.join(commercialPromoMean, Seq("Reseller_Cluster","Reseller_Cluster_LEVELS","SKU_Name","Season"), "left")
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_JOINED_PROMO_MEAN")
    commercialWithCompCannDF = commercialWithCompCannDF
      .withColumn("always_promo_Flag", when(col("PromoFlagAvg")===1, 1).otherwise(0)).drop("PromoFlagAvg")
      .withColumn("EOL", when(col("Reseller_Cluster_LEVELS")==="CDW",
        when(col("SKU_Name")==="LJ Pro M402dn", 0).otherwise(col("EOL"))).otherwise(col("EOL")))
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_WITH_LAST_EOL_MODIFICATION")
    
    commercialWithCompCannDF = commercialWithCompCannDF
      .withColumn("cann_group", lit("None"))
      .withColumn("cann_group", when(col("SKU_Name").contains("M20") || col("SKU_Name").contains("M40"),"M201_M203/M402").otherwise(col("cann_group")))
      .withColumn("cann_group", when(col("SKU_Name").contains("M22") || col("SKU_Name").contains("M42"),"M225_M227/M426").otherwise(col("cann_group")))
      .withColumn("cann_group", when(col("SKU_Name").contains("M25") || col("SKU_Name").contains("M45"),"M252_M254/M452").otherwise(col("cann_group")))
      .withColumn("cann_group", when(col("SKU_Name").contains("M27") || col("SKU_Name").contains("M28"),"M277_M281/M477").otherwise(col("cann_group")))
      .withColumn("cann_group", when(col("SKU_Name").contains("8710") || col("SKU_Name").contains("8720") || col("SKU_Name").contains("6968") || col("SKU_Name").contains("6978"),"Weber/Muscatel").otherwise(col("cann_group")))
      .withColumn("cann_group", when(col("SKU_Name").contains("6255") || col("SKU_Name").contains("7155") || col("SKU_Name").contains("7855") || col("SKU_Name").contains("6988") || col("SKU_Name").contains("6978"),"Palermo/Muscatel").otherwise(col("cann_group")))
      .withColumn("cann_receiver", lit("None"))
      .withColumn("cann_receiver", when(col("SKU_Name").contains("M40"), "M402").otherwise(col("cann_receiver")))
      .withColumn("cann_receiver", when(col("SKU_Name").contains("M42"), "M426").otherwise(col("cann_receiver")))
      .withColumn("cann_receiver", when(col("SKU_Name").contains("M45"), "M452").otherwise(col("cann_receiver")))
      .withColumn("cann_receiver", when(col("SKU_Name").contains("M47"), "M477").otherwise(col("cann_receiver")))
      .withColumn("cann_receiver", when(col("SKU_Name").contains("8710") || col("SKU_Name").contains("8720"), "Weber").otherwise(col("cann_receiver")))
      .withColumn("cann_receiver", when(col("SKU_Name").contains("6968") || col("SKU_Name").contains("6978"), "Muscatel").otherwise(col("cann_receiver")))
      .withColumn("cann_receiver", when(col("SKU_Name").contains("6255") || col("SKU_Name").contains("7155") || col("SKU_Name").contains("7855"), "Palermo").otherwise(col("cann_receiver")))
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_AFTER_CANN_RECEIVER_CALC")

    commercialWithCompCannDF=commercialWithCompCannDF
      .withColumn("is201",when(col("SKU_Name").contains("M201") or col("SKU_Name").contains("M203"),1).otherwise(0))
      .withColumn("is225",when(col("SKU_Name").contains("M225") or col("SKU_Name").contains("M227"),1).otherwise(0))
      .withColumn("is252",when(col("SKU_Name").contains("M252") or col("SKU_Name").contains("M254"),1).otherwise(0))
      .withColumn("is277",when(col("SKU_Name").contains("M277") or col("SKU_Name").contains("M281"),1).otherwise(0))
      .withColumn("isM40",when(col("SKU_Name").contains("M40"),1).otherwise(0))
      .withColumn("isM42",when(col("SKU_Name").contains("M42"),1).otherwise(0))
      .withColumn("isM45",when(col("SKU_Name").contains("M45"),1).otherwise(0))
      .withColumn("isM47",when(col("SKU_Name").contains("M47"),1).otherwise(0))

    val commWeek1=commercialWithCompCannDF.where(col("isM40")===1).groupBy("Week_End_Date").agg(mean(col("IR")).alias("Direct_Cann_201"))
      .withColumn("is201",lit(1))
    val commWeek2=commercialWithCompCannDF.where(col("isM42")===1).groupBy("Week_End_Date").agg(mean(col("IR")).alias("Direct_Cann_225"))
      .withColumn("is225",lit(1))
    val commWeek3=commercialWithCompCannDF.where(col("isM45")===1).groupBy("Week_End_Date").agg(mean(col("IR")).alias("Direct_Cann_252"))
      .withColumn("is252",lit(1))
    val commWeek4=commercialWithCompCannDF.where(col("isM47")===1).groupBy("Week_End_Date").agg(mean(col("IR")).alias("Direct_Cann_277"))
      .withColumn("is277",lit(1))
    commercialWithCompCannDF=commercialWithCompCannDF.join(commWeek1, Seq("is201", "Week_End_Date"), "left")
      .join(commWeek2, Seq("is225", "Week_End_Date"), "left")
      .join(commWeek3, Seq("is252", "Week_End_Date"), "left")
      .join(commWeek4, Seq("is277", "Week_End_Date"), "left")
      .withColumn("Direct_Cann_201", when(col("Direct_Cann_201").isNull,0).otherwise(col("Direct_Cann_201")))
      .withColumn("Direct_Cann_225", when(col("Direct_Cann_201").isNull,0).otherwise(col("Direct_Cann_225")))
      .withColumn("Direct_Cann_252", when(col("Direct_Cann_201").isNull,0).otherwise(col("Direct_Cann_252")))
      .withColumn("Direct_Cann_277", when(col("Direct_Cann_201").isNull,0).otherwise(col("Direct_Cann_277")))
   //writeDF(commercialWithCompCannDF, "commercialWithCompCannDF_WITH_DIRECT_CANN")

    commercialWithCompCannDF = commercialWithCompCannDF.drop("is225","is201","is252","is277","isM40","isM42","isM45","isM47")
      .withColumn("Hardware_GM", when(col("Category_1")==="Value" && col("Week_End_Date")>="2016-07-01", col("Hardware_GM")+lit(68)).otherwise(col("Hardware_GM")))
      .withColumn("Hardware_GM", when(col("Category_1")==="Value" && col("Week_End_Date")>="2017-05-01", col("Hardware_GM")+lit(8)).otherwise(col("Hardware_GM")))
      .withColumn("Hardware_GM", when(col("Category Custom")==="A4 SMB" && col("Week_End_Date")>="2017-11-01", col("Hardware_GM")-lit(7.51)).otherwise(col("Hardware_GM")))
      .withColumn("Hardware_GM", when(col("Category Custom").isin("A4 Value","A3 Value") && col("Week_End_Date")>="2017-11-01", col("Hardware_GM")+lit(33.28)).otherwise(col("Hardware_GM")))
      .withColumn("Supplies_GM", when(col("L1_Category")==="Scanners",0).otherwise(col("Supplies_GM")))
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_DIRECT_CANN_20")*/

    commercialWithCompCannDF = commercialWithCompCannDF
      .withColumn("exclude", when(!col("PL_LEVELS").isin("3Y"),when(col("Reseller_Cluster_LEVELS").isin("Other - Option C", "eTailerOther - Option C", "Stockpiler & Deal Chaser", "eTailerStockpiler & Deal Chaser") || col("low_volume")===1 || col("low_baseline")===1 || col("spike")===1 || col("opposite")===1 || col("EOL")===1 || col("BOL")===1 || col("no_promo_sales")===1,1).otherwise(0)).otherwise(0))
      .withColumn("exclude", when(col("PL_LEVELS").isin("3Y"),when(col("Reseller_Cluster_LEVELS").isin("Other - Option C", "eTailerOther - Option C", "Stockpiler & Deal Chaser", "eTailerStockpiler & Deal Chaser") || col("low_volume")===1 || /*col("low_baseline")===1 ||*/ col("spike")===1 || col("opposite")===1 || col("EOL")===1 || col("BOL")===1 || col("no_promo_sales")===1,1).otherwise(0)).otherwise(col("exclude")))
      .withColumn("exclude", when(col("SKU_Name").contains("Sprocket"), 1).otherwise(col("exclude")))
      .withColumn("AE_NP_IR", col("NP_IR"))
      .withColumn("AE_ASP_IR", lit(0))
      .withColumn("AE_Other_IR", lit(0))
      .withColumn("Street_PriceWhoChange_log", when(col("Changed_Street_Price")===0, 0).otherwise(log(col("Street Price")*col("Changed_Street_Price"))))
      .withColumn("SKUWhoChange", when(col("Changed_Street_Price")===0, 0).otherwise(col("SKU")))
      .withColumn("PriceChange_HPS_OPS", when(col("Changed_Street_Price")===0, 0).otherwise(col("HPS_OPS_LEVELS")))
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_EXCLUDE_SKUWHOCHANGE")

    val maxWED = commercialWithCompCannDF.agg(max("Week_End_Date")).head().getDate(0)
    val maxWEDSeason = commercialWithCompCannDF.where(col("Week_End_Date")===maxWED).sort(col("Week_End_Date").desc).select("Season").head().getString(0)
    val latestSeasonCommercial = commercialWithCompCannDF.where(col("Season")===maxWEDSeason)

    val windForSeason = Window.orderBy(col("Week_End_Date").desc)
    val uniqueSeason = commercialWithCompCannDF.withColumn("rank", row_number().over(windForSeason))
      .where(col("rank")===2).select("Season").head().getString(0)

    val latestSeason = latestSeasonCommercial.select("Week_End_Date").distinct().count()
    if (latestSeason<13) {
      commercialWithCompCannDF = commercialWithCompCannDF.withColumn("Season_most_recent", when(col("Season")===maxWEDSeason,uniqueSeason).otherwise(col("Season")))
    }else {
      commercialWithCompCannDF = commercialWithCompCannDF.withColumn("Season_most_recent", col("Season"))
    }

    val format = new SimpleDateFormat("d-M-y_h-m-s")
    import java.util.Calendar;
    commercialWithCompCannDF.write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/Pricing/Output/Preregression_Commercial/regression_data_commercial_Jan27.csv")
    //commercialWithCompCannDF.write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/Pricing/Output/Preregression_Commercial/regression_data_commercial"+format.format(Calendar.getInstance().getTime().toString.replace(" ","%20"))+".csv")
  }
}