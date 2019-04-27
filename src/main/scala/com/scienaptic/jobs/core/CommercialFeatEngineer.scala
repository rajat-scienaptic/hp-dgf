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

object CommercialFeatEnggProcessor {
  val Cat_switch=1
  val min_baseline = 2
  val stability_weeks = 4
  val intro_weeks = 6

  val dat2000_01_01 = to_date(unix_timestamp(lit("2000-01-01"),"yyyy-MM-dd").cast("timestamp"))
  val dat9999_12_31 = to_date(unix_timestamp(lit("9999-12-31"),"yyyy-MM-dd").cast("timestamp"))

  //val indexerForSpecialPrograms = new StringIndexer().setInputCol("Special_Programs").setOutputCol("Special_Programs_fact").setHandleInvalid("keep")
  //val pipelineForSpecialPrograms = new Pipeline().setStages(Array(indexerForSpecialPrograms))

  //val indexerForResellerCluster = new StringIndexer().setInputCol("Reseller_Cluster").setOutputCol("Reseller_Cluster_fact").setHandleInvalid("keep")
  //val pipelineForResellerCluster= new Pipeline().setStages(Array(indexerForResellerCluster))

  def execute(executionContext: ExecutionContext): Unit = {

    val sparkConf = new SparkConf().setAppName("Test")
    val spark = SparkSession.builder
      //.master("local[*]")
      .master("yarn-client")
      .appName("Commercial-R")
      .config(sparkConf)
      .getOrCreate

    val baselineThreshold = if (min_baseline/2 > 0) min_baseline/2 else 0

    import spark.implicits._
    val maxRegressionDate = renameColumns(spark.read.option("header",true).option("inferSchema",true).csv("/etherData/managedSources/NPD/NPD_weekly.csv"))
      .select("Week_End_Date").withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"),"MM/dd/yyyy").cast("timestamp")))
      .withColumn("Week_End_Date", date_sub(col("Week_End_Date"), 7)).agg(max("Week_End_Date")).head().getDate(0)

    val currentTS = executionContext.spark.read.json("/etherData/state/currentTS.json").select("ts").head().getString(0)
    //val commercialDF = spark.read.option("header","true").option("inferSchema","true").csv("E:\\Scienaptic\\HP\\Pricing\\Outputs\\April8_Run\\April8_outputs\\posqty_output_commercial.csv")
    var commercialDF = spark.read.option("header","true").option("inferSchema","true").csv("/etherData/Pricing/Outputs/POS_Commercial/posqty_commercial_output_"+currentTS+".csv")
      .repartition(500).persist(StorageLevel.MEMORY_AND_DISK)
    var commercial = renameColumns(commercialDF)
    commercial.columns.toList.foreach(x => {
      commercial = commercial.withColumn(x, when(col(x).cast("string") === "NA" || col(x).cast("string") === "", null).otherwise(col(x)))
    })
    commercial = commercial
      //.withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"),"yyyy-MM-dd").cast("timestamp")))
      //TODO For production keep this
      .withColumn("Week_End_Date", to_date(col("Week_End_Date")))
      .where(col("Week_End_Date") >= lit("2014-01-01"))
      //AVIK Change: Making max pre-regression dynamic based on NPD
      .where(col("Week_End_Date") <= lit(maxRegressionDate))
      //.where(col("Week_End_Date") <= lit("2019-03-30"))
      //.withColumn("ES date",to_date(unix_timestamp(col("ES date"),"yyyy-MM-dd").cast("timestamp")))
      //.withColumn("GA date",to_date(unix_timestamp(col("GA date"),"yyyy-MM-dd").cast("timestamp")))
      //TODO: For production keep this
      .withColumn("ES date",to_date(col("ES date")))
      .withColumn("GA date",to_date(col("GA date")))
      //.repartition(500).persist(StorageLevel.MEMORY_AND_DISK)
    commercial.printSchema()
    //val ifs2DF = spark.read.option("header","true").option("inferSchema","true").csv("E:\\Scienaptic\\HP\\Pricing\\Data\\April8Run_Inputs\\IFS2_most_recent.csv")
    val ifs2DF = spark.read.option("header","true").option("inferSchema","true").csv("/etherData/managedSources/IFS2/IFS2_most_recent.csv")
    var ifs2 = renameColumns(ifs2DF)
    ifs2.columns.toList.foreach(x => {
      ifs2 = ifs2.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    ifs2 = ifs2
      .withColumn("Valid_Start_Date", to_date(unix_timestamp(col("Valid_Start_Date"),"MM/dd/yyyy").cast("timestamp")))
      .withColumn("Valid_End_Date", to_date(unix_timestamp(col("Valid_End_Date"),"MM/dd/yyyy").cast("timestamp")))
    //writeDF(ifs2, "ifs2")
    ifs2.write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/commercialTemp/CommercialFeatEngg/ifs2.csv")

    commercial = commercial
      .withColumnRenamed("Reseller Cluster","Reseller_Cluster")
      .withColumnRenamed("Street Price","Street_Price_Org")
      //TODO: Check for precision in Street_Price as joining based on that too!
      .join(ifs2.dropDuplicates("SKU","Street_Price").select("SKU","Changed_Street_Price","Street_Price", "Valid_Start_Date", "Valid_End_Date"), Seq("SKU"), "left")
      .withColumn("Valid_Start_Date", when(col("Valid_Start_Date").isNull,dat2000_01_01).otherwise(col("Valid_Start_Date")))
      .withColumn("Valid_End_Date", when(col("Valid_End_Date").isNull,dat9999_12_31).otherwise(col("Valid_End_Date")))
      .withColumn("Street_Price", when(col("Street_Price").isNull, col("Street_Price_Org")).otherwise(col("Street_Price")))
      .where((col("Week_End_Date") >= col("Valid_Start_Date")) && (col("Week_End_Date") < col("Valid_End_Date")))
      .drop("Street_Price_Org","Changed_Street_Price","Valid_Start_Date","Valid_End_Date")
      .persist(StorageLevel.MEMORY_AND_DISK)
    //writeDF(commercialMergeifs2DF,"commercialMergeifs2DF")

    
    commercial = commercial.withColumn("Qty",col("Non_Big_Deal_Qty"))
      .withColumn("Special_Programs",lit("None"))
    //TODO: Optimize this: Combine into 1 group and aggregate, avoid 2 groups and then join
    var stockpiler_dealchaser = commercial
      .groupBy("Reseller_Cluster")
      .agg(sum("Qty").alias("Qty_total"))
    //writeDF(stockpiler_dealchaser,"stockpiler_dealchaser")
    val stockpiler_dealchaserPromo = commercial.where(col("IR")>0)
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
    //TODO: Broadcast this value
    val stockpilerResellerList = stockpiler_dealchaser.select("Reseller_Cluster").distinct().collect().map(_ (0).asInstanceOf[String]).toSet
    commercial = commercial
      .withColumn("Reseller_Cluster", when(col("Reseller_Cluster").isin(stockpilerResellerList.toSeq: _*), lit("Stockpiler & Deal Chaser")).otherwise(col("Reseller_Cluster")))
      .withColumn("Reseller_Cluster", when(col("eTailer")===lit(1), concat(lit("eTailer"),col("Reseller_Cluster"))).otherwise(col("Reseller_Cluster")))
        .drop("eTailer")
    //  .withColumn("Reseller_Cluster_LEVELS", col("Reseller_Cluster"))

    val commercialResFacNotPL = commercial.where(!col("PL").isin("E4","E0","ED"))
      .withColumn("Brand",lit("HP"))
        .withColumn("GA date",to_date(col("GA date")))
        .withColumn("ES date",to_date(col("ES date"))).cache()
    //writeDF(commercialResFacNotPL,"commercialHPBeforeGroup")
    var commercialHPDF = commercialResFacNotPL
      .groupBy("SKU","SKU_Name","Reseller_Cluster","Week_End_Date","Season","Street_Price","IPSLES","HPS/OPS","Series","Category","Category Subgroup","Category_1","Category_2","Category_3","Category Custom","Line","PL","L1_Category","L2_Category","PLC Status","GA date","ES date","Inv_Qty","Special_Programs")
      .agg(sum("Qty").as("Qty"), max("IR").as("IR"), sum("Big_Deal_Qty").as("Big_Deal_Qty"), sum("Non_Big_Deal_Qty").as("Non_Big_Deal_Qty"))
      .withColumnRenamed("HPS/OPS","HPS_OPS")
    //writeDF(commercialHPDF,"commercialHPAfterGroup")
    
    val commercialResFacPL = commercial.where(col("PL").isin("E4","E0","ED"))
    //writeDF(commercialResFacPL,"commercialSSBeforeGroup")
    val commercialSSDF = commercialResFacPL
      .groupBy("SKU","SKU_Name","Reseller_Cluster","Week_End_Date","Season","Special_Programs")
      .agg(sum("Qty").as("Qty"), sum("Inv_Qty").as("Inv_Qty"), max("IR").as("IR"), sum("Big_Deal_Qty").as("Big_Deal_Qty"), sum("Non_Big_Deal_Qty").as("Non_Big_Deal_Qty"))
    //writeDF(commercialSSDF,"commercialSSAfterGroup")*/

    //val auxTableDF = spark.read.option("header","true").option("inferSchema","true").csv("E:\\Scienaptic\\HP\\Pricing\\Data\\April8Run_Inputs\\Aux_sku_hierarchy.csv")
    val auxTableDF = spark.read.option("header","true").option("inferSchema","true").csv("/etherData/managedSources/AUX/Aux_sku_hierarchy.csv")
    var auxTable = renameColumns(auxTableDF).cache()
    auxTable.columns.toList.foreach(x => {
      auxTable = auxTable.withColumn(x, when(col(x).cast("string") === "NA" || col(x).cast("string") === "", null).otherwise(col(x)))
    })
    auxTable = auxTable
      .withColumn("GA date",to_date(col("GA date")))
      .withColumn("ES date",to_date(col("ES date")))
    auxTable.persist(StorageLevel.MEMORY_AND_DISK)
    val commercialSSJoinSKUDF = commercialSSDF.join(auxTable, Seq("SKU"), "left")
      .withColumnRenamed("HPS/OPS","HPS_OPS")
      .withColumn("L1_Category",col("L1: Use Case"))
      .withColumn("L2_Category",col("L2: Key functionality"))
      .drop("L1: Use Case","L2: Key functionality","Abbreviated_Name","Platform_Name","Mono/Color","Added","L3: IDC Category","L0: Format","Need_Big_Data","Need_IFS2?","Top_SKU","NPD_Model","NPD_Product_Company","Sales_Product_Name")

    var commercialSSFactorsDF = commercialSSJoinSKUDF
        .drop("L3: IDC Category","L0: Format","Need Big Data","Need IFS2?","Top SKU","NPD Model","NPD_Product_Company","Sales_Product_Name")
    commercialHPDF = commercialHPDF.withColumn("Brand",lit("HP"))
    commercialHPDF.cache()
    //writeDF(commercialHPDF,"commercialHPDF_BEFORE_UNION")
    //writeDF(commercialSSFactorsDF,"commercialSSFactorsDF_BEFORE_UNION")*/

    //val commercialHPandSSDF = /*commercialHPDF.withColumnRenamed("Street_Price","Street Price")*/doUnion(commercialSSFactorsDF, commercialHPDF.withColumnRenamed("Street_Price","Street Price")).ge
    //TODO: Check if , and $ coming in data. Regex is heavy operation.
    commercial = doUnion(commercialSSFactorsDF, commercialHPDF.withColumnRenamed("Street_Price","Street Price")).get
      .withColumn("ES date", to_date(unix_timestamp(col("ES date"),"yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA date", to_date(unix_timestamp(col("GA date"),"yyyy-MM-dd").cast("timestamp")))
      .withColumn("Street Price", (regexp_replace(col("Street Price"),lit("\\$"),lit(""))))
      .withColumn("Street Price", (regexp_replace(col("Street Price"),lit(","),lit(""))).cast("double"))
      .where(col("Street Price") =!= lit(0))
    //writeDF(commercialHPandSSDF,"commercialHPandSSDF_FILTERED_STREET_PRICE")
      commercial = commercial
      .withColumn("VPA", when(col("Reseller_Cluster").isin("CDW","PC Connection","Zones Inc","Insight Direct","PCM","GovConnection"), 1).otherwise(0))
      .withColumn("Promo_Flag", when(col("IR")>0,1).otherwise(0))
      .withColumn("Promo_Pct", when(col("Street Price").isNull,null).otherwise(col("IR")/col("Street Price").cast("double")))
      .withColumn("Discount_Depth_Category", when(col("Promo_Pct")===0, "No Discount").when(col("Promo_Pct")<=0.2, "Very Low").when(col("Promo_Pct")<=0.3, "Low").when(col("Promo_Pct")===0.4, "Moderate").when(col("Promo_Pct")<="0.5", "Heavy").otherwise(lit("Very Heavy")))
      .withColumn("log_Qty", log(col("Qty")))
      .withColumn("log_Qty", when(col("log_Qty").isNull, 0).otherwise(col("log_Qty")))
      .withColumn("price", log(lit(1)-col("Promo_Pct")))
      .withColumn("price", when(col("price").isNull, 0).otherwise(col("price")))
      .withColumn("Inv_Qty_log", log(col("Inv_Qty")))
      .withColumn("Inv_Qty_log", when(col("Inv_Qty_log").isNull, 0).otherwise(col("Inv_Qty_log")))
      .na.fill(0, Seq("log_Qty","price","Inv_Qty_log"))
    //writeDF(commercialHPandSSDF,"commercialHPandSSDF")
    //writeDF(doUnion(commercialSSFactorsDF, commercialHPDF.withColumnRenamed("Street_Price","Street Price")).get,"Commercial_HP_SS_Bind_Rows")

    /*Avik Aprl13 Change: 20190320 fixed cost udpated-update Scienaptic - Start*/
    val IFS2FC = ifs2.where(col("Account")==="Commercial").groupBy("Account","SKU")
      .agg(mean(col("Fixed_Cost")).as("Fixed_Cost")).drop("Account").distinct()
    commercial = commercial.join(IFS2FC, Seq("SKU"), "left")
    /*Avik Aprl13 Change: 20190320 fixed cost udpated-update Scienaptic - End*/

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
    commercial = commercial
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
          commercial = commercial.withColumn(x, when(col(x).isNull, 0).otherwise(col(x)))
      })
      commercial.persist(StorageLevel.MEMORY_AND_DISK)
    //writeDF(commercial,"commercialBeforeNPD")
    commercial.write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/commercialTemp/CommercialFeatEngg/commercialBeforeNPD.csv")
    /* ---------- BREAK HERE ---------- Wants 'commercial' dataframe only */

    //var npdDF = spark.read.option("header","true").option("inferSchema","true").csv("C:\\Users\\avika\\Downloads\\JarCode\\R Code Inputs\\NPD_weekly.csv")
    /*var npdDF = spark.read.option("header","true").option("inferSchema","true").csv("/etherData/managedSources/NPD/NPD_weekly.csv")
    var npd = renameColumns(npdDF)
    npd.columns.toList.foreach(x => {
      npd = npd.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    npd = npd.withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"),"MM/dd/yyyy").cast("timestamp"))).cache()
    //writeDF(npd,"npd")

    /*================= Brand not Main Brands =======================*/
    val npdChannelBrandFilterNotRetail = npd.where((col("Channel") =!= "Retail") && (col("Brand").isin("Canon","Epson","Brother","Lexmark","Samsung")))
      .where((col("DOLLARS")>0) && (col("MSRP__")>0)).cache()
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

    commercial = commercial.join(L1Comp, Seq("L1_Category","Week_End_Date"), "left")
      commercial = commercial
      .join(L2Comp, Seq("L2_Category","Week_End_Date"), "left")
    allBrands.foreach(x => {
      val l1Name = "L1_competition_"+x
      val l2Name = "L2_competition_"+x
        commercial = commercial.withColumn(l1Name, when((col(l1Name).isNull) || (col(l1Name)<0), 0).otherwise(col(l1Name)))
          .withColumn(l2Name, when(col(l2Name).isNull || col(l2Name)<0, 0).otherwise(col(l2Name)))
    })
    //writeDF(commercialWithCompetitionDF,"commercialWithCompetitionDF_AFTER_L1_L2_JOIN")
      commercial= commercial.na.fill(0, Seq("L1_competition_Brother","L1_competition_Canon","L1_competition_Epson","L1_competition_Lexmark","L1_competition_Samsung"))
        .na.fill(0, Seq("L2_competition_Brother","L2_competition_Epson","L2_competition_Canon","L2_competition_Lexmark","L2_competition_Samsung"))
      .repartition(500).cache()
    //writeDF(commercialWithCompetitionDF,"commercialWithCompetitionDF_WITH_L1_L2")
    /*====================================== Brand Not HP =================================*/
    val npdChannelNotRetailBrandNotHP = npd.where((col("Channel")=!="Retail") && (col("Brand")=!="HP"))
      .where((col("DOLLARS")>0) && (col("MSRP__")>0)).cache()
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
      commercial = commercial.join(L1CompetitionNonHP, Seq("L1_Category","Week_End_Date"), "left")
        .join(L2CompetitionNonHP, Seq("L2_Category","Week_End_Date"), "left")
        .withColumn("L1_competition", when((col("L1_competition").isNull) || (col("L1_competition")<0), 0).otherwise(col("L1_competition")))
        .withColumn("L2_competition", when((col("L2_competition").isNull) || (col("L2_competition")<0), 0).otherwise(col("L2_competition")))
        .na.fill(0, Seq("L1_competition","L2_competition"))
    //writeDF(commercialWithCompetitionDF,"commercialWithCompetitionDF_FORNONHP_Competition")
    
    /*=================================== Brand Not Samsung ===================================*/
    val npdChannelNotRetailBrandNotSamsung = npd.where((col("Channel")=!="Retail") && (col("Brand")=!="Samsung"))
      .where((col("DOLLARS")>0) && (col("MSRP__")>0)).cache()
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

      commercial = commercial.join(L1CompetitionSS, Seq("L1_Category","Week_End_Date"), "left")
        .join(L2CompetitionSS, Seq("L2_Category","Week_End_Date"), "left")
      .withColumn("L1_competition_ss", when((col("L1_competition_ss").isNull) || (col("L1_competition_ss")<0), 0).otherwise(col("L1_competition_ss")))
      .withColumn("L2_competition_ss", when((col("L2_competition_ss").isNull) || (col("L2_competition_ss")<0), 0).otherwise(col("L2_competition_ss")))
      .na.fill(0, Seq("L1_competition_ss","L2_competition_ss"))

      commercial = commercial
        .withColumn("L1_competition", when(col("Brand").isin("Samsung"), col("L1_competition_ss")).otherwise(col("L1_competition")))
        .withColumn("L2_competition", when(col("Brand").isin("Samsung"), col("L2_competition_ss")).otherwise(col("L2_competition")))
        .drop("L2_competition_ss","L1_competition_ss")
    //writeDF(commercialWithCompetitionDF,"commercialWithCompetitionDF_Samsung")
    
    /* ========================================================================================== */
    val commercialBrandinHP = commercial.where(col("Brand").isin("HP"))
        .withColumn("Qty_pmax",greatest(col("Qty"),lit(0))).cache()
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
      commercial = commercial.join(HPComp1, Seq("Week_End_Date","L1_Category"), "left")
        .join(HPComp2, Seq("Week_End_Date","L2_Category"), "left")
      .withColumn("L1_competition_HP_ssmodel", when((col("L1_competition_HP_ssmodel").isNull) || (col("L1_competition_HP_ssmodel")<0), 0).otherwise(col("L1_competition_HP_ssmodel")))
      .withColumn("L2_competition_HP_ssmodel", when((col("L2_competition_HP_ssmodel").isNull) || (col("L2_competition_HP_ssmodel")<0), 0).otherwise(col("L2_competition_HP_ssmodel")))
      .na.fill(0, Seq("L1_competition_HP_ssmodel","L2_competition_HP_ssmodel")).cache()
    //writeDF(commercialWithCompetitionDF,"commercialWithCompetitionDF_L1_L2_Competition_SS_Feat")
    /* ---------------- BREAK HERE -------------------------- wants 'commercial' only
      commercial = commercial
        .withColumn("wed_cat", concat_ws(".",col("Week_End_Date"), col("L1_Category")))
    
    val commercialWithCompetitionDFTemp1 = commercial
        .groupBy("wed_cat")
        .agg(sum(col("Promo_Pct")*col("Qty")).as("z"), sum(col("Qty")).as("w"))

      commercial = commercial.join(commercialWithCompetitionDFTemp1, Seq("wed_cat"), "left")
        .withColumn("L1_cannibalization", (col("z")-(col("Promo_Pct")*col("Qty")))/(col("w")-col("Qty")))
        .drop("z","w","wed_cat")
        .withColumn("wed_cat", concat_ws(".", col("Week_End_Date"), col("L2_Category")))

    val commercialWithCompetitionDFTemp2 = commercial
        .groupBy("wed_cat")
        .agg(sum(col("Promo_Pct")*col("Qty")).as("z"), sum(col("Qty")).as("w"))

      commercial = commercial.join(commercialWithCompetitionDFTemp2, Seq("wed_cat"), "left")
        .withColumn("L2_cannibalization", (col("z")-(col("Promo_Pct")*col("Qty")))/(col("w")-col("Qty")))
        .drop("z","w","wed_cat")


//    var commercialWithCompCannDF = commercialWithCompetitionDF

    val commercialWithAdj = commercial.withColumn("Adj_Qty", when(col("Qty")<=0,0).otherwise(col("Qty")))
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

      commercial = commercialGroupWEDL1.withColumn("L2_Category",col("L2_Category")).join(commercialGroupWEDL1Temp2.withColumn("L2_Category",col("L2_Category")), Seq("Week_End_Date","Brand", "L2_Category"), "left")
      .withColumn("L2_cannibalization", (col("sum1")-col("sumSKU1"))/(col("sum2")-col("sumSKU2")))
      .drop("sum1","sum2","sumSKU1","sumSKU2","Adj_Qty")
      .withColumn("L1_cannibalization", when(col("L1_cannibalization").isNull, 0).otherwise(col("L1_cannibalization")))
      .withColumn("L2_cannibalization", when(col("L2_cannibalization").isNull, 0).otherwise(col("L2_cannibalization")))
      .na.fill(0, Seq("L2_cannibalization","L1_cannibalization"))
      .withColumn("Sale_Price", col("Street Price")-col("IR")).persist(StorageLevel.MEMORY_AND_DISK)
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_CANNABILIZATION")
    /* ------------------------- BREAK HERE ------------------------- wants commercial and npd */
    val AverageWeeklySales = commercial
      .groupBy("SKU","Reseller_Cluster","Sale_Price","Street Price")
      .agg(mean(col("Qty")).as("POS_Qty"))

    var npdChannelNotRetail = npd.where(col("Channel")=!="Retail")
      .withColumn("Year", year(col("Week_End_Date")).cast("string"))
//      .withColumn("Year_LEVELS", col("year"))
    npdChannelNotRetail = npdChannelNotRetail
      .withColumn("Week", when(col("Week_End_Date").isNull, null).otherwise(extractWeekFromDateUDF(col("Week_End_Date").cast("string"), lit("yyyy-MM-dd"))))
        .cache()
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

      commercial = commercial
      .withColumn("Week", extractWeekFromDateUDF(col("Week_End_Date").cast("string"), lit("yyyy-MM-dd")))
    //.withColumn("Week_LEVELS",col("Week"))
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_With_WEEK")
    /*  ----------------------- BREAK HERE ------------------- wants seasonalityNPD, ifs2 and commercial  */
    val seasonalityNPDScanner = seasonalityNPD.where(col("L1_Category")==="Office - Personal")
      .withColumn("L1_Category", when(col("L1_Category")==="Office - Personal", "Scanners").otherwise(col("L1_Category")))
    //writeDF(seasonalityNPDScanner,"seasonalityNPDScanner")
    seasonalityNPD = doUnion(seasonalityNPD, seasonalityNPDScanner).get
    //writeDF(seasonalityNPD,"seasonalityNPD")
      commercial = commercial.join(seasonalityNPD, Seq("L1_Category","Week"), "left")
        .drop("Week")
        .withColumn("seasonality_npd2", when((col("USCyberMonday")===lit(1)) || (col("USThanksgivingDay")===lit(1)),0).otherwise(col("seasonality_npd").cast("int")))
        .join(ifs2.where(col("Account")==="Commercial").select("SKU","Hardware_GM","Supplies_GM","Hardware_Rev","Supplies_Rev", "Changed_Street_Price", "Valid_Start_Date", "Valid_End_Date"), Seq("SKU"), "left")
        .withColumn("Valid_Start_Date", when(col("Valid_Start_Date").isNull, dat2000_01_01).otherwise(col("Valid_Start_Date")))
        .withColumn("Valid_End_Date", when(col("Valid_End_Date").isNull, dat9999_12_31).otherwise(col("Valid_End_Date")))
        .where((col("Week_End_Date")>=col("Valid_Start_Date")) && (col("Week_End_Date")<col("Valid_End_Date")))
        .withColumn("Changed_Street_Price", when(col("Changed_Street_Price").isNull, 0).otherwise(col("Changed_Street_Price")))
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_Seasonality")

    val ifs2FilteredAccount = ifs2.where(col("Account").isin("Best Buy","Office Depot-Max","Staples")).cache()
    val ifs2RetailAvg = ifs2FilteredAccount
      .groupBy("SKU")
      .agg(mean("Hardware_GM").as("Hardware_GM_retail_avg"), mean("Hardware_Rev").as("Hardware_Rev_retail_avg"), mean("Supplies_GM").as("Supplies_GM_retail_avg"), mean("Supplies_Rev").as("Supplies_Rev_retail_avg")).cache()
    //writeDF(ifs2RetailAvg,"ifs2RetailAvg_WITH_HARDWARE_SUPPLIES")
      commercial = commercial.drop("Hardware_GM_retail_avg","Supplies_GM_retail_avg","Hardware_Rev_retail_avg","Supplies_Rev_retail_avg")
      .join(ifs2RetailAvg.select("SKU","Hardware_GM_retail_avg","Supplies_GM_retail_avg","Hardware_Rev_retail_avg","Supplies_Rev_retail_avg"), Seq("SKU"), "left")
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_BEFORE_IFS2_JOIN")
      commercial = commercial
      .withColumn("Hardware_GM_type", when(col("Hardware_GM").isNotNull, "Commercial").otherwise(when((col("Hardware_GM").isNull) && (col("Hardware_GM_retail_avg").isNotNull), "Retail").otherwise(lit(null))))
      .withColumn("Hardware_Rev_type", when(col("Hardware_Rev").isNotNull, "Commercial").otherwise(when((col("Hardware_Rev").isNull) && (col("Hardware_Rev_retail_avg").isNotNull), "Retail").otherwise(lit(null))))
      .withColumn("Supplies_GM_type", when(col("Supplies_GM").isNotNull, "Commercial").otherwise(when((col("Supplies_GM").isNull) && (col("Supplies_GM_retail_avg").isNotNull), "Retail").otherwise(lit(null))))
      .withColumn("Supplies_Rev_type", when(col("Supplies_Rev").isNotNull, "Commercial").otherwise(when((col("Supplies_Rev").isNull) && (col("Supplies_Rev_retail_avg").isNotNull), "Retail").otherwise(lit(null))))
      .withColumn("Hardware_GM", when(col("Hardware_GM").isNull, col("Hardware_GM_retail_avg")).otherwise(col("Hardware_GM")))
      .withColumn("Hardware_Rev", when(col("Hardware_Rev").isNull, col("Hardware_Rev_retail_avg")).otherwise(col("Hardware_Rev")))
      .withColumn("Supplies_GM", when(col("Supplies_GM").isNull, col("Supplies_GM_retail_avg")).otherwise(col("Supplies_GM")))
      .withColumn("Supplies_Rev", when(col("Supplies_Rev").isNull, col("Supplies_Rev_retail_avg")).otherwise(col("Supplies_Rev")))
        .cache()
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_WITH_HARDWARE_SUPPLIES_FEAT")*/

    val avgDiscountSKUAccountDF = commercial
      .groupBy("SKU_Name","Reseller_Cluster")
      .agg(sum(col("Qty")*col("IR")).as("Qty_IR"),sum(col("Qty")*col("Street Price").cast("double")).as("QTY_SP"))
      .withColumn("avg_discount_SKU_Account",col("Qty_IR")/col("QTY_SP"))
    //writeDF(avgDiscountSKUAccountDF,"avgDiscountSKUAccountDF")

      commercial = commercial.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      .join(avgDiscountSKUAccountDF.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("SKU_Name","Reseller_Cluster"), "left")
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
      commercial = commercial
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
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_WITH_LOGS")
    */
      commercial.write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/commercialTemp/CommercialFeatEngg/commercialWithCompCannDF.csv")

  }
}