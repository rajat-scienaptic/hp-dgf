package com.scienaptic.jobs.core

import com.scienaptic.jobs.ExecutionContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import com.scienaptic.jobs.bean.UnionOperation.doUnion
import java.util.UUID

import com.scienaptic.jobs.utility.Utils.renameColumns
import com.crealytics.spark.excel._
import org.apache.spark.sql.functions.rank
import com.scienaptic.jobs.utility.CommercialUtility.{addDaystoDateStringUDF, checkPrevQtsGTBaseline, concatenateRank, createlist, extractWeekFromDateUDF}
import com.scienaptic.jobs.utility.Utils._
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.DataType
import sun.text.normalizer.UCharacter

object GAPTransform {
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

    //Only group with mutate will have join back to original dataframe but group with summarize wont have join. summarize gives only 1 row per group.
//    val spark = executionContext.spark
//    val spark = SparkSession.builder
//      .config("spark.executor.heartbeatInterval", "10000s")
//      .getOrCreate()

    val sparkConf = new SparkConf().setAppName("gap")
    val spark = SparkSession.builder
      .master("yarn-client")
      .appName("gap")
      .config(sparkConf)
      .config("spark.executor.heartbeatInterval", "10000s")
      .config("spark.network.timeout", "600s")
      .getOrCreate

    import spark.implicits._
    val currentTS = spark.read.json("/etherData/state/currentTS.json").select("ts").head().getString(0)

    var businessPrintersPromoRawExcelDF=spark.read.excel(useHeader = true,treatEmptyValuesAsNulls = true,
      inferSchema = true,addColorColumns = false,"Promotions!A5",timestampFormat = "MM/dd/yyyy"
      , excerptSize = 100)
      .load("/etherData/managedSources/GAP/BusinessPrinters_WEEKLY.xlsx")
    businessPrintersPromoRawExcelDF.columns.toList.foreach(x => {
      businessPrintersPromoRawExcelDF = businessPrintersPromoRawExcelDF.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    val businessPrintersPromoRawDF=businessPrintersPromoRawExcelDF.na.drop(Seq("Brand"))
      .withColumn("Start Date", to_date(unix_timestamp(col("Start Date"),"MM/dd/yyyy").cast("timestamp")))
      .withColumn("End Date", to_date(unix_timestamp(col("End Date"),"MM/dd/yyyy").cast("timestamp")))
      .withColumn("FileName",lit("BusinessPrinters_WEEKLY"))
      .select("Brand","Product","Part Number","Merchant SKU","Market Segment","Product Type","Start Date","End Date",
      "Promotion Type","Bundle Type","Merchant","Value","Conditions / Notes","On Ad","FileName")
      .withColumnRenamed("Merchant","Valid Resellers")

    var personalPrintersPromoRawExcelDF=spark.read.excel(useHeader = true,treatEmptyValuesAsNulls = true,
      inferSchema = true,addColorColumns = false,"Promotions!A5",timestampFormat = "MM/dd/yyyy"
      , excerptSize = 100
    ).load("/etherData/managedSources/GAP/PersonalSOHOPrinters_WEEKLY.xlsx")
//    var personalPrintersPromoRawExcelDF=renameColumns(spark.read.option("header","true").option("inferSchema","true")
//      .csv("C:\\Users\\HP\\Desktop\\files\\PersonalSOHOPrinters.csv"))
//      .withColumn("Start Date", to_date(unix_timestamp(col("Start Date"),"MM/dd/yyyy").cast("timestamp")))
//      .withColumn("End Date", to_date(unix_timestamp(col("End Date"),"MM/dd/yyyy").cast("timestamp")))
//      .withColumn("Value",regexp_replace(col("Value"),"\\$",""))
//      .withColumn("Value",regexp_replace(col("Value"),",","").cast("double"))
    personalPrintersPromoRawExcelDF.columns.toList.foreach(x => {
      personalPrintersPromoRawExcelDF = personalPrintersPromoRawExcelDF.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    val personalPrintersPromoRawDF=personalPrintersPromoRawExcelDF.na.drop(Seq("Brand"))
      .withColumn("Start Date", to_date(unix_timestamp(col("Start Date"),"MM/dd/yyyy").cast("timestamp")))
      .withColumn("End Date", to_date(unix_timestamp(col("End Date"),"MM/dd/yyyy").cast("timestamp")))
      .withColumn("FileName",lit("PersonalSOHOPrinters_WEEKLY"))
      .select("Brand","Product","Part Number","Merchant SKU","Market Segment","Product Type","Start Date","End Date",
        "Promotion Type","Bundle Type","Merchant","Value","Conditions / Notes","On Ad","FileName")
      .withColumnRenamed("Merchant","Valid Resellers")

    var promo3=businessPrintersPromoRawDF.union(personalPrintersPromoRawDF)
    promo3=promo3.where((col("Promotion Type") === lit("Instant Savings"))
      && (col("Product").isNotNull) && (col("Product") =!= lit(".")) &&
      (lower(col("Part Number")) =!= "select") && (col("Part Number") =!= "na"))
    promo3=promo3.sort(asc("Start Date")
      ,desc("End Date"),asc("Brand")
      ,asc("Valid Resellers"),asc("Part Number"),asc("Merchant SKU"))
    promo3=promo3.dropDuplicates("Brand","Part Number","Start Date","End Date","Valid Resellers")
    val maxdate=promo3.select(col("Start Date")).agg(max("Start Date")).head().getDate(0)
    promo3=promo3.withColumn("Max_Start Date", lit(maxdate))
    promo3 = promo3.withColumn("Max_Start Date_Add91"
      , date_add(col("Max_Start Date").cast("timestamp"), -13*7))
          .where(col("Start Date")>col("Max_Start Date_Add91"))
          .drop("Max_Start Date_Add91")
    promo3=promo3.withColumn("Part Number",when(col("Part Number")==="J9V83A","J9V80A")
      .when(col("Part Number")==="M9L74A","M9L75A").when(col("Part Number")==="J9V91A","J9V90A").when(col("Part Number")==="J9V92A","J9V90A")
    .otherwise(col("Part Number")))

    val GAPInputPromoRawDF = renameColumns(spark.read.option("header","true").option("inferSchema","true")
      .csv("/etherData/managedSources/GAP/gap_input_promo.csv"))
      .withColumn("Start Date", to_date(unix_timestamp(col("Start Date"),"yyyy-MM-dd").cast("timestamp")))
      .withColumn("End Date", to_date(unix_timestamp(col("End Date"),"yyyy-MM-dd").cast("timestamp")))
    var promo11=GAPInputPromoRawDF.join(promo3,GAPInputPromoRawDF("Start Date")===promo3("Start Date"),"leftanti")
    promo3=promo3.select("Brand","Product","Part Number","Merchant SKU","Product Type"
      ,"Start Date","End Date","Promotion Type","Bundle Type","Valid Resellers","Value","Conditions / Notes","On Ad","FileName")
    promo11=promo11.select("Brand","Product","Part Number","Merchant SKU","Product Type"
      ,"Start Date","End Date","Promotion Type","Bundle Type","Valid Resellers","Value","Conditions / Notes","On Ad","FileName")
    promo3=promo3.union(promo11)
      .select("Brand","Product","Part Number","Merchant SKU","Product Type"
        ,"Start Date","End Date","Promotion Type","Bundle Type","Valid Resellers","Value","Conditions / Notes","On Ad","FileName")
    promo3.coalesce(1).write.option("header","true").mode(SaveMode.Overwrite)
      .csv("/etherData/Pricing/Outputs/POS_GAP/gap_input_promo_"+currentTS+".csv")


    var businessPrintersAdRawExcelDF=renameColumns(spark.read.excel(useHeader = true,treatEmptyValuesAsNulls = true,
      inferSchema = true,addColorColumns = false,"'Retail Advertising'!A5",timestampFormat = "MM/dd/yyyy"
      , excerptSize = 100
    ).load("/etherData/managedSources/GAP/BusinessPrinters_WEEKLY.xlsx"))
    businessPrintersAdRawExcelDF.columns.toList.foreach(x => {
      businessPrintersAdRawExcelDF = businessPrintersAdRawExcelDF.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    val businessPrintersAdRawDF=businessPrintersAdRawExcelDF.na.drop(Seq("Brand"))
      .withColumn("Ad Date", to_date(unix_timestamp(col("Ad Date"),"MM/dd/yyyy").cast("timestamp")))
      .withColumn("End Date", to_date(unix_timestamp(col("End Date"),"MM/dd/yyyy").cast("timestamp")))
      .withColumn("FileName",lit("BusinessPrinters_WEEKLY"))
      .select("Merchant","Brand","Product","Part Number","Product Type"
        ,"Shelf Price When Advertised","Advertised Price"
        ,"Ad Date","End Date","Promotion Type","Bundle Type","Instant Savings","Mail-in Rebate","Price Drop","Bundle","Peripheral"
        ,"Free Gift","Merchant Gift Card","Merchant Rewards","Recycling","Misc_","Total Value","Details","Ad Location","Ad Name"
        ,"Page Number","Region","Print Verified","Online Verified","gap URL","FileName")

    var personalPrintersAdRawExcelDF=renameColumns(spark.read.excel(useHeader = true,treatEmptyValuesAsNulls = true,
      inferSchema = true,addColorColumns = false,"'Retail Advertising'!A5",timestampFormat = "MM/dd/yyyy"
      , excerptSize = 100
    ).load("/etherData/managedSources/GAP/PersonalSOHOPrinters_WEEKLY.xlsx"))
//var personalPrintersAdRawExcelDF=renameColumns(spark.read.option("header","true").option("inferSchema","true")
//  .csv("/etherData/managedSources/GAP/PersonalSOHOPrinters2.csv"))
//  .withColumn("Ad Date", to_date(unix_timestamp(col("Ad Date"),"MM/dd/yyyy").cast("timestamp")))
//  .withColumn("End Date", to_date(unix_timestamp(col("End Date"),"MM/dd/yyyy").cast("timestamp")))
//  .withColumn("Shelf Price When Advertised",regexp_replace(col("Shelf Price When Advertised"),"\\$",""))
//  .withColumn("Shelf Price When Advertised",regexp_replace(col("Shelf Price When Advertised"),",","").cast("double"))
//  .withColumn("Advertised Price",regexp_replace(col("Advertised Price"),"\\$",""))
//  .withColumn("Advertised Price",regexp_replace(col("Advertised Price"),",","").cast("double"))
//  .withColumn("Instant Savings",regexp_replace(col("Instant Savings"),"\\$",""))
//  .withColumn("Instant Savings",regexp_replace(col("Instant Savings"),",","").cast("double"))
//  .withColumn("Free Gift",regexp_replace(col("Free Gift"),"\\$",""))
//  .withColumn("Free Gift",regexp_replace(col("Free Gift"),",","").cast("double"))
//  .withColumn("Merchant Gift Card",regexp_replace(col("Merchant Gift Card"),"\\$",""))
//  .withColumn("Merchant Gift Card",regexp_replace(col("Merchant Gift Card"),",","").cast("double"))
//  .withColumn("Merchant Rewards",regexp_replace(col("Merchant Rewards"),"\\$",""))
//  .withColumn("Merchant Rewards",regexp_replace(col("Merchant Rewards"),",","").cast("double"))
//  .withColumn("Recycling",regexp_replace(col("Recycling"),"\\$",""))
//  .withColumn("Recycling",regexp_replace(col("Recycling"),",","").cast("double"))
//  .withColumn("Misc_",regexp_replace(col("Misc_"),"\\$",""))
//  .withColumn("Misc_",regexp_replace(col("Misc_"),",","").cast("double"))
//  .withColumn("Total Value",regexp_replace(col("Total Value"),"\\$",""))
//  .withColumn("Total Value",regexp_replace(col("Total Value"),",","").cast("double"))
    personalPrintersAdRawExcelDF.columns.toList.foreach(x => {
      personalPrintersAdRawExcelDF = personalPrintersAdRawExcelDF.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    val personalPrintersAdRawDF=personalPrintersAdRawExcelDF.na.drop(Seq("Brand"))
      .withColumn("Ad Date", to_date(unix_timestamp(col("Ad Date"),"MM/dd/yyyy").cast("timestamp")))
      .withColumn("End Date", to_date(unix_timestamp(col("End Date"),"MM/dd/yyyy").cast("timestamp")))
      .withColumn("FileName",lit("PersonalSOHOPrinters_WEEKLY"))
      .select("Merchant","Brand","Product","Part Number","Product Type"
        ,"Shelf Price When Advertised","Advertised Price"
        ,"Ad Date","End Date","Promotion Type","Bundle Type","Instant Savings","Mail-in Rebate","Price Drop","Bundle","Peripheral"
        ,"Free Gift","Merchant Gift Card","Merchant Rewards","Recycling","Misc_","Total Value","Details","Ad Location","Ad Name"
        ,"Page Number","Region","Print Verified","Online Verified","gap URL","FileName")

    var ad3=businessPrintersAdRawDF.union(personalPrintersAdRawDF)
    ad3=ad3.where((col("Product").isNotNull) && (col("Product") =!= ".")
      && (lower(col("Part Number")) =!= "select"))
    val maxaddate=ad3.select(col("Ad Date")).agg(max("Ad Date")).head().getDate(0)
    ad3=ad3.withColumn("Max_Ad Date", lit(maxaddate))
    ad3 = ad3.withColumn("Max_Ad Date_Add91"
      , date_add(col("Max_Ad Date").cast("timestamp"), -12*7))
          .where(col("Ad Date")>col("Max_Ad Date_Add91"))
          .drop("Max_Ad Date_Add91")
    ad3=ad3.withColumn("Part Number",when(col("Part Number")==="J9V83A","J9V80A")
      .when(col("Part Number")==="M9L74A","M9L75A")
      .when(col("Part Number")==="J9V91A","J9V90A")
      .when(col("Part Number")==="J9V92A","J9V90A")
    .otherwise(col("Part Number")))

    val GAPInputAdRawDF = renameColumns(spark.read.option("header","true").option("inferSchema","true")
      .csv("/etherData/managedSources/GAP/gap_input_ad.csv"))
      .withColumn("Ad Date", to_date(unix_timestamp(col("Ad Date"),"dd-MM-yyyy").cast("timestamp")))
      .withColumn("End Date", to_date(unix_timestamp(col("End Date"),"dd-MM-yyyy").cast("timestamp")))

    val ad11=GAPInputAdRawDF.join(ad3,GAPInputAdRawDF("Ad Date")===ad3("Ad Date"),"leftanti")
    ad3=doUnion(ad11,ad3).get
      .select("Merchant","Brand","Product","Part Number","Shelf Price When Advertised","Advertised Price"
      ,"Ad Date","End Date","Promotion Type","Bundle Type","Instant Savings","Mail-in Rebate","Price Drop","Bundle","Peripheral"
    ,"Free Gift","Merchant Gift Card","Merchant Rewards","Recycling","Misc_","Total Value","Details","Ad Location","Ad Name"
    ,"Page Number","Region","Print Verified","Online Verified","gap URL","FileName")

    ad3.coalesce(1).write.option("header","true").mode(SaveMode.Overwrite)
      .csv("/etherData/Pricing/Outputs/POS_GAP/gap_input_ad_"+currentTS+".csv")



//    var promo3 = renameColumns(spark.read.option("header","true").option("inferSchema","true")
//      .csv("C:\\Users\\HP\\Desktop\\files\\gap_input_promo.csv"))
//      .withColumn("Start Date", to_date(unix_timestamp(col("Start Date"),"yyyy-MM-dd").cast("timestamp")))
//      .withColumn("End Date", to_date(unix_timestamp(col("End Date"),"yyyy-MM-dd").cast("timestamp")))

    promo3=promo3.withColumn("Ad_Promo",when(col("On Ad")==="Y",1).otherwise(0))
      .withColumn("On Ad",lit(""))
      .withColumn("Promo_Start_Date",col("Start Date"))
      .withColumn("Promo_End_Date",col("End Date"))
      .drop("Start Date","End Date")
      .where(col("Promo_End_Date")>=to_date(unix_timestamp(lit("01-01-2014"),"dd-MM-yyyy").cast("timestamp")))
      .na.drop(Seq("Value"))
      .dropDuplicates()
      .withColumn("Promo_Start_Date_wday",dayofweek(col("Promo_Start_Date")).cast(IntegerType))
      .withColumn("Promo_End_Date_wday",dayofweek(col("Promo_End_Date")).cast(IntegerType))
      .withColumn("Adj_Promo_Start_Date"
        ,to_date(unix_timestamp(expr("date_sub(Promo_Start_Date,Promo_Start_Date_wday-1)"),"yyyy-MM-dd").cast("timestamp")))
      .withColumn("Adj_Promo_End_Date"
        ,to_date(unix_timestamp(expr("date_add(Promo_End_Date,7-Promo_End_Date_wday)"),"yyyy-MM-dd").cast("timestamp")))
      .withColumn("Promo_Length",datediff(col("Promo_End_Date"),col("Promo_Start_Date"))+1)
      .withColumn("Adj_Promo_Length",
        datediff(col("Adj_Promo_End_Date"),col("Adj_Promo_Start_Date"))+1)
      .withColumn("Weeks",(col("Adj_Promo_Length")/7).cast("int"))
      .withColumn("Weeks",
        when(col("Weeks").isNull || col("Weeks")<=0, 0).otherwise(col("Weeks")))
      .withColumn("repList", createlist(col("Weeks")))

    promo3=promo3
      .withColumn("add", (explode(col("repList")))).drop("repList")
          .withColumn("add",col("add")+lit(1))

    promo3=promo3
      .withColumn("add", col("add")*lit(7))
      .withColumn("Week_End_Date"
        ,to_date(unix_timestamp(expr("date_add(Adj_Promo_Start_Date,add-1)"),"dd-MM-yyyy").cast("timestamp")))
      .withColumnRenamed("Part Number","SKU")
      .withColumnRenamed("Valid Resellers","Account")
      .withColumnRenamed("Promotion Type","Promotion_Type_Promo")
      .withColumnRenamed("Value","Value_Promo")
      .select("Account", "SKU","Brand","Product","Promotion_Type_Promo","Value_Promo","Ad_Promo",
         "Promo_Start_Date","Promo_End_Date","Week_End_Date")
      .withColumn("Days_on_Promo_Start",
        datediff(col("Week_End_Date"),col("Promo_Start_Date"))+lit(1))
      .withColumn("Days_on_Promo_Start",when(col("Days_on_Promo_Start")>lit(6),lit(7))
        .otherwise(col("Days_on_Promo_Start")))
      .withColumn("Days_on_Promo_End",lit(7)-datediff(col("Week_End_Date"),col("Promo_End_Date")))
      .withColumn("Days_on_Promo_End",when(col("Days_on_Promo_End")>lit(6),lit(7))
        .otherwise(col("Days_on_Promo_End")))
      .withColumn("Days_on_Promo",least("Days_on_Promo_End","Days_on_Promo_Start"))
      .select("Account", "SKU","Brand","Product","Promotion_Type_Promo","Value_Promo","Ad_Promo",
        "Days_on_Promo","Week_End_Date")

//    promo13.coalesce(1).write.option("header","true").mode(SaveMode.Overwrite)
//          .csv("src/main/scala/com/scienaptic/jobs/core/output/promo13.csv")

//    var ad3 = renameColumns(spark.read.option("header","true").option("inferSchema","true")
//      .csv("C:\\Users\\HP\\Desktop\\files\\gap_input_ad.csv"))
//      .withColumn("Ad Date", to_date(unix_timestamp(col("Ad Date"),"yyyy-MM-dd").cast("timestamp")))
//      .withColumn("End Date", to_date(unix_timestamp(col("End Date"),"yyyy-MM-dd").cast("timestamp")))

    ad3=ad3.withColumn("Ad",lit(1).cast("int"))
      .withColumn("Ad_Start_Date"
        ,to_date(unix_timestamp(col("Ad Date"),"dd-MM-yyyy").cast("timestamp")))
      .withColumn("Ad_End_Date"
        ,to_date(unix_timestamp(col("End Date"),"dd-MM-yyyy").cast("timestamp")))
        .drop("Ad Date","End Date")
      .where(col("Ad_End_Date")>=to_date(unix_timestamp(lit("01-01-2014"),"dd-MM-yyyy").cast("timestamp")))
      .withColumn("Instant Savings",when(col("Instant Savings").isNull,lit(0))
        .otherwise(col("Instant Savings")))
      .dropDuplicates()
      .withColumn("Ad_Start_Date_wday",dayofweek(col("Ad_Start_Date")).cast(IntegerType))
      .withColumn("Ad_End_Date_wday",dayofweek(col("Ad_End_Date")).cast(IntegerType))
      .withColumn("Adj_Ad_Start_Date"
        ,to_date(unix_timestamp(expr("date_sub(Ad_Start_Date,Ad_Start_Date_wday-1)"),"dd-MM-yyyy").cast("timestamp")))
      .withColumn("Adj_Ad_End_Date"
        ,to_date(unix_timestamp(expr("date_add(Ad_End_Date,7-Ad_End_Date_wday)"),"dd-MM-yyyy").cast("timestamp")))
      .withColumn("Ad_Length",datediff(col("Ad_End_Date"),col("Ad_Start_Date"))+1)
      .withColumn("Adj_Ad_Length",datediff(col("Adj_Ad_End_Date"),col("Adj_Ad_Start_Date"))+1)
      .withColumn("Weeks",(col("Adj_Ad_Length")/7).cast("int"))
      .withColumn("Weeks",
        when(col("Weeks").isNull || col("Weeks")<=0, 0).otherwise(col("Weeks")))
      .withColumn("repList", createlist(col("Weeks")))
      .withColumn("add", (explode(col("repList")))).drop("repList")
          .withColumn("add",col("add")+lit(1))
      .withColumn("add", col("add")*lit(7))
      .withColumn("Week_End_Date"
        ,to_date(unix_timestamp(expr("date_add(Adj_Ad_Start_Date,add-1)"),"dd-MM-yyyy").cast("timestamp")))
      .withColumnRenamed("Part Number","SKU")
      .withColumnRenamed("Merchant","Account")
      .withColumnRenamed("Promotion Type","Promotion_Type_Ad")
      .withColumnRenamed("Instant Savings","Value_Ad")
      .select("Account", "SKU","Brand","Product","Promotion_Type_Ad","Value_Ad","Ad Location",
        "Page Number","Print Verified","Online Verified","Ad","Week_End_Date")
      .withColumn("Print Verified",when(col("Print Verified")==="Y",1).otherwise(0))
      .withColumn("Online Verified",when(col("Online Verified")==="Y",1).otherwise(0))
      .withColumn("Page Number",when(col("Page Number").isNull,lit("NA"))
        .otherwise(col("Page Number")))

//    ad13.coalesce(1).write.option("header","true").mode(SaveMode.Overwrite)
//      .csv("src/main/scala/com/scienaptic/jobs/core/output/ad13.csv")

/*
    val promo13 = renameColumns(spark.read.option("header","true").option("inferSchema","true")
      //.csv("/etherData/ManagedSources/GAP/gap_input_promo.csv"))
      .csv("src/main/scala/com/scienaptic/jobs/core/promo13.csv"))
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"),"dd-MM-yyyy").cast("timestamp")))

    val ad13 = renameColumns(spark.read.option("header","true").option("inferSchema","true")
      //.csv("/etherData/ManagedSources/GAP/gap_input_promo.csv"))
      .csv("src/main/scala/com/scienaptic/jobs/core/ad13.csv"))
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"),"dd-MM-yyyy").cast("timestamp")))
*/
    var Promo_Ad=promo3.join(ad3,Seq("SKU","Brand","Product","Account","Week_End_Date"),"fullouter")
    Promo_Ad=Promo_Ad.withColumn("Promotion_Type",when(col("Promotion_Type_Promo").isNull
      ,col("Promotion_Type_Ad")).otherwise(col("Promotion_Type_Promo")))
      .withColumn("Promotion_Type_Promo",lit(""))
      .withColumn("Promotion_Type_Ad",lit(""))
      .withColumn("Ad",when(col("Ad_Promo")===lit(1) or col("Ad")=== lit(1),lit(1))
      .otherwise(lit(0)))
      .withColumn("Ad_Promo",lit(""))
//    Promo_Ad.coalesce(1).write.option("header","true").mode(SaveMode.Overwrite)
//            .csv("src/main/scala/com/scienaptic/jobs/core/output/Promo_Ad3.csv")
    Promo_Ad=Promo_Ad
      .withColumn("Value_Promo",when(col("Value_Promo").isNull,lit(0))
        .otherwise(col("Value_Promo")))
      .withColumn("Value_Ad",when(col("Value_Ad").isNull,lit(0))
        .otherwise(col("Value_Ad")))
      .withColumn("Total_IR",when(col("Value_Promo")>col("Value_Ad"),col("Value_Promo"))
        .otherwise(col("Value_Ad")))
      .withColumn("Value_Promo",lit(""))
      .withColumn("Value_Ad",lit(""))

    Promo_Ad=Promo_Ad
      .where(col("Account") isin("Best Buy", "BestBuy.com", "Office Depot","OfficeDepot.com", "OfficeMax", "OfficeMax.com",
      "Staples", "Staples.com", "Costco", "Costco.com","Sams Club", "Samsclub.com", "SamsClub.com",
      "BJs Wholesale Club", "BJS Wholesale Club","BJs.com", "Walmart", "WalMart", "Walmart.com","WalMart.com"))
      .withColumn("Online",when(col("Account") isin("BestBuy.com", "OfficeDepot.com", "OfficeMax.com",
        "Staples.com", "Costco.com", "Samsclub.com","SamsClub.com", "BJs.com", "Walmart.com","WalMart.com"),lit(1))
      .otherwise(lit(0)))
//    Promo_Ad2.coalesce(1).write.option("header","true").mode(SaveMode.Overwrite)
//      .csv("src/main/scala/com/scienaptic/jobs/core/output/promo5.csv")

    Promo_Ad=Promo_Ad
      .withColumn("Account",when(col("Account")===lit("BestBuy.com"),"Best Buy").otherwise(col("Account")))
      .withColumn("Account",when(col("Account")===lit("OfficeDepot.com"),"Office Depot").otherwise(col("Account")))
      .withColumn("Account",when(col("Account")===lit("OfficeMax.com"),"OfficeMax").otherwise(col("Account")))
      .withColumn("Account",when(col("Account")===lit("Staples.com"),"Staples").otherwise(col("Account")))
      .withColumn("Account",when(col("Account")===lit("Costco.com"),"Costco").otherwise(col("Account")))
    Promo_Ad=Promo_Ad
      .withColumn("Account",when(col("Account")===lit("Samsclub.com"),"Sams Club").otherwise(col("Account")))
      .withColumn("Account",when(col("Account")===lit("SamsClub.com"),"Sams Club").otherwise(col("Account")))
      .withColumn("Account",when(col("Account")===lit("BJs.com"),"BJs Wholesale Club").otherwise(col("Account")))
      .withColumn("Account",when(col("Account")===lit("Walmart.com"),"Walmart").otherwise(col("Account")))
    Promo_Ad=Promo_Ad
      .withColumn("Account",when(col("Account")===lit("WalMart.com"),"WalMart").otherwise(col("Account")))
      .withColumn("Account",when(col("Account")===lit("WalMart"),"Walmart").otherwise(col("Account")))
      .withColumn("Account",when(col("Account")===lit("BJS Wholesale Club"),"BJs Wholesale Club").otherwise(col("Account")))
      .withColumn("Account",when(col("Account")===lit("BestBuy.com"),"Best Buy").otherwise(col("Account")))
      .withColumn("Account",when(col("Account")===lit("Sams Club"),"Sam's Club").otherwise(col("Account")))
      .withColumn("Account",when(col("Account")===lit("Office Depot"),"Office Depot-Max").otherwise(col("Account")))
      .withColumn("Account",when(col("Account")===lit("OfficeMax"),"Office Depot-Max").otherwise(col("Account")))
      .select("SKU","Brand","Account","Online","Week_End_Date"
        ,"Total_IR","Ad","Promotion_Type","Ad Location","Product","Days_on_Promo")

//        val windowSpec=Window.partitionBy("SKU","Brand","Account","Online","Week_End_Date")
//      .orderBy("Week_End_Date","SKU","Brand","Account","Online","Total_IR")
//    val GAP=Promo_Ad
//      .withColumn("Ad",max("Ad").over(windowSpec))
//      .withColumn("Total_IR",max("Total_IR").over(windowSpec))
//      .withColumn("Promotion_Type",last(col("Promotion_Type")).over(windowSpec))
//      .withColumn("Ad Location",first("Ad Location").over(windowSpec))
//      .withColumn("Product",first("Product").over(windowSpec))

    val GAP=Promo_Ad.orderBy("Week_End_Date","SKU","Brand","Account","Online","Total_IR")
      .groupBy("SKU","Brand","Account","Online","Week_End_Date")
    .agg(max("Ad").alias("Ad"),max("Total_IR").alias("Total_IR")
      ,max("Days_on_Promo").alias("Days_on_Promo")
      ,last("Promotion_Type").alias("Promotion_Type")
      ,first("Ad Location").alias("Ad Location"),first("Product").alias("Product"))

    val GAPCOMP=GAP.where(col("Brand") isin("Brother","Canon","Xerox","Samsung","Lexmark","Epson"))
      .withColumn("Product",lower(col("Product")))
    val GAPNPD_matching = renameColumns(spark.read.option("header","true").option("inferSchema","true")
      .csv("/etherData/managedSources/GAP/GAP NPD comp matching.csv"))
      .withColumn("Product",lower(col("Product")))
      .withColumnRenamed("SKU","Right_SKU")

    val GAPNPD=GAPCOMP.join(GAPNPD_matching,Seq("Brand","Product"),"inner")
      .drop(col("Market_Category")).drop("Right_SKU")
    val GAPHP=GAP.where(col("Brand") isin("HP"))

    val sku_hierarchy=renameColumns(spark.read.excel(useHeader = true,treatEmptyValuesAsNulls = true,
      inferSchema = true,addColorColumns = false,"sku_hierarchy!A1",timestampFormat = "dd-MM-yyyy"
      , excerptSize = 100
    ).load("/etherData/managedSources/AUX/Aux Tables.xlsx"))
      .withColumn("L1_Category",col("`L1: Use Case`"))
      .withColumn("L2_Category",col("`L2: Key functionality`"))
      .select("SKU","Category_1","Category_2","L1_Category","L2_Category")

    val HPSKU=GAPHP.join(sku_hierarchy,Seq("SKU"),"inner")
    val finalmerge=doUnion(GAPNPD,HPSKU).get
      .withColumnRenamed("Ad Location","Ad_Location")
        .select("Brand","Product","SKU","Account","Online","Week_End_Date","Ad"
        ,"Total_IR","Days_on_Promo","Promotion_Type","Ad_Location","L2_Category","L1_Category"
          ,"Category_2","Category_1")
    finalmerge.coalesce(1).write.option("header","true").mode(SaveMode.Overwrite)
      .csv("/etherData/Pricing/Outputs/POS_GAP/gap_data_full_"+currentTS+".csv")


  }
}