package com.scienaptic.jobs.core.gap

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.Utils.renameColumns
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object GAPTransform1 {
  val Cat_switch=1
  val min_baseline = 2
  val stability_weeks = 4
  val intro_weeks = 6

  val partNumber = (partNumber : String)  => {
    if(partNumber == "J9V91A" || partNumber == "J9V92A" || partNumber == "J9V83A"){
      "J9V80A"
    }else if(partNumber == "M9L74A") {
      "M9L75A"
    }else if(partNumber == "Z3M52A") {
      "K7G93A"
    }else if(partNumber == "5LJ23A" || partNumber == "4KJ65A") {
      "3UC66A"
    }else if(partNumber == "3UK84A") {
      "1KR45A"
    }else{
      partNumber
    }
  }

  val dat2000_01_01 = to_date(unix_timestamp(lit("2000-01-01"),"yyyy-MM-dd").cast("timestamp"))
  val dat9999_12_31 = to_date(unix_timestamp(lit("9999-12-31"),"yyyy-MM-dd").cast("timestamp"))

  val indexerForSpecialPrograms = new StringIndexer().setInputCol("Special_Programs").setOutputCol("Special_Programs_fact")
  val pipelineForSpecialPrograms = new Pipeline().setStages(Array(indexerForSpecialPrograms))

  val indexerForResellerCluster = new StringIndexer().setInputCol("Reseller_Cluster").setOutputCol("Reseller_Cluster_fact")
  val pipelineForResellerCluster= new Pipeline().setStages(Array(indexerForResellerCluster))

  def partNumberUDF = udf(partNumber)

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
    val currentTS = spark.read.json("/etherData/state/currentTS.json").select("ts").head().getString(0)

    var businessPrintersPromoRawExcelDF=renameColumns(spark.read.option("header","true").option("inferSchema","true")
      .csv("/etherData/managedSources/GAP/BusinessPrinters_WEEKLY_Promotions.csv"))
    businessPrintersPromoRawExcelDF.columns.toList.foreach(x => {
      businessPrintersPromoRawExcelDF = businessPrintersPromoRawExcelDF.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    val businessPrintersPromoRawDF=businessPrintersPromoRawExcelDF.na.drop(Seq("Brand"))
      .withColumn("Start Date", to_date(unix_timestamp(col("Start Date"),"MM/dd/yyyy").cast("timestamp")))
      .withColumn("End Date", to_date(unix_timestamp(col("End Date"),"MM/dd/yyyy").cast("timestamp")))
      .withColumn("FileName",lit("BusinessPrinters_WEEKLY"))
      .select("Brand","Product","Part Number","Merchant SKU","Product Type","Start Date","End Date",
      "Promotion Type","Bundle Type","Merchant","FileName", "Value")
      .withColumnRenamed("Merchant","Valid Resellers")

    var personalPrintersPromoRawExcelDF=renameColumns(spark.read.option("header","true").option("inferSchema","true")
      .csv("/etherData/managedSources/GAP/PersonalSOHOPrinters_WEEKLY_Promotions.csv"))
      .withColumn("Start Date", to_date(unix_timestamp(col("Start Date"),"MM/dd/yyyy").cast("timestamp")))
      .withColumn("End Date", to_date(unix_timestamp(col("End Date"),"MM/dd/yyyy").cast("timestamp")))
      .withColumn("Value",regexp_replace(col("Value"),"\\$",""))
      .withColumn("Value",regexp_replace(col("Value"),",","").cast("double"))
    personalPrintersPromoRawExcelDF.columns.toList.foreach(x => {
      personalPrintersPromoRawExcelDF = personalPrintersPromoRawExcelDF.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    val personalPrintersPromoRawDF=personalPrintersPromoRawExcelDF.na.drop(Seq("Brand"))
      .withColumn("Start Date", to_date(unix_timestamp(col("Start Date"),"MM/dd/yyyy").cast("timestamp")))
      .withColumn("End Date", to_date(unix_timestamp(col("End Date"),"MM/dd/yyyy").cast("timestamp")))
      .withColumn("FileName",lit("PersonalSOHOPrinters_WEEKLY"))
      .select("Brand","Product","Part Number","Merchant SKU","Product Type","Start Date","End Date",
        "Promotion Type","Bundle Type","Merchant","FileName", "Value")
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
    // TODO : change to formula
    promo3=promo3.withColumn("Part Number", partNumberUDF(col("Part Number")))

    val GAPInputPromoRawDF = renameColumns(spark.read.option("header","true").option("inferSchema","true")
      .csv("/etherData/managedSources/GAP/gap_input_promo.csv"))
      .withColumn("Start Date", to_date(unix_timestamp(col("Start Date"),"yyyy-MM-dd").cast("timestamp")))
      .withColumn("End Date", to_date(unix_timestamp(col("End Date"),"yyyy-MM-dd").cast("timestamp")))
    var promo11=GAPInputPromoRawDF.join(promo3,GAPInputPromoRawDF("Start Date")===promo3("Start Date"),"leftanti")
    promo3=promo3.select("Brand","Product","Part Number","Merchant SKU","Product Type"
      ,"Start Date","End Date","Promotion Type","Bundle Type","Valid Resellers","FileName", "Value")
    promo11=promo11.select("Brand","Product","Part Number","Merchant SKU","Product Type"
      ,"Start Date","End Date","Promotion Type","Bundle Type","Valid Resellers","FileName", "Value")
    promo3=promo3.union(promo11)
      .select("Brand","Product","Part Number","Merchant SKU","Product Type"
        ,"Start Date","End Date","Promotion Type","Bundle Type","Valid Resellers","FileName", "Value")
    promo3.write.option("header","true").mode(SaveMode.Overwrite)
      .csv("/etherData/Pricing/Outputs/POS_GAP/gap_input_promo_"+currentTS+".csv")


  }
}
