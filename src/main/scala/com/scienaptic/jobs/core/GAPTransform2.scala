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

object GAPTransform2 {
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

    var businessPrintersAdRawExcelDF=renameColumns(spark.read.option("header","true").option("inferSchema","true")
      .csv("/etherData/managedSources/GAP/BusinessPrinters_WEEKLY_Retail_Advertising.csv"))
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

var personalPrintersAdRawExcelDF=renameColumns(spark.read.option("header","true").option("inferSchema","true")
  .csv("/etherData/managedSources/GAP/PersonalSOHOPrinters_WEEKLY_Retail_Advertising.csv"))
  .withColumn("Ad Date", to_date(unix_timestamp(col("Ad Date"),"MM/dd/yyyy").cast("timestamp")))
  .withColumn("End Date", to_date(unix_timestamp(col("End Date"),"MM/dd/yyyy").cast("timestamp")))
  .withColumn("Shelf Price When Advertised",regexp_replace(col("Shelf Price When Advertised"),"\\$",""))
  .withColumn("Shelf Price When Advertised",regexp_replace(col("Shelf Price When Advertised"),",","").cast("double"))
  .withColumn("Advertised Price",regexp_replace(col("Advertised Price"),"\\$",""))
  .withColumn("Advertised Price",regexp_replace(col("Advertised Price"),",","").cast("double"))
  .withColumn("Instant Savings",regexp_replace(col("Instant Savings"),"\\$",""))
  .withColumn("Instant Savings",regexp_replace(col("Instant Savings"),",","").cast("double"))
  .withColumn("Free Gift",regexp_replace(col("Free Gift"),"\\$",""))
  .withColumn("Free Gift",regexp_replace(col("Free Gift"),",","").cast("double"))
  .withColumn("Merchant Gift Card",regexp_replace(col("Merchant Gift Card"),"\\$",""))
  .withColumn("Merchant Gift Card",regexp_replace(col("Merchant Gift Card"),",","").cast("double"))
  .withColumn("Merchant Rewards",regexp_replace(col("Merchant Rewards"),"\\$",""))
  .withColumn("Merchant Rewards",regexp_replace(col("Merchant Rewards"),",","").cast("double"))
  .withColumn("Recycling",regexp_replace(col("Recycling"),"\\$",""))
  .withColumn("Recycling",regexp_replace(col("Recycling"),",","").cast("double"))
  .withColumn("Misc_",regexp_replace(col("Misc_"),"\\$",""))
  .withColumn("Misc_",regexp_replace(col("Misc_"),",","").cast("double"))
  .withColumn("Total Value",regexp_replace(col("Total Value"),"\\$",""))
  .withColumn("Total Value",regexp_replace(col("Total Value"),",","").cast("double"))
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

    ad3.write.option("header","true").mode(SaveMode.Overwrite)
      .csv("/etherData/Pricing/Outputs/POS_GAP/gap_input_ad_"+currentTS+".csv")

  }
}