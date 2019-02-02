package com.scienaptic.jobs.core

import com.scienaptic.jobs.ExecutionContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import com.scienaptic.jobs.bean.UnionOperation.doUnion
import java.util.UUID
import com.scienaptic.jobs.utility.Utils.renameColumns

import org.apache.spark.sql.functions.rank
import com.scienaptic.jobs.utility.CommercialUtility.{addDaystoDateStringUDF, checkPrevQtsGTBaseline, concatenateRank, createlist, extractWeekFromDateUDF}
import org.apache.spark.sql.expressions.Window

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
    val sparkConf = new SparkConf().setAppName("Test")
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Retail-R")
      .config(sparkConf)
      .getOrCreate

    import spark.implicits._

val BusinessPrintersRawDF = renameColumns(spark.read.option("header","true").option("inferSchema","true")
.csv("/etherData/ManagedSources/GAP/BusinessPrinters_WEEKLY_promotions.csv"))
        .withColumn("Start_Date", to_date(unix_timestamp(col("Start_Date"),"dd-MM-yyyy").cast("timestamp")))
      .withColumn("End_Date", to_date(unix_timestamp(col("End_Date"),"dd-MM-yyyy").cast("timestamp")))  

val PersonalPrintersRawDF = renameColumns(spark.read.option("header","true").option("inferSchema","true")
.csv("/etherData/ManagedSources/GAP/PersonalSOHOPrinters_WEEKLY_promotions.csv"))
        .withColumn("Start_Date", to_date(unix_timestamp(col("Start_Date"),"dd-MM-yyyy").cast("timestamp")))
      .withColumn("End_Date", to_date(unix_timestamp(col("End_Date"),"dd-MM-yyyy").cast("timestamp")))  


val promo1=BusinessPrintersRawDF.select("Brand","Product","Part_Number","Merchant_SKU","Market_Segment","Product_Type","Start_Date","End_Date",
"Promotion_Type","Bundle_Type","Merchant","Value","Conditions_Notes","On_Ad","FileName")
.withColumnRenamed("Merchant","Valid_Resellers")

val promo2=PersonalPrintersRawDF.select("Brand","Product","Part_Number","Merchant_SKU","Market_Segment","Product_Type","Start_Date","End_Date",
"Promotion_Type","Bundle_Type","Merchant","Value","Conditions_Notes","On_Ad","FileName")
.withColumnRenamed("Merchant","Valid_Resellers")

val promo3=promo1.union(promo2)
val promo4=promo3.where((col("Promotion_Type") === "Instant_Savings") or (col("Product").isNotNull) or (col("Product") =!= ".") or 
  (col("Part_Number") =!= "Select") or (col("Part_Number") =!= "na"))

val promo5=promo4.sort(asc("Start_Date"),desc("End_Date"),asc("Brand"),asc("Valid_Resellers"),asc("Part_Number"),asc("Merchant_SKU"))
val promo6=promo5.dropDuplicates("Brand","Part_Number","Start_Date","End_Date","Valid_Resellers")
val maxdate=promo6.select(col("Start_Date").cast("string")).agg(max("Start_Date")).head().getString(0)
val maxDateObj = to_date(unix_timestamp(lit(maxdate),"yyyy-MM-dd").cast("timestamp"))
val promo7=promo6.withColumn("Max_Start_Date", lit(maxDateObj))

val promo8 = promo7.withColumn("Max_Start_Date_Add91", date_add(col("Max_Start_Date").cast("timestamp"), -13*7))
      .where(col("Start_Date")>col("Max_Start_Date_Add91"))
      .drop("Max_Start_Date_Add91")
val promo9=promo8.withColumn("Part_Number",when(col("Part_Number")==="J9V83A","J9V80A")
  .when(col("Part_Number")==="M9L74A","M9L75A").when(col("Part_Number")==="J9V91A","J9V90A").when(col("Part_Number")==="J9V92A","J9V90A")
.otherwise(col("Part_Number")))

val promo10=promo9.withColumnRenamed("Brand","Right_Brand").withColumnRenamed("Product","Right_Product")
.withColumnRenamed("Part_Number","Right_Part_Number").withColumnRenamed("Merchant_SKU","Right_Merchant_SKU")
.withColumnRenamed("Market_Segment","Right_Market_Segment").withColumnRenamed("Product_Type","Right_Product_Type")
.withColumnRenamed("Start_Date","Right_Start_Date").withColumnRenamed("End_Date","Right_End_Date")
.withColumnRenamed("Promotion_Type","Right_Promotion_Type").withColumnRenamed("Bundle_Type","Right_Bundle_Type")
.withColumnRenamed("Valid_Resellers","Right_Valid_Resellers").withColumnRenamed("Value","Right_Value")
.withColumnRenamed("Conditions_Notes","Right_Conditions_Notes").withColumnRenamed("On_Ad","Right_On_Ad")
.withColumnRenamed("FileName","Right_FileName")

val GAPInputPromoRawDF = renameColumns(spark.read.option("header","true").option("inferSchema","true")
.csv("/etherData/ManagedSources/GAP/gap_input_promo.csv"))
// val promo11=GAPInputPromoRawDF.join(promo10, Seq("Start_Date"), "left")
val promo11=GAPInputPromoRawDF.join(promo10,GAPInputPromoRawDF("Start_Date")===promo10("Right_Start_Date"),"left")

val promo12=doUnion(promo11,promo9).get.select("Brand","Product","Part_Number","Merchant_SKU","Market_Segment","Product_Type"
  ,"Start_Date","End_Date","Promotion_Type","Bundle_Type","Valid_Resellers","Value","Conditions_Notes","On_Ad","FileName")
promo12.write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/Pricing/Output/POS_GAP/gap_input_promo.csv")


// .withColumn("Ad_Promo",when(col("On_Ad")==="Y",1).otherwise(0))
// .withColumn("On_Ad",null)
// .withColumn("Promo_Start_Date","Start_Date")
// .withColumn("Promo_End_Date","End_Date")
// .withColumn("Start_Date",null)
// .withColumn("End_Date",null)
// .where(col("Promo_End_Date")>=lit("2014-01-01").cast(DateType) or col("Value").isNotNull)


val BusinessPrintersAdRawDF = renameColumns(spark.read.option("header","true").option("inferSchema","true")
.csv("/etherData/ManagedSources/GAP/BusinessPrinters_WEEKLY_ads.csv"))
        .withColumn("Ad_Date", to_date(unix_timestamp(col("Ad_Date"),"dd-MM-yyyy").cast("timestamp")))
      .withColumn("End_Date", to_date(unix_timestamp(col("End_Date"),"dd-MM-yyyy").cast("timestamp")))  


val PersonalPrintersAdRawDF = renameColumns(spark.read.option("header","true").option("inferSchema","true")
.csv("/etherData/ManagedSources/GAP/PersonalSOHOPrinters_WEEKLY_ads.csv"))
        .withColumn("Ad_Date", to_date(unix_timestamp(col("Ad_Date"),"dd-MM-yyyy").cast("timestamp")))
      .withColumn("End_Date", to_date(unix_timestamp(col("End_Date"),"dd-MM-yyyy").cast("timestamp")))  


val ad1=BusinessPrintersAdRawDF.select("Merchant","Brand","Product","Part_Number","Product_Type"
  ,"Shelf_Price_When_Advertised","Advertised_Price"
  ,"Ad_Date","End_Date","Promotion_Type","Bundle_Type","Instant_Savings","Mail-in_Rebate","Price_Drop","Bundle","Peripheral"
,"Free_Gift","Merchant_Gift_Card","Merchant_Rewards","Recycling","Misc","Total_Value","Details","Ad_Location","Ad_Name"
,"Page_Number","Region","Print_Verified","Online_Verified","gap_URL","FileName")

val ad2=PersonalPrintersAdRawDF.select("Merchant","Brand","Product","Part_Number","Product_Type"
  ,"Shelf_Price_When_Advertised","Advertised_Price"
  ,"Ad_Date","End_Date","Promotion_Type","Bundle_Type","Instant_Savings","Mail-in_Rebate","Price_Drop","Bundle","Peripheral"
,"Free_Gift","Merchant_Gift_Card","Merchant_Rewards","Recycling","Misc","Total_Value","Details","Ad_Location","Ad_Name"
,"Page_Number","Region","Print_Verified","Online_Verified","gap_URL","FileName")

val ad3=ad1.union(ad2)
val ad6=ad3.where((col("Product").isNotNull) or (col("Product") =!= ".") or (col("Part_Number") =!= "Select"))

val maxaddate=ad6.select(col("Ad_Date").cast("string")).agg(max("Ad_Date")).head().getString(0)
val maxadDateObj = to_date(unix_timestamp(lit(maxaddate),"yyyy-MM-dd").cast("timestamp"))
val ad7=ad6.withColumn("Max_Ad_Date", lit(maxadDateObj))

val ad8 = ad7.withColumn("Max_Ad_Date_Add91", date_add(col("Max_Ad_Date").cast("timestamp"), -13*7))
      .where(col("Ad_Date")>col("Max_Ad_Date_Add91"))
      .drop("Max_Ad_Date_Add91")
val ad9=ad8.withColumn("Part_Number",when(col("Part_Number")==="J9V83A","J9V80A")
  .when(col("Part_Number")==="M9L74A","M9L75A").when(col("Part_Number")==="J9V91A","J9V90A").when(col("Part_Number")==="J9V92A","J9V90A")
.otherwise(col("Part_Number")))

val ad10=ad9.withColumnRenamed("Merchant","Right_Merchant").withColumnRenamed("Brand","Right_Brand").
withColumnRenamed("Product","Right_Product").withColumnRenamed("Part_Number","Right_Part_Number")
.withColumnRenamed("Product_Type","Right_Product_Type").withColumnRenamed("Shelf_Price_When_Advertised","Right_Shelf_Price_When_Advertised")
.withColumnRenamed("Advertised_Price","Right_Advertised_Price").withColumnRenamed("Ad_Date","Right_Ad_Date")
.withColumnRenamed("End_Date","Right_End_Date")
.withColumnRenamed("Promotion_Type","Right_Promotion_Type").withColumnRenamed("Bundle_Type","Right_Bundle_Type")
.withColumnRenamed("Instant_Savings","Right_Instant_Savings").withColumnRenamed("Mail-in_Rebate","Right_Mail-in_Rebate")
.withColumnRenamed("Price_Drop","Right_Price_Drop").withColumnRenamed("Bundle","Right_Bundle")
.withColumnRenamed("Peripheral","Right_Peripheral").withColumnRenamed("Free_Gift","Right_Free_Gift")
.withColumnRenamed("Merchant_Gift_Card","Right_Merchant_Gift_Card").withColumnRenamed("Merchant_Rewards","Right_Merchant_Rewards")
.withColumnRenamed("Recycling","Right_Recycling").withColumnRenamed("Misc","Right_Misc")
.withColumnRenamed("Total_Value","Right_Total_Value").withColumnRenamed("Details","Right_Details")
.withColumnRenamed("Ad_Location","Right_Ad_Location").withColumnRenamed("Ad_Name","Right_Ad_Name")
.withColumnRenamed("Page_Number","Right_Page_Number").withColumnRenamed("Region","Right_Region")
.withColumnRenamed("Print_Verified","Right_Print_Verified").withColumnRenamed("Online_Verified","Right_Online_Verified")
.withColumnRenamed("gap_URL","Right_gap_URL")
.withColumnRenamed("FileName","Right_FileName")

val GAPInputAdRawDF = renameColumns(spark.read.option("header","true").option("inferSchema","true")
  .csv("/etherData/ManagedSources/GAP/gap_input_ad.csv"))
val ad11=GAPInputAdRawDF.join(ad10,GAPInputAdRawDF("Ad_Date")===ad10("Right_Ad_Date"),"left")

val ad12=doUnion(ad11,ad9).get.select("Merchant","Brand","Product","Part_Number","Shelf_Price_When_Advertised","Advertised_Price"
  ,"Ad_Date","End_Date","Promotion_Type","Bundle_Type","Instant_Savings","Mail-in_Rebate","Price_Drop","Bundle","Peripheral"
,"Free_Gift","Merchant_Gift_Card","Merchant_Rewards","Recycling","Misc","Total_Value","Details","Ad_Location","Ad_Name"
,"Page_Number","Region","Print_Verified","Online_Verified","gap_URL","FileName")
// .withColumn("Ad",1)
ad12.write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/Pricing/Output/POS_GAP/gap_input_ad.csv")

val GAPDF = renameColumns(spark.read.option("header","true").option("inferSchema","true").csv("/etherData/ManagedSources/GAP/gap_data_full.csv"))
GAPDF.write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/Pricing/Output/POS_GAP/gap_data_full.csv")


  }
}