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

object GAPTransform4 {
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
    var Promo_Ad=renameColumns(spark.read.option("header","true").option("inferSchema","true")
      .csv("/etherData/Pricing/Outputs/POS_GAP/promo_ad_intermediate.csv"))
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"),"yyyy-MM-dd").cast("timestamp")))
    Promo_Ad.columns.toList.foreach(x => {
      Promo_Ad = Promo_Ad.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
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

    val sku_hierarchy=renameColumns(spark.read.option("header","true").option("inferSchema","true")
      .csv("/etherData/managedSources/AUX/Aux_sku_hierarchy.csv"))
      .withColumn("L1_Category",col("`L1: Use Case`"))
      .withColumn("L2_Category",col("`L2: Key functionality`"))
      .select("SKU","Category_1","Category_2","L1_Category","L2_Category")

    val HPSKU=GAPHP.join(sku_hierarchy,Seq("SKU"),"inner")
    val finalmerge=doUnion(GAPNPD,HPSKU).get
      .withColumnRenamed("Ad Location","Ad_Location")
        .select("Brand","Product","SKU","Account","Online","Week_End_Date","Ad"
        ,"Total_IR","Days_on_Promo","Promotion_Type","Ad_Location","L2_Category","L1_Category"
          ,"Category_2","Category_1")
    finalmerge.write.option("header","true").mode(SaveMode.Overwrite)
      .csv("/etherData/Pricing/Outputs/POS_GAP/gap_data_full_"+currentTS+".csv")


  }
}