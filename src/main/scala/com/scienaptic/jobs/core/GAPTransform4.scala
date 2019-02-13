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

  def execute(executionContext: ExecutionContext): Unit = {
    val sparkConf = new SparkConf().setAppName("gap")
    val spark = SparkSession.builder
      .master("yarn-client")
      .appName("gap")
      .config(sparkConf)
      .config("spark.executor.heartbeatInterval", "10000s")
      .config("spark.network.timeout", "600s")
      .getOrCreate

    import spark.implicits._
    //val currentTS = spark.read.json("/etherData/state/currentTS.json").select("ts").head().getString(0)
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
    Promo_Ad.write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/GAPTemp/Promo_Ad.csv")

  }
}