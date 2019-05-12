package com.scienaptic.jobs.core.gap

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.bean.UnionOperation.doUnion
import com.scienaptic.jobs.utility.Utils.renameColumns
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import scala.collection.mutable
import org.apache.spark.sql.expressions.Window

object GAPTransform5 {
  val Cat_switch=1
  val min_baseline = 2
  val stability_weeks = 4
  val intro_weeks = 6

  val dat2000_01_01 = to_date(unix_timestamp(lit("2000-01-01"),"yyyy-MM-dd").cast("timestamp"))
  val dat9999_12_31 = to_date(unix_timestamp(lit("9999-12-31"),"yyyy-MM-dd").cast("timestamp"))
  val TEMP_OUTPUT_DIR = "/etherData/GAPTemp/temp/gap_data_full.csv"

  val takeFirst = udf((collectedList: mutable.WrappedArray[String]) => {
    try {
      collectedList.head
    } catch {
      case _ : Exception => null
    }
  })

  val takeLast = udf((collectedList: mutable.WrappedArray[String]) => {
    try {
      collectedList.last
    } catch {
      case _ : Exception => null
    }
  })

  def execute(executionContext: ExecutionContext): Unit = {
    val sparkConf = new SparkConf().setAppName("gap")
    val spark = SparkSession.builder
      .master("yarn-client")
      .appName("gap")
      .config(sparkConf)
      .config("spark.executor.heartbeatInterval", "10000s")
      .config("spark.network.timeout", "600s")
      .getOrCreate
    val currentTS = spark.read.json("/etherData/state/currentTS.json").select("ts").head().getString(0)
    var Promo_Ad = spark.read.option("header","true").option("inferSchema","true").csv("/etherData/GAPTemp/Promo_Ad.csv")
      .withColumn("Week_End_Date", to_date(col("Week_End_Date")))
    Promo_Ad=Promo_Ad
      .withColumn("Account",when(col("Account")===lit("WalMart.com"),"WalMart").otherwise(col("Account")))
      .withColumn("Account",when(col("Account")===lit("WalMart"),"Walmart").otherwise(col("Account")))
      .withColumn("Account",when(col("Account")===lit("BJS Wholesale Club"),"BJs Wholesale Club").otherwise(col("Account")))
//      .withColumn("Account",when(col("Account")===lit("BestBuy.com"),"Best Buy").otherwise(col("Account")))
      .withColumn("Account",when(col("Account")===lit("Sams Club"),"Sam's Club").otherwise(col("Account")))
      .withColumn("Account",when(col("Account")===lit("Office Depot"),"Office Depot-Max").otherwise(col("Account")))
      .withColumn("Account",when(col("Account")===lit("OfficeMax"),"Office Depot-Max").otherwise(col("Account")))
      .select("SKU","Brand","Account","Online","Week_End_Date"
        ,"Total_IR","Ad","Promotion_Type","Ad Location","Product","Days_on_Promo")

    /*  Change for Order within Group  - Start*/
    val productWind = Window.partitionBy("SKU","Brand","Account","Online","Week_End_Date")
      .orderBy(col("Week_End_Date"),col("SKU"),col("Brand"),col("Account"),col("Online"),col("Total_IR"),col("Product").desc, col("Ad Location").desc, col("Promotion_Type"))
    Promo_Ad = Promo_Ad.withColumn("collected_product", collect_list(col("Product")).over(productWind))
    Promo_Ad = Promo_Ad.withColumn("collected_promotion", collect_list(col("Promotion_Type")).over(productWind))
    Promo_Ad = Promo_Ad.withColumn("collected_ad_location", collect_list(col("Ad Location")).over(productWind))

    Promo_Ad = Promo_Ad.withColumn("Product2", takeFirst(col("collected_product")))
    Promo_Ad = Promo_Ad.withColumn("Promotion_Type2", takeLast(col("collected_promotion")))
    Promo_Ad = Promo_Ad.withColumn("Ad Location2", takeFirst(col("collected_ad_location")))

    var GAP = Promo_Ad.drop("collected_product","collected_promotion","collected_ad_location")
      .drop("Product","Promotion_Type","Ad Location")
      .withColumnRenamed("Product2", "Product")
      .withColumnRenamed("Promotion_Type2", "Promotion_Type")
      .withColumnRenamed("Ad Location2", "Ad Location")
    GAP = GAP.groupBy("SKU","Brand","Account","Online","Week_End_Date")
        .agg(max("Ad").as("Ad"), sum(when(col("Ad").isNull,1).otherwise(0)).as("Ad_null_count"),
          max("Total_IR").as("Total_IR"), sum(when(col("Total_IR").isNull,1).otherwise(0)).as("Total_IR_null_count"),
          max("Days_on_Promo").as("Days_on_Promo"), sum(when(col("Days_on_Promo").isNull,1).otherwise(0)).as("Days_on_Promo_null_count"),
          first("Ad Location").as("Ad Location"), sum(when(col("Ad Location").isNull,1).otherwise(0)).as("Ad Location_null_count"),
          last("Promotion_Type").as("Promotion_Type"), sum(when(col("Promotion_Type").isNull,1).otherwise(0)).as("Promotion_Type_null_count"),
          first("Product").as("Product"), sum(when(col("Product").isNull,1).otherwise(0)).as("Product_null_count"))

    GAP = GAP
      .withColumn("Ad", when(col("Ad_null_count")>0, null).otherwise(col("Ad")))
      .withColumn("Total_IR", when(col("Total_IR_null_count")>0, null).otherwise(col("Total_IR")))
      .withColumn("Days_on_Promo", when(col("Days_on_Promo_null_count")>0, null).otherwise(col("Days_on_Promo")))
      .withColumn("Ad Location", when(col("Ad Location_null_count")>0, null).otherwise(col("Ad Location")))
      .withColumn("Promotion_Type", when(col("Promotion_Type_null_count")>0, null).otherwise(col("Promotion_Type")))
      .withColumn("Product", when(col("Product_null_count")>0, null).otherwise(col("Product")))
        .drop("Ad_null_count","Total_IR_null_count","Days_on_Promo_null_count","Ad Location_null_count","Promotion_Type_null_count","Product_null_count")
    /*  Change for Order within Group  - End  */

    /*val GAP=Promo_Ad.orderBy("Week_End_Date","SKU","Brand","Account","Online","Total_IR")
      .groupBy("SKU","Brand","Account","Online","Week_End_Date")
    .agg(max("Ad").alias("Ad"),max("Total_IR").alias("Total_IR")
    .agg(max("Ad").alias("Ad"),max("Total_IR").alias("Total_IR")
      ,max("Days_on_Promo").alias("Days_on_Promo")
      ,last("Promotion_Type").alias("Promotion_Type")
      ,first("Ad Location").alias("Ad Location"),first("Product").alias("Product")).repartition(500)*/

    val GAPCOMP=GAP.where(col("Brand") isin("Brother","Canon","Xerox","Samsung","Lexmark","Epson"))
      .withColumn("Product",lower(col("Product")))
    val GAPNPD_matching = renameColumns(spark.read.option("header","true").option("inferSchema","true")
      .csv("/etherData/managedSources/GAP/GAP NPD comp matching.csv"))
      .withColumn("Product",lower(col("Product")))
      .withColumnRenamed("SKU","Right_SKU")

    val GAPNPD=GAPCOMP.join(GAPNPD_matching,Seq("Brand","Product"),"inner")
      .drop(col("Market_Category")).drop("Right_SKU")

    val GAPHP=GAP.where(col("Brand") isin("HP")).repartition(500)

    val sku_hierarchy=renameColumns(spark.read.option("header","true").option("inferSchema","true")
      .csv("/etherData/managedSources/AUX/Aux_sku_hierarchy.csv"))
      .withColumn("L1_Category",col("`L1: Use Case`"))
      .withColumn("L2_Category",col("`L2: Key functionality`"))
      .select("SKU","Category_1","Category_2","L1_Category","L2_Category").repartition(500)

    val HPSKU=GAPHP.join(sku_hierarchy,Seq("SKU"),"inner")

    val finalmerge=doUnion(GAPNPD,HPSKU).get
      .withColumnRenamed("Ad Location","Ad_Location")
        .select("Brand","Product","SKU","Account","Online","Week_End_Date","Ad"
        ,"Total_IR","Days_on_Promo","Promotion_Type","Ad_Location","L2_Category","L1_Category"
          ,"Category_2","Category_1")

    finalmerge.write.option("header","true").mode(SaveMode.Overwrite)
      .csv(TEMP_OUTPUT_DIR)

    var gapDataFull = spark.read.option("header","true").option("inferSchema","true").csv(TEMP_OUTPUT_DIR)
      .withColumn("Week_End_Date", to_date(col("Week_End_Date")))

    gapDataFull.select("Brand","Product","SKU","Account","Online","Week_End_Date","Ad"
        ,"Total_IR","Days_on_Promo","Promotion_Type","Ad_Location","L2_Category","L1_Category"
        ,"Category_2","Category_1")
    .coalesce(1).write.option("header","true").mode(SaveMode.Overwrite)
      .csv("/etherData/Pricing/Outputs/POS_GAP/gap_data_full_"+currentTS+".csv")



	}
}
