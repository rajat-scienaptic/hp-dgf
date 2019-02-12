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

object GAPTransform3 {
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
    var promo3=renameColumns(spark.read.option("header","true").option("inferSchema","true")
      .csv("/etherData/Pricing/Outputs/POS_GAP/gap_input_promo.csv"))
      .withColumn("Start Date", to_date(unix_timestamp(col("Start Date"),"yyyy-MM-dd").cast("timestamp")))
      .withColumn("End Date", to_date(unix_timestamp(col("End Date"),"yyyy-MM-dd").cast("timestamp")))
    promo3.columns.toList.foreach(x => {
      promo3 = promo3.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })

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

    var ad3 = renameColumns(spark.read.option("header","true").option("inferSchema","true")
      .csv("/etherData/Pricing/Outputs/POS_GAP/gap_input_ad.csv"))
      .withColumn("Ad Date", to_date(unix_timestamp(col("Ad Date"),"yyyy-MM-dd").cast("timestamp")))
      .withColumn("End Date", to_date(unix_timestamp(col("End Date"),"yyyy-MM-dd").cast("timestamp")))

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

    var Promo_Ad=promo3.join(ad3,Seq("SKU","Brand","Product","Account","Week_End_Date"),"fullouter")
    Promo_Ad=Promo_Ad.withColumn("Promotion_Type",when(col("Promotion_Type_Promo").isNull
      ,col("Promotion_Type_Ad")).otherwise(col("Promotion_Type_Promo")))
      .withColumn("Promotion_Type_Promo",lit(""))
      .withColumn("Promotion_Type_Ad",lit(""))
      .withColumn("Ad",when(col("Ad_Promo")===lit(1) or col("Ad")=== lit(1),lit(1))
      .otherwise(lit(0)))
      .withColumn("Ad_Promo",lit(""))

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
      .csv("/etherData/Pricing/Outputs/POS_GAP/gap_data_full.csv")


  }
}