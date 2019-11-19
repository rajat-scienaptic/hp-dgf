package com.scienaptic.jobs.core.gap

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.CommercialUtility.createlist
import com.scienaptic.jobs.utility.Utils.renameColumns
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{SaveMode, SparkSession}

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
    val currentTS = spark.read.json("/etherData/state/currentTS.json").select("ts").head().getString(0)
    var promo3=renameColumns(spark.read.option("header","true").option("inferSchema","true")
      .csv("/etherData/Pricing/Outputs/POS_GAP/gap_input_promo_"+currentTS+".csv"))
      .withColumn("Start Date", to_date(unix_timestamp(col("Start Date"),"yyyy-MM-dd").cast("timestamp")))
      .withColumn("End Date", to_date(unix_timestamp(col("End Date"),"yyyy-MM-dd").cast("timestamp")))
    promo3.columns.toList.foreach(x => {
      promo3 = promo3.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })

    promo3=promo3/*.withColumn("Ad_Promo",when(col("On Ad")==="Y",1).otherwise(0))*/
      .withColumn("On Ad",lit(""))
      .withColumn("Promo_Start_Date",col("Start Date"))
      .withColumn("Promo_End_Date",col("End Date"))
      .drop("Start Date","End Date")
      .where(col("Promo_End_Date")>=to_date(unix_timestamp(lit("01-01-2014"),"dd-MM-yyyy").cast("timestamp")))
//      .na.drop(Seq("Value"))
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
      .select("Account", "SKU","Brand","Product","Promotion_Type_Promo","Value_Promo",/*"Ad_Promo",*/
         "Promo_Start_Date","Promo_End_Date","Week_End_Date")
      .withColumn("Days_on_Promo_Start",
        datediff(col("Week_End_Date"),col("Promo_Start_Date"))+lit(1))
      .withColumn("Days_on_Promo_Start",when(col("Days_on_Promo_Start")>lit(6),lit(7))
        .otherwise(col("Days_on_Promo_Start")))
      .withColumn("Days_on_Promo_End",lit(7)-datediff(col("Week_End_Date"),col("Promo_End_Date")))
      .withColumn("Days_on_Promo_End",when(col("Days_on_Promo_End")>lit(6),lit(7))
        .otherwise(col("Days_on_Promo_End")))
      .withColumn("Days_on_Promo",least("Days_on_Promo_End","Days_on_Promo_Start"))
      .select("Account", "SKU","Brand","Product","Promotion_Type_Promo","Value_Promo",/*"Ad_Promo",*/
        "Days_on_Promo","Week_End_Date")

    var ad3 = renameColumns(spark.read.option("header","true").option("inferSchema","true")
      .csv("/etherData/Pricing/Outputs/POS_GAP/gap_input_ad_"+currentTS+".csv"))
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
      .withColumn("Page Number",when(col("Page Number").isNull || col("Page Number") === "na",lit("NA")) // TODO : add 'na'
        .otherwise(col("Page Number")))

    var Promo_Ad=promo3.join(ad3,Seq("SKU","Brand","Product","Account","Week_End_Date"),"fullouter")
    Promo_Ad=Promo_Ad.withColumn("Promotion_Type",when(col("Promotion_Type_Promo").isNull
      ,col("Promotion_Type_Ad")).otherwise(col("Promotion_Type_Promo")))
      .withColumn("Promotion_Type_Promo",lit(""))
      .withColumn("Promotion_Type_Ad",lit(""))
      /*.withColumn("Ad",when(col("Ad_Promo")===lit(1) or col("Ad")=== lit(1),lit(1))
      .otherwise(lit(0)))
      .withColumn("Ad_Promo",lit(""))*/

    Promo_Ad.write.option("header","true").mode(SaveMode.Overwrite)
      .csv("/etherData/Pricing/Outputs/POS_GAP/promo_ad_intermediate.csv")


  }
}
