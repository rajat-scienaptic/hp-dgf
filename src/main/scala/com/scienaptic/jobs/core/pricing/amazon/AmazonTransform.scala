package com.scienaptic.jobs.core.pricing.amazon

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.Utils.renameColumns
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

object AmazonTransform {
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
    val spark = executionContext.spark
    val currentTS = spark.read.json("/etherData/state/currentTS.json").select("ts").head().getString(0)
    var rawfile=renameColumns(spark.read.option("header","true").option("inferSchema","true")
      .csv("/etherData/managedSources/Amazon/ASIN/Amazon_LBB_ODBC_Database.csv"))
      .withColumn("nDate", to_date(unix_timestamp(col("nDate"),"MM-dd-yyyy").cast("timestamp")))
    rawfile.columns.toList.foreach(x => {
      rawfile = rawfile.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    rawfile = rawfile.cache()
    rawfile=rawfile.where(col("SKU")=!= lit("(blank)") /*or col("SKU").isNotNull*/)
      .drop("Sum of 04 - HP Commit","Sum of 01 - AMZ Sales")
      .withColumnRenamed("nDate","Week_beginning_day")
      .withColumnRenamed("08 - AMZ Sales Price","AMZ Sales Price")
      .withColumnRenamed("07 - LBB","LBB")
        .withColumnRenamed("Product_Title","Product Name")
      .where(col("Week_beginning_day").isNotNull)
      .withColumn("Week_End_Date",date_add(col("Week_beginning_day"),6))
      .withColumn("AMZ Sales Price",when(col("AMZ Sales Price").isNull,lit(0))
        .otherwise(col("AMZ Sales Price")))
      .withColumn("LBB",when(col("LBB").isNull,lit(0))
        .otherwise(col("LBB")))
      .withColumn("Online",lit(1))
      .withColumn("Account",lit("Amazon-Proper"))
      .select("SKU","Account","Week_End_Date","Online","AMZ Sales Price","LBB")
        .withColumnRenamed("AMZ Sales Price","AMZ_Sales_Price")
    rawfile.coalesce(1).write.option("header","true").mode(SaveMode.Overwrite)
      .csv("/etherData/Pricing/Outputs/POS_Amazon/amazon_sales_price_"+currentTS+".csv")

  }
}