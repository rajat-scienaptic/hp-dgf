package com.scienaptic.jobs.core.pricing.retail

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import com.scienaptic.jobs.ExecutionContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object RetailPreRegressionPart16 {

  def execute(executionContext: ExecutionContext): Unit = {
    val spark: SparkSession = executionContext.spark


    var retailWithCompCannDF  = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/retailTemp/RetailFeatEngg/retail-Season-L1L2CannOfflineOnline-PART15.csv")
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("EOL_Date", to_date(unix_timestamp(col("EOL_Date"), "yyyy-MM-dd").cast("timestamp"))).cache()

    var retailWithInnerCompCannDF = retailWithCompCannDF

    ////
    // Modify Cannibalization ####
    val retailWithAdj2 = retailWithInnerCompCannDF.withColumn("Adj_Qty", when(col("POS_Qty") <= 0, 0).otherwise(col("POS_Qty")))
    val retailGroupWEDSKUOnline = retailWithAdj2.groupBy("Week_End_Date", "SKU", "Online")
      .agg(sum(col("Promo_Pct_Min") * col("Adj_Qty")).as("sumSKU1"), sum("Adj_Qty").as("sumSKU2"))
      .join(retailWithAdj2, Seq("Week_End_Date", "SKU", "Online"), "right")

    //    retailGroupWEDSKUOnline.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("D:\\files\\temp\\retail-Feb06-r-1538.csv")
    var retailGroupWEDCompCannTemp1 = retailGroupWEDSKUOnline
      .groupBy("Week_End_Date", "Online", "Brand", "L1_Category")
      .agg(sum(col("Promo_Pct_Min") * col("Adj_Qty")).as("sum1"), sum("Adj_Qty").as("sum2"))

    var retailGroupWEDL1CompCann1 = retailGroupWEDSKUOnline
      .join(retailGroupWEDCompCannTemp1, Seq("Week_End_Date", "Brand", "L1_Category", "Online"), "left")
      .withColumn("L1_cannibalization_OnOffline_Min", (col("sum1") - col("sumSKU1")) / (col("sum2") - col("sumSKU2")))
      .drop("sum1", "sum2")

    //    retailGroupWEDL1CompCann1.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("D:\\files\\temp\\retail-Feb06-r-15343.csv")
    val retailGroupWEDSKU2InnerOnline = retailGroupWEDL1CompCann1
      .groupBy("Week_End_Date", "Online", "Brand", "Account", "L1_Category")
      .agg(sum(col("Promo_Pct_Min") * col("Adj_Qty")).as("sumInner1"), sum("Adj_Qty").as("sumInner2"))
    //      .join(retailWithAdj2, Seq("Week_End_Date", "SKU", "Online"), "right")

    var retailGroupWEDL1InnerCompCann2 = retailGroupWEDL1CompCann1
      .join(retailGroupWEDSKU2InnerOnline, Seq("Week_End_Date", "Online", "Brand", "L1_Category", "Account"), "left")
      .withColumn("L1_Innercannibalization_OnOffline_Min", (col("sumInner1") - (col("Promo_Pct_Min") * col("Adj_Qty"))) / (col("sumInner2") - col("Adj_Qty")))
      .drop("sum1", "sum2")

    retailGroupWEDL1InnerCompCann2.write.option("header", true).mode(SaveMode.Overwrite).csv("/etherData/retailTemp/RetailFeatEngg/retail-L1L2InnerCann-half-PART16.csv")

  }
}
