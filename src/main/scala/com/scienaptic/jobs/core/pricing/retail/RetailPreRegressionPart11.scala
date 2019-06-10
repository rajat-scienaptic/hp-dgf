package com.scienaptic.jobs.core.pricing.retail

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import com.scienaptic.jobs.ExecutionContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object RetailPreRegressionPart11 {

  def execute(executionContext: ExecutionContext): Unit = {
    val spark: SparkSession = executionContext.spark

    var retailWithCompetitionDF = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/home/avik/Scienaptic/HP/data/May31_Run/spark_out_retail/retail-L1L2Cann-half-PART10.csv")
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("EOL_Date", to_date(unix_timestamp(col("EOL_Date"), "yyyy-MM-dd").cast("timestamp"))).cache()

    var retailWithCompCannDF = retailWithCompetitionDF

    //DON'T remove join
    val retailWithAdj = retailWithCompCannDF.withColumn("Adj_Qty", when(col("POS_Qty") <= 0, 0).otherwise(col("POS_Qty")))
      //.withColumn("Promo_Pct_Bck", col("Promo_Pct"))
      .na.fill(0, Seq("Promo_Pct"))

    val retailGroupWEDSKU = retailWithAdj.groupBy("Week_End_Date", "SKU")
      .agg(sum(col("Promo_Pct") * col("Adj_Qty")).as("sumSKU1"), sum("Adj_Qty").as("sumSKU2"))
      .withColumn("Week_End_Date", col("Week_End_Date"))
      .join(retailWithAdj.withColumn("Week_End_Date", col("Week_End_Date")), Seq("Week_End_Date", "SKU"), "right")

    val retailGroupWEDL1Temp = retailGroupWEDSKU
      .groupBy("Week_End_Date", "Brand", "L1_Category")
      .agg(sum(col("Promo_Pct") * col("Adj_Qty")).as("sum1"), sum("Adj_Qty").as("sum2"))

    val retailGroupWEDL1 = retailGroupWEDSKU.withColumn("L1_Category", col("L1_Category"))
      .join(retailGroupWEDL1Temp.withColumn("L1_Category", col("L1_Category")), Seq("Week_End_Date", "Brand", "L1_Category"), "left")
      .withColumn("L1_cannibalization", (col("sum1") - col("sumSKU1")) / (col("sum2") - col("sumSKU2"))) // chenged the name to L2_cannibalization
      .drop("sum1", "sum2")

    val retailGroupWEDL1Temp2 = retailGroupWEDL1
      .groupBy("Week_End_Date", "Brand", "L2_Category")
      .agg(sum(col("Promo_Pct") * col("Adj_Qty")).as("sum1"), sum("Adj_Qty").as("sum2"))

    retailWithCompCannDF = retailGroupWEDL1.withColumn("L2_Category", col("L2_Category"))
      .join(retailGroupWEDL1Temp2.withColumn("L2_Category", col("L2_Category")), Seq("Week_End_Date", "Brand", "L2_Category"), "left")
      .withColumn("L2_cannibalization", (col("sum1") - col("sumSKU1")) / (col("sum2") - col("sumSKU2")))

    retailWithCompCannDF = retailWithCompCannDF
      .drop("sum1", "sum2", "sumSKU1", "sumSKU2", "Adj_Qty")
      .withColumn("L1_cannibalization", when(col("L1_cannibalization").isNull || col("L1_cannibalization") === "", 0).otherwise(col("L1_cannibalization")))
      .withColumn("L2_cannibalization", when(col("L2_cannibalization").isNull || col("L2_cannibalization") === "", 0).otherwise(col("L2_cannibalization")))
      .na.fill(0, Seq("L2_cannibalization", "L1_cannibalization"))

    retailWithCompCannDF = retailWithCompCannDF
      //Avik Change: When Total IR is null, it should give Street price as sale price
      .withColumn("Total_IR", when(col("Total_IR").isNull, 0).otherwise(col("Total_IR")))
      .withColumn("Sale_Price", col("Street_Price") - col("Total_IR"))
      .withColumn("Price_Range_20_Perc_high", lit(1.2) * col("Sale_Price"))
    retailWithCompCannDF.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("/home/avik/Scienaptic/HP/data/May31_Run/spark_out_retail/retail-L1L2Cann-PART11.csv")

  }
}
