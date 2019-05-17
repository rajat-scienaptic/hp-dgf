package com.scienaptic.jobs.core.pricing.retail

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import com.scienaptic.jobs.ExecutionContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object RetailPreRegressionPart17 {

  def execute(executionContext: ExecutionContext): Unit = {
    val spark: SparkSession = executionContext.spark


    var retailGroupWEDL1InnerCompCann2  = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/retailTemp/RetailFeatEngg/retail-L1L2InnerCann-half-PART16.csv")
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("EOL_Date", to_date(unix_timestamp(col("EOL_Date"), "yyyy-MM-dd").cast("timestamp"))).cache()


    //    retailGroupWEDL1InnerCompCann2.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("D:\\files\\temp\\retail-Feb06-r-1548.csv")
    ////
    val retailGroupWEDSKU3Online = retailGroupWEDL1InnerCompCann2
      .groupBy("Week_End_Date", "Online", "Brand", "L2_Category")
      .agg(sum(col("Promo_Pct_Min") * col("Adj_Qty")).as("sum1"), sum("Adj_Qty").as("sum2"))
    //      .join(retailWithAdj2, Seq("Week_End_Date", "SKU", "Online"), "right")

    var retailGroupWEDL1CompCann3 = retailGroupWEDL1InnerCompCann2
      .join(retailGroupWEDSKU3Online, Seq("Week_End_Date", "Online", "Brand", "L2_Category"), "left")
      .withColumn("L2_cannibalization_OnOffline_Min", (col("sum1") - col("sumSKU1")) / (col("sum2") - col("sumSKU2")))
      .drop("sum1", "sum2", "sumInner1", "sumInner2")

    ////
    val retailGroupWEDSKU4InnerOnline = retailGroupWEDL1CompCann3
      .groupBy("Week_End_Date", "Online", "Brand", "Account", "L2_Category")
      .agg(sum(col("Promo_Pct_Min") * col("Adj_Qty")).as("sumInner1"), sum("Adj_Qty").as("sumInner2"))
    //      .join(retailWithAdj2, Seq("Week_End_Date", "SKU", "Online"), "right")

    var retailGroupWEDL1InnerCompCann3 = retailGroupWEDL1CompCann3
      .join(retailGroupWEDSKU4InnerOnline, Seq("Week_End_Date", "Online", "Brand", "L2_Category", "Account"), "left")
      .withColumn("L2_Innercannibalization_OnOffline_Min", (col("sumInner1") - (col("Promo_Pct_Min") * col("Adj_Qty"))) / (col("sumInner2") - col("Adj_Qty")))
      .drop("sum1", "sum2", "sumInner1", "sumInner2", "sumSKU1", "sumSKU2", "Adj_Qty")
      .withColumn("L1_Innercannibalization_OnOffline_Min", when(col("L1_Innercannibalization_OnOffline_Min").isNull, 0).otherwise(col("L1_Innercannibalization_OnOffline_Min")))
      .withColumn("L2_Innercannibalization_OnOffline_Min", when(col("L2_Innercannibalization_OnOffline_Min").isNull, 0).otherwise(col("L2_Innercannibalization_OnOffline_Min")))
      .na.fill(0, Seq("L1_Innercannibalization_OnOffline_Min", "L2_Innercannibalization_OnOffline_Min"))
      // done: retail$PriceBand<-cut(retail$Street.Price,   #TODO: binning  *quantile
      //                        breaks=c(0,100,150,200,300,500,10000),
      //                        labels=c("<100","100-150","150-200","200-300","300-500","500+"))// check : https://rpubs.com/pierrelafortune/cutdocumentation
      .withColumn("PriceBand", when(col("Street_Price").between(0, 100), lit("<100"))
      .when(col("Street_Price").between(100, 150), lit("100-150"))
      .when(col("Street_Price").between(150, 200), lit("150-200"))
      .when(col("Street_Price").between(200, 300), lit("200-300"))
      .when(col("Street_Price").between(300, 500), lit("300-500"))
      .when(col("Street_Price").between(500, 10000), lit("500+"))
    )
      .withColumn("PriceBand", when(col("PriceBand").isNull, "NA").otherwise(col("PriceBand")))

    retailGroupWEDL1InnerCompCann3.write.option("header", true).mode(SaveMode.Overwrite).csv("/etherData/retailTemp/RetailFeatEngg/retail-L1L2InnerCann-PART17.csv")


  }
}
