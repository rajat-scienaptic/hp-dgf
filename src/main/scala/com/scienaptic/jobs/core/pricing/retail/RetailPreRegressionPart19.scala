package com.scienaptic.jobs.core.pricing.retail

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import com.scienaptic.jobs.ExecutionContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object RetailPreRegressionPart19 {

  def execute(executionContext: ExecutionContext): Unit = {
    val spark: SparkSession = executionContext.spark

    var retailGroupWEDL1InnerCompCann3  = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/home/avik/Scienaptic/HP/data/May31_Run/spark_out_retail/retail-PriceBandCannOfflineOnline-PART18.csv")
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("EOL_Date", to_date(unix_timestamp(col("EOL_Date"), "yyyy-MM-dd").cast("timestamp"))).cache()

    var retailWithCompCann2DF = retailGroupWEDL1InnerCompCann3
      .withColumn("PriceBand_Copy", col("PriceBand"))
      .withColumn("L1_Category_Copy", col("L1_Category"))
      .withColumn("Cate", concat_ws(".", col("L1_Category_Copy"), col("PriceBand_Copy")))
      .withColumn("Cate", when(col("Cate").isin("Home and Home Office.200-300", "Home and Home Office.300-500"), "Home and Home Office.200-500")
        .when(col("Cate").isin("Office - Personal.<100", "Office - Personal.100-150"), "Office - Personal.<150")
        .when(col("Cate").isin("Scanners.200-300", "Scanners.300-500"), "Scanners.200-500").otherwise(col("Cate")))

    //    retailWithCompCann2DF.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("D:\\files\\temp\\retail-Feb06-r-1613.csv")

    //Modify Cannibalization 4 ####
    val retailWithAdj4 = retailWithCompCann2DF.withColumn("Adj_Qty", when(col("POS_Qty") <= 0, 0).otherwise(col("POS_Qty")))
    val retailGroupWEDSKUOnline3 = retailWithAdj4.groupBy("Week_End_Date", "SKU", "Online")
      .agg(sum(col("Promo_Pct_Min") * col("Adj_Qty")).as("sumSKU1"), sum("Adj_Qty").as("sumSKU2"))
      .join(retailWithAdj4, Seq("Week_End_Date", "SKU", "Online"), "right")
      .withColumn("Cate", col("Cate"))

    val retailGroupWEDPriceBrandWithOnlineCateTemp1 = retailGroupWEDSKUOnline3
      .groupBy("Week_End_Date", "Online", "Cate", "Brand")
      .agg(sum(col("Promo_Pct_Min") * col("Adj_Qty")).as("sum1"), sum("Adj_Qty").as("sum2"))

    val retailGroupWEDPriceBrandWithOnlineCate1 = retailGroupWEDSKUOnline3
      .join(retailGroupWEDPriceBrandWithOnlineCateTemp1, Seq("Week_End_Date", "Online", "Cate", "Brand"), "left")
      .withColumn("Cate_cannibalization_OnOffline_Min", (col("sum1") - col("sumSKU1")) / (col("sum2") - col("sumSKU2")))
      .drop("sum1", "sum2", "sumSKU1", "sumSKU2")

    val retailGroupWEDPriceBrandWithOnlineCateTemp2 = retailGroupWEDPriceBrandWithOnlineCate1
      .groupBy("Week_End_Date", "Online", "Cate", "Brand", "Account")
      .agg(sum(col("Promo_Pct_Min") * col("Adj_Qty")).as("sumInner1"), sum("Adj_Qty").as("sumInner2"))

    retailWithCompCann2DF = retailGroupWEDPriceBrandWithOnlineCate1
      .join(retailGroupWEDPriceBrandWithOnlineCateTemp2, Seq("Week_End_Date", "Online", "Cate", "Brand", "Account"), "left")
      .withColumn("Cate_Innercannibalization_OnOffline_Min", (col("sumInner1") - (col("Promo_Pct_Min") * col("Adj_Qty"))) / (col("sumInner2") - col("Adj_Qty")))
      .drop("sumInner1", "sumInner2")
      .withColumn("Cate_cannibalization_OnOffline_Min", when(col("Cate_cannibalization_OnOffline_Min").isNull, 0).otherwise(col("Cate_cannibalization_OnOffline_Min")))
      .withColumn("Cate_Innercannibalization_OnOffline_Min", when(col("Cate_Innercannibalization_OnOffline_Min").isNull, 0).otherwise(col("Cate_Innercannibalization_OnOffline_Min")))
      .na.fill(0, Seq("Cate_cannibalization_OnOffline_Min", "Cate_Innercannibalization_OnOffline_Min"))

    var retailWithCompCann3DF = retailWithCompCann2DF
      .withColumn("BOPIS", when(col("BOPIS").isNull, 0).otherwise(col("BOPIS")))
      /*  CR1 - Logic removed from R code  */
      /*.withColumn("BOPISbtbhol", when(col("BOPIS") === 1 && col("Season").isin("BTB'16", "BTB'17", "HOL'16"), 1).otherwise(lit(0)))
      .withColumn("BOPISbts", when(col("BOPIS") === 1 && col("Season").isin("BTS'16", "BTS'17"), lit(1)).otherwise(lit(0)))*/
      .withColumn("Special_Programs", when(col("BOPIS") === 1 && col("Account").isin("Staples"), "BOPIS").otherwise(col("Special_Programs")))
      /*  CR1 - Deduplication removed from R code  */
      //.distinct()
    retailWithCompCann3DF.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("/home/avik/Scienaptic/HP/data/May31_Run/spark_out_retail/retail-CateCannOfflineOnline-PART19.csv")

  }
}
