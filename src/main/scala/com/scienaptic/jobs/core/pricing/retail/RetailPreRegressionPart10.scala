package com.scienaptic.jobs.core.pricing.retail

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.Utils.renameColumns
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object RetailPreRegressionPart10 {

  def execute(executionContext: ExecutionContext): Unit = {
    val spark: SparkSession = executionContext.spark


    var retailWithCompetitionDF = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/retailTemp/RetailFeatEngg/retail-L1L2-HP-PART09.csv")
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("EOL_Date", to_date(unix_timestamp(col("EOL_Date"), "yyyy-MM-dd").cast("timestamp"))).cache()

    var inkPromo = renameColumns(executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/managedSources/Calendar/Ink_Promo/Ink BOGO data.csv"))
    inkPromo.columns.toList.foreach(x => {
      inkPromo = inkPromo.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    inkPromo = inkPromo.cache()
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "MM/dd/yyyy").cast("timestamp")))

    retailWithCompetitionDF = retailWithCompetitionDF
      .join(inkPromo, Seq("Category", "Account", "Week_End_Date"), "left")
      .withColumn("BOGO_dummy", when(col("BOGO_dummy").isNull, "No.BOGO").otherwise(col("BOGO_dummy")))
      .withColumn("BOGO", when(col("BOGO_dummy") === "No.BOGO", 0).otherwise(1))
      //.withColumn("wed_cat", concat_ws(".", col("Week_End_Date"), col("L1_Category")))
    //      .withColumn("POS_Qty", when(col("POS_Qty") < 0, 0).otherwise(col("POS_Qty")))

    /* AVIK change: Code commented in R code
    var retailWithCompetitionDFtmep1 = retailWithCompetitionDF
      .groupBy("wed_cat")
      .agg(sum(col("Promo_Pct") * col("POS_Qty")).as("z"), sum(col("POS_Qty")).as("w"))

    retailWithCompetitionDF = retailWithCompetitionDF.join(retailWithCompetitionDFtmep1, Seq("wed_cat"), "left")
      .withColumn("L1_cannibalization", (col("z") - (col("Promo_Pct") * col("POS_Qty"))) / (col("w") - col("POS_Qty")))
      .drop("z", "w", "wed_cat")
      .withColumn("wed_cat", concat_ws(".", col("Week_End_Date"), col("L2_Category")))

    // write
    //    retailWithCompetitionDF.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("D:\\files\\temp\\retail-Feb06-r-1125.csv")
    //        var retailWithCompetitionDF = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("D:\\files\\temp\\retail-r-1125.csv").cache()
    //          .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
    //          .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
    //          .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
    //          .withColumn("EOL_Date", to_date(unix_timestamp(col("EOL_Date"), "yyyy-MM-dd").cast("timestamp"))).cache()

    var retailWithCompetitionDFtemp2 = retailWithCompetitionDF
      .groupBy("wed_cat")
      .agg(sum(col("Promo_Pct") * col("POS_Qty")).as("z"), sum(col("POS_Qty")).as("w"))

    retailWithCompetitionDF = retailWithCompetitionDF.join(retailWithCompetitionDFtemp2, Seq("wed_cat"), "left")
      .withColumn("L2_cannibalization", (col("z") - (col("Promo_Pct") * col("POS_Qty"))) / (col("w") - col("POS_Qty")))
      .drop("z", "w", "wed_cat")

    //    retailWithCompetitionDF.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("D:\\files\\temp\\retail-Feb06-r-1134.csv")
    */

    var retailWithCompCannDF = retailWithCompetitionDF

    //DON'T remove join
    val retailWithAdj = retailWithCompCannDF.withColumn("Adj_Qty", when(col("POS_Qty") <= 0, 0).otherwise(col("POS_Qty")))
    val retailGroupWEDSKU = retailWithAdj.groupBy("Week_End_Date", "SKU")
      .agg(sum(col("Promo_Pct") * col("Adj_Qty")).as("sumSKU1"), sum("Adj_Qty").as("sumSKU2"))
      .withColumn("Week_End_Date", col("Week_End_Date"))
      .join(retailWithAdj.withColumn("Week_End_Date", col("Week_End_Date")), Seq("Week_End_Date", "SKU"), "right")


    retailWithCompCannDF.write.option("header", true).mode(SaveMode.Overwrite).csv("/etherData/retailTemp/RetailFeatEngg/retail-L1L2Cann-half-PART10.csv")

  }
}
