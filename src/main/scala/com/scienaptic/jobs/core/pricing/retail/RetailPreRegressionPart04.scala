package com.scienaptic.jobs.core.pricing.retail

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.bean.UnionOperation
import com.scienaptic.jobs.utility.Utils.renameColumns
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object RetailPreRegressionPart04 {

  def execute(executionContext: ExecutionContext): Unit = {
    val spark: SparkSession = executionContext.spark

    val focusedAccounts = List("HP Shopping", "Amazon-Proper", "Best Buy", "Office Depot-Max", "Staples")

    var retailJoinAggUpstreamDF  = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/home/avik/Scienaptic/HP/data/May31_Run/spark_out_retail/retail-retailJoinAggUpstreamDF-PART03.csv")
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))

    var retailJoinAggUpstreamWithNATreatmentDF  = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/home/avik/Scienaptic/HP/data/May31_Run/spark_out_retail/retail-retailJoinAggUpstreamWithNATreatmentDF-PART03.csv")
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))

    var spreadPriceDF = retailJoinAggUpstreamDF
      .filter(col("Special_Programs").isin("None"))

    /*  Reshape Starts  */
    val distinctAccounts = spreadPriceDF.select("Account").distinct().collect().map(_ (0).asInstanceOf[String]).toList

    var reshapewithImpAveAndMin = spreadPriceDF
      .select("Account", "SKU", "Week_End_Date", "Online", "ImpAve", "ImpMin")

    distinctAccounts.foreach(account => {
      reshapewithImpAveAndMin = reshapewithImpAveAndMin
        .withColumn("ImpAve_" + account.replaceAll("[-+.^:,\\s]", ""), when(col("Account") === account, col("ImpAve")))
        .withColumn("ImpMin_" + account.replaceAll("[-+.^:,\\s]", ""), when(col("Account") === account, col("ImpMin")))
    })

    /* do not uncomment
    val maxAgg = reshapewithImpAveAndMin.columns.filter(col => col.contains("ImpAve_") || col.contains("ImpMin_")).map(row => {
      // prepare aggregation
      val aggregatedAVEColumnName = s"max(ImpAve_" + s"$row" + ")"
      val aggregatedMINColumnName = s"max(ImpMin_" + s"$row(_)" + ")"
      aggregationMap("ImpAve_" + col(_)) = "max"
      aggregationMap("ImpAve_" + col(_)) = "max"

      // prepare rename
      renameMap(aggregatedAVEColumnName) = "ImpAve_" + col(_)
      renameMap(aggregatedMINColumnName) = "ImpMin_" + col(_)

    } ).toMap
    */

    spreadPriceDF = spreadPriceDF
      .join(reshapewithImpAveAndMin, Seq("Account", "SKU", "Week_End_Date", "Online"), "inner")
      .drop("Account")
      .groupBy("SKU", "Week_End_Date", "Online")
      .agg(max("ImpAve_AmazonProper").as("ImpAve_AmazonProper"), max("ImpMin_AmazonProper").as("ImpMin_AmazonProper"), max("ImpAve_BestBuy").as("ImpAve_BestBuy"),
        max("ImpMin_BestBuy").as("ImpMin_BestBuy"), max("ImpAve_HPShopping").as("ImpAve_HPShopping"), max("ImpMin_HPShopping").as("ImpMin_HPShopping"), max("ImpAve_OfficeDepotMax").as("ImpAve_OfficeDepotMax"),
        max("ImpMin_OfficeDepotMax").as("ImpMin_OfficeDepotMax"), max("ImpAve_Staples").as("ImpAve_Staples"), max("ImpMin_Staples").as("ImpMin_Staples"))
    /*  Reshape Ends  */

    spreadPriceDF = spreadPriceDF
      .select("SKU", "Week_End_Date", "Online", "ImpAve_AmazonProper", "ImpMin_AmazonProper", "ImpAve_BestBuy", "ImpMin_BestBuy", "ImpAve_HPShopping", "ImpMin_HPShopping", "ImpAve_OfficeDepotMax", "ImpMin_OfficeDepotMax", "ImpAve_Staples", "ImpMin_Staples")

    val retailJoinSpreadPrice = retailJoinAggUpstreamDF
      .join(spreadPriceDF, Seq("SKU", "Week_End_Date", "Online"), "left")
      .withColumn("ImpAve_AmazonProper", when(col("ImpAve_AmazonProper").isNull, col("Street_Price")).otherwise(col("ImpAve_AmazonProper")))
      .withColumn("ImpMin_AmazonProper", when(col("ImpMin_AmazonProper").isNull, col("Street_Price")).otherwise(col("ImpMin_AmazonProper")))
      .withColumn("ImpAve_BestBuy", when(col("ImpAve_BestBuy").isNull, col("Street_Price")).otherwise(col("ImpAve_BestBuy")))
      .withColumn("ImpMin_BestBuy", when(col("ImpMin_BestBuy").isNull, col("Street_Price")).otherwise(col("ImpMin_BestBuy")))
      .withColumn("ImpAve_HPShopping", when(col("ImpAve_HPShopping").isNull, col("Street_Price")).otherwise(col("ImpAve_HPShopping")))
      .withColumn("ImpMin_HPShopping", when(col("ImpMin_HPShopping").isNull, col("Street_Price")).otherwise(col("ImpMin_HPShopping")))
      .withColumn("ImpAve_OfficeDepotMax", when(col("ImpAve_OfficeDepotMax").isNull, col("Street_Price")).otherwise(col("ImpAve_OfficeDepotMax")))
      .withColumn("ImpMin_OfficeDepotMax", when(col("ImpMin_OfficeDepotMax").isNull, col("Street_Price")).otherwise(col("ImpMin_OfficeDepotMax")))
      .withColumn("ImpAve_Staples", when(col("ImpAve_Staples").isNull, col("Street_Price")).otherwise(col("ImpAve_Staples")))
      .withColumn("ImpMin_Staples", when(col("ImpMin_Staples").isNull, col("Street_Price")).otherwise(col("ImpMin_Staples")))

    //val currentTS = executionContext.spark.read.json("/etherData/state/currentTS.json").select("ts").head().getString(0)
    var amz = renameColumns(executionContext.spark.read.option("header", "true").option("inferSchema", true).csv("/home/avik/Scienaptic/HP/data/May31_Run/inputs/amazon_sales_price.csv")).cache()
      //.withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "MM/dd/yyyy").cast("timestamp")))
    amz.columns.toList.foreach(x => {
      amz = amz.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })

    val retailAMZMergeDF = retailJoinSpreadPrice
      .join(amz, Seq("SKU", "Week_End_Date", "Account", "Online"), "left")
      .withColumn("AMZ_Sales_Price", when(col("AMZ_Sales_Price") < 10 || col("AMZ_Sales_Price").isNull, col("ImpMin_AmazonProper")).otherwise(col("AMZ_Sales_Price")))
      .withColumn("ImpMin", when(col("Account").isin("Amazon-Proper"), col("AMZ_Sales_Price")).otherwise(col("ImpMin")))
      .withColumn("ImpMin", when(col("Flash_IR") > 0 && (col("Flash_IR") =!= (col("Street_Price") - col("ImpMin"))), col("Street_Price") - col("Flash_IR")).otherwise(col("ImpMin")))
      .withColumn("Other_IR", when(col("Online") === 1,
        when((col("Street_Price") - col("ImpMin") - col("NP_IR") - col("ASP_IR")) > 5, col("Street_Price") - col("ImpMin") - col("NP_IR") - col("ASP_IR")).otherwise(0))
        .otherwise(col("Other_IR")))
      .withColumn("Total_IR", col("NP_IR") + col("ASP_IR") + col("Other_IR"))

    val retailOtherAccounts = retailJoinAggUpstreamWithNATreatmentDF
      .filter(!col("Account").isin(focusedAccounts: _*))

    var retailUnionRetailOtherAccountsDF = UnionOperation.doUnion(retailAMZMergeDF, retailOtherAccounts).get
      .withColumn("Promo_Flag", when(col("Total_IR") > 0, 1).otherwise(0))
      .withColumn("NP_Flag", when(col("NP_IR") > 0, 1).otherwise(0))
      .withColumn("ASP_Flag", when(col("ASP_IR") > 0, 1).otherwise(0))
      .withColumn("Other_IR_Flag", when(col("Other_IR") > 0, 1).otherwise(0))
      .withColumn("Promo_Pct", col("Total_IR") / col("Street_Price"))
      .withColumn("Discount_Depth_Category", when(col("Promo_Pct") === 0, lit("No Discount"))
        .when(col("Promo_Pct") <= 0.2, lit("Very Low"))
        .when(col("Promo_Pct") <= 0.3, lit("Low"))
        .when(col("Promo_Pct") <= 0.4, lit("Moderate"))
        .when(col("Promo_Pct") <= 0.5, lit("Heavy"))
        .otherwise(lit("Very Heavy")))
      .withColumn("price", log(lit(1) - col("Promo_Pct")))


    retailUnionRetailOtherAccountsDF.coalesce(1).write.mode(SaveMode.Overwrite).option("header", true).csv("/home/avik/Scienaptic/HP/data/May31_Run/spark_out_retail/retail-r-retailUnionRetailOtherAccountsDF-part04.csv")

  }
}
