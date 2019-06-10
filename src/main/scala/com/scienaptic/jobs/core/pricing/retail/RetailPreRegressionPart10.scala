package com.scienaptic.jobs.core.pricing.retail

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.Utils.renameColumns
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.scienaptic.jobs.bean.UnionOperation.doUnion

import scala.collection.mutable

object RetailPreRegressionPart10 {

  def execute(executionContext: ExecutionContext): Unit = {
    val spark: SparkSession = executionContext.spark


    var retailWithCompetitionDF = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/home/avik/Scienaptic/HP/data/May31_Run/spark_out_retail/retail-L1L2-HP-PART09.csv")
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("EOL_Date", to_date(unix_timestamp(col("EOL_Date"), "yyyy-MM-dd").cast("timestamp"))).cache()

    var inkPromo = renameColumns(executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/home/avik/Scienaptic/HP/data/May31_Run/inputs/Ink BOGO data.csv"))
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

    /* CR1 - Walmart Source - Start */
    val walmartRetail = retailWithCompCannDF.where(col("Account")==="Walmart")

    var walmartPOS = renameColumns(spark.read.option("header",true).option("inferSchema",true).csv("/home/avik/Scienaptic/HP/data/May31_Run/inputs/walmart_posqty.csv"))
    walmartPOS = walmartPOS.where(!col("Store Type Desc").isin("Unknown","BASE STR Nghbrhd Mkt") && (col("POS Sales") > 0) && (col("POS Qty") > 0))
        .withColumn("SKU", substring(col("Vendor Stk/Part Nbr"), 0, 6))
    walmartPOS = walmartPOS
      .withColumn("Year", substring(col("Week Desc"), 21, 4))
      .withColumn("Month", substring(col("Week Desc"), 13, 3))
      .withColumn("Day", substring(col("Week Desc"), 17, 2))
      .withColumn("Week_End_Date", concat_ws("-", col("Year"), col("Month"), col("Day")))
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MMM-dd").cast("timestamp")))
        .withColumn("Outlet", when(col("Store Type Desc")==="ONLINE", lit("Walmart.com")).otherwise(lit("walmart")))
      .drop("Day","Month","Year")
    /*  Average Weekly price  */
    walmartPOS = walmartPOS.groupBy("SKU","Week_End_Date","Outlet")
        .agg(sum("POS Qty").as("Walmart_Qty"), sum(when(col("POS Qty").isNull, 1).otherwise(0)).as("WQty_NCount"),
          mean("Unit Retail").as("retail_price"), sum(when(col("Unit Retail").isNull, 1).otherwise(0)).as("rPrice_NCount"),
          (sum("POS Sales")/sum("POS Qty")).as("avg_price"), sum(when(col("POS Sales").isNull || col("POS Qty").isNull, 1).otherwise(0)).as("aPrice_NCount"),
          min(col("POS Sales")/col("POS Qty")).as("min_price"), sum(when(col("POS Sales").isNull || col("POS Qty").isNull, 1).otherwise(0)).as("mPrice_NCount"),
          (sum("POS Cost")/sum("POS Qty")).as("avg_cost"), sum(when(col("POS Cost").isNull || col("POS Qty").isNull, 1).otherwise(0)).as("aCost_NCount"),
          sum("POS Sales").as("total_sales"), sum(when(col("POS Sales").isNull, 1).otherwise(0)).as("tSales_NCount"))
        .withColumn("Walmart_Qty", when(col("WQty_NCount")>0, null).otherwise(col("Walmart_Qty")))
        .withColumn("retail_price", when(col("rPrice_NCount")>0, null).otherwise(col("retail_price")))
        .withColumn("avg_price", when(col("aPrice_NCount")>0, null).otherwise(col("avg_price")))
        .withColumn("min_price", when(col("mPrice_NCount")>0, null).otherwise(col("min_price")))
        .withColumn("avg_cost", when(col("aCost_NCount")>0, null).otherwise(col("avg_cost")))
        .withColumn("total_sales", when(col("tSales_NCount")>0, null).otherwise(col("total_sales")))
        .drop("WQty_NCount","rPrice_NCount","aPrice_NCount","mPrice_NCount","aCost_NCount","tSales_NCount")

    walmartPOS = walmartPOS
        .withColumn("Account", lit("Walmart"))
        .withColumn("Online", when(col("Outlet")==="Walmart.com", 1).otherwise(0))
        .drop("Outlet")
        .withColumn("Special_Programs", lit("None"))

    //walmartPOS = walmartPOS.join(walmartRetail.drop("Special_Programs"), Seq("Account","Online","SKU","Week_End_Date"), "right")
    walmartPOS = walmartPOS.join(walmartRetail.withColumnRenamed("Special_Programs", "Special_Programs_y"), Seq("Account","Online","SKU","Week_End_Date"), "right")

    val visID = renameColumns(spark.read.option("header",true).option("inferSchema",true).csv("/home/avik/Scienaptic/HP/data/May31_Run/inputs/Walmart_checks_visual.csv"))
        .withColumn("EOL_vis", to_date(unix_timestamp(col("EOL_vis"), "MM/dd/yyyy").cast("timestamp")))
        .withColumn("EOL_vis_online", to_date(unix_timestamp(col("EOL_vis_online"), "MM/dd/yyyy").cast("timestamp")))
        .withColumn("BOL_vis", to_date(unix_timestamp(col("BOL_vis"), "MM/dd/yyyy").cast("timestamp")))
        .withColumn("BOL_vis_online", to_date(unix_timestamp(col("BOL_vis_online"), "MM/dd/yyyy").cast("timestamp")))

    walmartPOS = walmartPOS.join(visID, Seq("SKU"), "left")

    walmartPOS = walmartPOS
        .withColumn("EOL_vis", when(col("Online")===0, col("EOL_vis")).otherwise(col("EOL_vis_online")))
        .withColumn("BOL_vis", when(col("Online")===1, col("BOL_vis")).otherwise(col("BOL_vis_online")))
        .withColumn("EOL_vis_flag", when(col("Week_End_Date")>col("EOL_vis"),1).otherwise(0))
        .withColumn("BOL_vis_flag", when(col("Week_End_Date")<col("BOL_vis"),1).otherwise(0))

    val dmerge = renameColumns(spark.read.option("header",true).option("inferSchema",true).csv("/home/avik/Scienaptic/HP/data/May31_Run/inputs/drop_in_merge.csv"))
        //.withColumnRenamed("SKU","SKU")
        .withColumn("cSKU", col("cSKU").cast("string"))

    walmartPOS = walmartPOS.join(dmerge, Seq("SKU"), "left")
        .withColumn("cSKU", when(col("cSKU").isNull, 0).otherwise(col("cSKU")))
        //.na.fill(0, Seq("cSKU"))
        .withColumn("cSKU", when(col("cSKU")===0, col("SKU")).otherwise(col("cSKU")))

    val wmtGroup = walmartPOS.groupBy("cSKU","Week_End_Date","Online")
        .agg(sum("Walmart_Qty").as("c_quantity"), sum(when(col("Walmart_Qty").isNull,1).otherwise(0)).as("c_quant_NCount"),
          (sum(col("avg_price")*col("Walmart_Qty"))/sum("Walmart_Qty")).as("c_avg_price"), sum(when(col("avg_price").isNull || col("Walmart_Qty").isNull,1).otherwise(0)).as("avg_NCount"),
          max("Street_Price").as("c_Street_Price"), sum(when(col("Street_Price").isNull,1).otherwise(0)).as("Street_NCount"))
        .withColumn("c_quantity", when(col("c_quant_NCount")>0,null).otherwise(col("c_quantity")))
        .withColumn("c_avg_price", when(col("avg_NCount")>0, null).otherwise(col("c_avg_price")))
        .withColumn("c_Street_Price", when(col("Street_NCount")>0, null).otherwise(col("c_Street_Price")))
        .drop("c_quant_NCount","avg_NCount","Street_NCount")

    walmartPOS = walmartPOS.join(wmtGroup, Seq("cSKU","Week_End_Date","Online"), "left")

    val wmtQ = walmartPOS.groupBy("SKU","Online")
      .agg(mean("Walmart_Qty").as("avg_quantity"), sum(when(col("Walmart_Qty").isNull,1).otherwise(0)).as("Walmart_NCount"),
        mean("c_quantity").as("avg_c_quantity"), sum(when(col("c_quantity").isNull,1).otherwise(0)).as("c_quant_NCount"))
        .withColumn("avg_quantity", when(col("Walmart_NCount")>0, null).otherwise(col("avg_quantity")))
        .withColumn("avg_c_quantity", when(col("c_quant_NCount")>0, null).otherwise(col("avg_c_quantity")))

    walmartPOS = walmartPOS.join(wmtQ, Seq("SKU","Online"), "left")
      .withColumn("c_quantity_f", when(col("avg_c_quantity").between(0, 200), lit("<200"))
        .when(col("avg_c_quantity").between(200, 1500), lit("200-1500"))
        .when(col("avg_c_quantity").between(1500, 6000), lit("1500-6000"))
        .when(col("avg_c_quantity").between(6000, 50000), lit("6000-50000")))
    val jan10Date = to_date(lit("2015-01-10"))
    walmartPOS = walmartPOS.withColumn("drop_in_week", when(col("c_quantity")===col("Walmart_Qty"), 0).otherwise(1))
        .withColumn("exclude", lit(0))
        .withColumn("exclude", when(col("Week_End_Date")<jan10Date || col("EOL_criterion")===1 || col("BOL_criterion")===1 || col("EOL_vis_flag")===1 ||
          col("BOL_vis_flag")===1 || col("data_useful_vis")===0 || col("drop_in_ex")===1 || col("drop_in_flag")===1,1).otherwise(0))
        .withColumn("exclude", when(col("POS_Qty")<0, 1).otherwise(col("exclude")))
        .withColumn("exclude", when(col("POS_Qty")<0, 1).otherwise(col("exclude")))

    walmartPOS = walmartPOS
        .withColumn("NP_IR", when(col("Street_Price")-col("avg_price")<=0, 0.00001).otherwise(col("Street_Price")-col("avg_price")))
        .withColumn("Promo_Pct", when(col("Street_Price")-col("avg_price")<=0, 0).otherwise(abs((col("Street_Price")-col("avg_price"))/col("Street_Price"))))
        .withColumn("c_discount", when(col("c_Street_Price")-col("c_avg_price")<=0, 0.00001).otherwise(col("c_Street_Price")-col("c_avg_price")))
        .withColumn("c_discount_perc", when(col("c_Street_Price")-col("c_avg_price")<=0, 0).otherwise(abs((col("c_Street_Price")-col("c_avg_price"))/col("c_Street_Price"))))

    retailWithCompCannDF = retailWithCompCannDF.where(col("Account")=!="Walmart").na.fill(0, Seq("Promo_Pct"))
    retailWithCompCannDF = doUnion(retailWithCompCannDF, walmartPOS).get
    /* CR1 - Walmart Source - End */


    //DON'T remove join
    /*val retailWithAdj = retailWithCompCannDF.withColumn("Adj_Qty", when(col("POS_Qty") <= 0, 0).otherwise(col("POS_Qty")))
    val retailGroupWEDSKU = retailWithAdj.groupBy("Week_End_Date", "SKU")
      .agg(sum(col("Promo_Pct") * col("Adj_Qty")).as("sumSKU1"), sum("Adj_Qty").as("sumSKU2"))
      .withColumn("Week_End_Date", col("Week_End_Date"))
      .join(retailWithAdj.withColumn("Week_End_Date", col("Week_End_Date")), Seq("Week_End_Date", "SKU"), "right")*/


    retailWithCompCannDF.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("/home/avik/Scienaptic/HP/data/May31_Run/spark_out_retail/retail-L1L2Cann-half-PART10.csv")

  }
}
