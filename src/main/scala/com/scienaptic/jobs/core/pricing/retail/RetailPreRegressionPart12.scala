package com.scienaptic.jobs.core.pricing.retail

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.CommercialUtility.extractWeekFromDateUDF
import com.scienaptic.jobs.utility.Utils.renameColumns
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object RetailPreRegressionPart12 {

  val dat2000_01_01 = to_date(unix_timestamp(lit("2000-01-01"), "yyyy-MM-dd").cast("timestamp"))
  val dat9999_12_31 = to_date(unix_timestamp(lit("9999-12-31"), "yyyy-MM-dd").cast("timestamp"))

  def execute(executionContext: ExecutionContext): Unit = {
    val spark: SparkSession = executionContext.spark

    var retailWithCompCannDF = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/retailTemp/RetailFeatEngg/retail-L1L2Cann-PART11.csv")
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("EOL_Date", to_date(unix_timestamp(col("EOL_Date"), "yyyy-MM-dd").cast("timestamp"))).cache()

    var npd = renameColumns(executionContext.spark.read.option("header", "true").option("inferSchema", "true").csv("/etherData/managedSources/NPD/NPD_weekly.csv")).cache()
    npd.columns.toList.foreach(x => {
      npd = npd.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    npd = npd.cache()
      .withColumn("Week_End_Date", when(col("Week_End_Date").isNull || col("Week_End_Date") === "", lit(null)).otherwise(
        when(col("Week_End_Date").contains("-"), to_date(unix_timestamp(col("Week_End_Date"), "dd-MM-yyyy").cast("timestamp")))
          .otherwise(to_date(unix_timestamp(col("Week_End_Date"), "MM/dd/yyyy").cast("timestamp")))
      ))

    var IFS2 = renameColumns(executionContext.spark.read.option("header", "true").option("inferSchema", true).csv("/etherData/managedSources/IFS2/IFS2_most_recent.csv"))
    IFS2.columns.toList.foreach(x => {
      IFS2 = IFS2.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    IFS2 = IFS2
      .withColumn("Valid_Start_Date", to_date(unix_timestamp(col("Valid_Start_Date"), "MM/dd/yyyy").cast("timestamp")))
      .withColumn("Valid_End_Date", to_date(unix_timestamp(col("Valid_End_Date"), "MM/dd/yyyy").cast("timestamp"))).cache()


    var npdChanneRetail = npd.where(col("Channel") === "Retail")
      .withColumn("Year", when(col("Week_End_Date").isNull, null).otherwise(year(col("Week_End_Date")).cast("string")))
      .withColumn("Week", when(col("Week_End_Date").isNull, null).otherwise(extractWeekFromDateUDF(col("Week_End_Date").cast("string"), lit("yyyy-MM-dd")).cast(StringType)))
    val npdFilteredL1CategoryDF = npdChanneRetail.where(col("L1_Category").isNotNull)
    var seasonalityNPD = npdFilteredL1CategoryDF
      .groupBy("Week", "L1_Category")
      .agg(sum("UNITS").as("UNITS"), countDistinct("Year").as("Years"))
      .withColumn("UNITS_average", col("UNITS") / col("Years"))

    val seasonalityNPDSum = seasonalityNPD
      .groupBy("L1_Category")
      //AVIK Change: Count on L1_Category instead of Units_average
      .agg(count("L1_Category").as("number_weeks"), sum("UNITS_average").as("UNITS_average"))
      .withColumn("average", col("UNITS_average") / col("number_weeks"))
      .drop("UNITS_average", "number_weeks")

    seasonalityNPD = seasonalityNPD.join(seasonalityNPDSum, Seq("L1_Category"), "left")
      .withColumn("seasonality_npd", (col("UNITS_average") / col("average")) - lit(1))
      .drop("UNITS", "UNITS_average", "average", "Years")


    retailWithCompCannDF = retailWithCompCannDF
      .withColumn("Week", when(col("Week_End_Date").isNull, null).otherwise(extractWeekFromDateUDF(col("Week_End_Date").cast("string"), lit("yyyy-MM-dd")).cast(StringType)))
      .join(seasonalityNPD, Seq("L1_Category", "Week"), "left").drop("Week")
      .withColumn("seasonality_npd2", when((col("USCyberMonday") === lit(1)) || (col("USThanksgivingDay") === lit(1)), 0).otherwise(col("seasonality_npd")))
      .withColumn("seasonality_npd2", when(col("PL") === "4X", lit(1)).otherwise(col("seasonality_npd2")))
      .withColumn("Street_Price", roundUDF(col("Street_Price")))

    IFS2 = IFS2
      .withColumn("Street_Price", roundUDF(col("Street_Price")))
      //AVIK change: Code was missing
        .withColumn("Account", when(col("Account")==="Amazon", "Amazon-Proper").otherwise(col("Account")))
        .withColumn("Account", when(col("Account")==="hpshopping.com", "HP Shopping").otherwise(col("Account")))

    retailWithCompCannDF = retailWithCompCannDF
      .join(IFS2.filter(col("Account").isin("Amazon-Proper", "Best Buy", "Office Depot-Max", "Staples", "Costco", "Sam's Club", "HP Shopping", "Walmart"))
        .select("SKU", "Account", "Street_Price", "Hardware_GM", "Supplies_GM", "Hardware_Rev", "Supplies_Rev", "Valid_Start_Date", "Valid_End_Date", "supplies_GM_scaling_factor"), Seq("SKU", "Account", "Street_Price"), "left")
      .withColumn("Valid_Start_Date", when(col("Valid_Start_Date").isNull, dat2000_01_01).otherwise(col("Valid_Start_Date")))
      .withColumn("Valid_End_Date", when(col("Valid_End_Date").isNull, dat9999_12_31).otherwise(col("Valid_End_Date")))
      //      .withColumn("Street_Price", when(col("Street_Price").isNull, col("Street_Price_Org")).otherwise(col("Street_Price")))
      .filter((col("Week_End_Date") >= col("Valid_Start_Date")) && (col("Week_End_Date") < col("Valid_End_Date")))
    //AVIK Change: Order based on Hardware_GM and Supplies_GM
//    retailWithCompCannDF.coalesce(1).write.mode(SaveMode.Overwrite).option("header", true).csv("D:\\files\\temp\\spark-out-local\\10thMay\\retail-part12-mergeIFS2.csv")

    //May10 Change: Drop duplicate based on SKU, Account Start
    /*val restOfRetailGroupedDF = retailWithCompCannDF
      .groupBy("SKU", "Account")
      .agg(mean(col("Hardware_Rev")).as("Hardware_Rev2"),
        mean(col("Hardware_GM")).as("Hardware_GM2"),
        mean(col("Supplies_Rev")).as("Supplies_Rev2"),
        mean(col("Supplies_GM")).as("Supplies_GM2"),
        mean(col("supplies_GM_scaling_factor")).as("supplies_GM_scaling_factor2")
      )

    retailWithCompCannDF = retailWithCompCannDF.join(restOfRetailGroupedDF, Seq("SKU", "Account"), "left")
      .drop("Hardware_Rev", "Hardware_GM", "Supplies_Rev", "Supplies_GM","supplies_GM_scaling_factor")
      .withColumnRenamed("Hardware_Rev2", "Hardware_Rev")
      .withColumnRenamed("Hardware_GM2", "Hardware_GM")
      .withColumnRenamed("Supplies_Rev2", "Supplies_Rev")
      .withColumnRenamed("Supplies_GM2", "Supplies_GM")
      .withColumnRenamed("supplies_GM_scaling_factor2", "supplies_GM_scaling_factor")*/
      //.dropDuplicates("SKU", "Account")
    /*
 //    val windForSKUnAccount = Window.partitionBy("SKU","Account").orderBy("Hardware_GM","Supplies_GM")
     //val windForSKUnAccountSupplies = Window.partitionBy("SKU","Account").orderBy("Supplies_GM")
     var aveGM = retailWithCompCannDF/*.drop("Supplies_GM")*/.withColumn("row_num", row_number().over(windForSKUnAccount))
       .where(col("row_num")===1).drop("row_num")
     val aveGMSupplies = retailWithCompCannDF.select("SKU","Account","Supplies_GM").withColumn("row_num", row_number().over(windForSKUnAccountSupplies))
       .where(col("row_num")===1).drop("row_num")
     aveGM = aveGM.join(aveGMSupplies, Seq("SKU","Account"), "left")*/
    var aveGM = retailWithCompCannDF
        .dropDuplicates("SKU", "Account", "Street_Price")

    aveGM = aveGM //retailWithCompCannDF.dropDuplicates("SKU", "Account")
      .groupBy("SKU")
      .agg(mean(col("Hardware_GM")).as("aveHWGM"),
        mean(col("Supplies_GM")).as("aveSuppliesGM"))

    //    aveGM.coalesce(1).write.mode(SaveMode.Overwrite).option("header", true).csv("D:\\files\\temp\\retail-r-1419.csv")

    retailWithCompCannDF = retailWithCompCannDF
      .join(aveGM, Seq("SKU"), "left")
      .withColumn("Hardware_GM", when(col("Hardware_GM").isNull, col("aveHWGM")).otherwise(col("Hardware_GM")))
//      .withColumn("Hardware_GM", when(col("aveHWGM").isNull, lit(null).cast(StringType)).otherwise(col("Hardware_GM")))
      .withColumn("Supplies_GM", when(col("Supplies_GM").isNull, col("aveSuppliesGM")).otherwise(col("Supplies_GM")))
//      .withColumn("Supplies_GM", when(col("aveSuppliesGM").isNull, lit(null).cast(StringType)).otherwise(col("Supplies_GM")))
      .withColumn("Supplies_GM", when(col("PL") === "4X", 0).otherwise(col("Supplies_GM")))

    // write
    retailWithCompCannDF.write.mode(SaveMode.Overwrite).option("header", true).csv("/etherData/retailTemp/RetailFeatEngg/retail-Seasonality-Hardware-PART12.csv")

  }
}
