package com.scienaptic.jobs.core.pricing.retail

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.bean.UnionOperation.doUnion
import com.scienaptic.jobs.utility.Utils.renameColumns
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object RetailPreRegressionPart01 {

  val dat2000_01_01 = to_date(unix_timestamp(lit("2000-01-01"), "yyyy-MM-dd").cast("timestamp"))
  val dat9999_12_31 = to_date(unix_timestamp(lit("9999-12-31"), "yyyy-MM-dd").cast("timestamp"))
  val minimumRegressionDate = "2014-01-01"

  def execute(executionContext: ExecutionContext): Unit = {
    val spark: SparkSession = executionContext.spark

    var npd = renameColumns(executionContext.spark.read.option("header", "true").option("inferSchema", "true").csv("/etherData/managedSources/NPD/NPD_weekly.csv"))
    //val maximumRegressionDate = npd.select("Week_End_Date").withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"),"MM/dd/yyyy").cast("timestamp"))).agg(max("Week_End_Date")).head().getDate(0)
    val maximumRegressionDate = npd.select("Week_End_Date").withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"),"MM/dd/yyyy").cast("timestamp")))
      .withColumn("Week_End_Date", date_sub(col("Week_End_Date"), 7)).agg(max("Week_End_Date")).head().getDate(0)
    val currentTS = executionContext.spark.read.json("/etherData/state/currentTS.json").select("ts").head().getString(0)
    var retail = renameColumns(executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/Pricing/Outputs/POS_Retail/posqty_output_retail_"+currentTS+".csv"))
    retail.columns.toList.foreach(x => {
      retail = retail.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    retail = retail.cache()
      .withColumn("Account", when(col("Account") === "Costco Wholesale", "Costco")
        .when(col("Account").isin("Wal-Mart Online"), "Walmart")
        .when(col("Account").isin("Amazon.Com"), "Amazon-Proper")
        .when(col("Account").isin("Micro Electronics Inc", "Fry's Electronics Inc", "Target Stores"), "Rest of Retail").otherwise(col("Account")))
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .where(col("Week_End_Date") >= minimumRegressionDate)
      .where(col("Week_End_Date") <= maximumRegressionDate)
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumnRenamed("Street_Price", "Street_Price_Org")


    var IFS2 = renameColumns(executionContext.spark.read.option("header", "true").option("inferSchema", true).csv("/etherData/managedSources/IFS2/IFS2_most_recent.csv"))
    IFS2.columns.toList.foreach(x => {
      IFS2 = IFS2.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    IFS2 = IFS2
      .withColumn("Valid_Start_Date", to_date(unix_timestamp(col("Valid_Start_Date"), "MM/dd/yyyy").cast("timestamp")))
      .withColumn("Valid_End_Date", to_date(unix_timestamp(col("Valid_End_Date"), "MM/dd/yyyy").cast("timestamp"))).cache()


    var retailMergeIFS2DF = retail
      //      .drop("Street_Price") // removed to avoid ambiguity
      .join(IFS2.dropDuplicates("SKU", "Street_Price").select("SKU", "Changed_Street_Price", "Street_Price", "Valid_Start_Date", "Valid_End_Date", "Fixed_Cost"), Seq("SKU"), "left")
      /*.withColumn("Changed_Street_Price", when(col("Changed_Street_Price").isNull, 0).otherwise(col("Changed_Street_Price")))
      .withColumn("Changed_Street_Price", when((col("Changed_Street_Price") === 1) && (col("Category").isin("Home","SMB")), 0).otherwise(col("Changed_Street_Price")))*/
      .withColumn("Special_Programs", lit("None"))
      .withColumn("Valid_Start_Date", when(col("Valid_Start_Date").isNull, dat2000_01_01).otherwise(col("Valid_Start_Date")))
      .withColumn("Valid_End_Date", when(col("Valid_End_Date").isNull, dat9999_12_31).otherwise(col("Valid_End_Date")))
      .withColumn("Street_Price", when(col("Street_Price").isNull, col("Street_Price_Org")).otherwise(col("Street_Price")))
      .where((col("Week_End_Date") >= col("Valid_Start_Date")) && (col("Week_End_Date") <= col("Valid_End_Date")))
      .drop("Street_Price_Org", "Valid_Start_Date", "Valid_End_Date")
      .withColumn("Fixed_Cost", when(col("Fixed_Cost").isNull, 0).otherwise(col("Fixed_Cost"))) // added new change

    var retailBBYBundleAccountDF = retailMergeIFS2DF.filter((col("Account") === "Best Buy") &&
      (col("Raw_POS_Qty") > col("POS_Qty")))

    retailBBYBundleAccountDF = retailBBYBundleAccountDF
      .withColumn("Special_Programs", lit("BBY Bundle"))
      .filter(col("SKU").isNotNull)
      .withColumn("POS_Qty", col("Raw_POS_Qty") - col("POS_Qty"))

    retailMergeIFS2DF = doUnion(retailMergeIFS2DF, retailBBYBundleAccountDF).get
      .withColumn("POS_Qty", when(col("POS_Qty").isNull, 0).otherwise(col("POS_Qty")))
      .withColumn("Distribution_Inv", when(col("Distribution_Inv").isNull, 0).otherwise(col("Distribution_Inv")))
      .withColumn("Distribution_Inv", when(col("Distribution_Inv") < 0, 0).otherwise(col("Distribution_Inv")))
      .withColumn("Distribution_Inv", when(col("Distribution_Inv") > 1, 1).otherwise(col("Distribution_Inv")))
      .withColumn("SKU", when(col("SKU") === "J9V91A", "J9V90A").otherwise(col("SKU")))
      .withColumn("SKU", when(col("SKU") === "J9V92A", "J9V90A").otherwise(col("SKU")))
      .withColumn("SKU", when(col("SKU") === "M9L74A", "M9L75A").otherwise(col("SKU")))
      //AVIK Change: To make sure precision matches
      .withColumn("Distribution_Inv", round(col("Distribution_Inv").cast("double"), 9))
      .drop("Raw_POS_Qty")

    val retailAggregatePOSDF = retailMergeIFS2DF
      .groupBy("SKU", "Account", "Week_End_Date", "Online", "Special_Programs")
      .agg(sum(col("POS_Qty")).as("POS_Qty"),
        mean(col("Distribution_Inv")).as("Distribution_Inv"))

    retailMergeIFS2DF = retailMergeIFS2DF.drop("POS_Qty", "Distribution_Inv")
      .join(retailAggregatePOSDF, Seq("SKU", "Account", "Week_End_Date", "Online", "Special_Programs"), "left")

    //Change May 10: Drop duplicate section start
    /*var restOfRetailDF = retailMergeIFS2DF
      .filter(col("Account") === "Rest of Retail")

    val restOfRetailGroupedDF = restOfRetailDF
      .groupBy("SKU", "Account", "Week_End_Date", "Online", "Special_Programs")
      .agg(mean(col("Distribution_Inv")).as("Distribution_Inv2"),
       /* mean(col("Street_Price")).as("Street_Price2"),*/
        sum(col("POS_Qty")).as("POS_Qty2")
      )

    restOfRetailDF = restOfRetailDF.join(restOfRetailGroupedDF, Seq("SKU", "Account", "Week_End_Date", "Online", "Special_Programs"), "left")
      .drop("Distribution_Inv",/* "Street_Price",*/ "POS_Qty")
      .withColumnRenamed("Distribution_Inv2", "Distribution_Inv")
      /*.withColumnRenamed("Street_Price2", "Street_Price")*/
      .withColumnRenamed("POS_Qty2", "POS_Qty")
      .dropDuplicates("SKU", "Account", "Week_End_Date", "Online", "Special_Programs")

    var nonRestOfRetailAcounts = retailMergeIFS2DF
      .filter(col("Account") =!= "Rest of Retail")*/

   /* val groupedStreetPrice = nonRestOfRetailAcounts
      .groupBy("SKU", "Account", "Week_End_Date", "Online", "Special_Programs")
      .agg(mean(col("Street_Price")).as("Street_Price2"))

    nonRestOfRetailAcounts = nonRestOfRetailAcounts.join(groupedStreetPrice, Seq("SKU", "Account", "Week_End_Date", "Online", "Special_Programs"), "left")
      .drop("Street_Price")
      .withColumnRenamed("Street_Price2", "Street_Price")*/

//    var retailJoinRetailTreatmentAndAggregatePOSDF = nonRestOfRetailAcounts
    var retailJoinRetailTreatmentAndAggregatePOSDF = retailMergeIFS2DF
      .dropDuplicates("SKU", "Account", "Week_End_Date", "Online", "Special_Programs")

//    retailJoinRetailTreatmentAndAggregatePOSDF = retailJoinRetailTreatmentAndAggregatePOSDF.unionByName(restOfRetailDF)
    //Change May 10: Drop duplicate section End

    var SKUMapping = renameColumns(executionContext.spark.read.option("header", true).option("inferSchema", true).csv("D:\\files\\input\\R\\17thMay\\s-print_SKU_mapping.csv"))
    SKUMapping.columns.toList.foreach(x => {
      SKUMapping = SKUMapping.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    SKUMapping = SKUMapping.cache()
//      .withColumn("Product", lower(col("Product")))

    var GAP1 = renameColumns(executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/Pricing/Outputs/POS_GAP/gap_data_full_" + currentTS + ".csv")).cache()
    GAP1.columns.toList.foreach(x => {
      GAP1 = GAP1.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    GAP1 = GAP1.cache()
      //.withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "dd-MM-yyyy").cast("timestamp")))
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp"))) // TODO : Sandeepans WED format
//      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "dd-MM-yyyy").cast("timestamp")))
      .withColumn("Total_IR", when(col("Account") === "Best Buy" && col("SKU") === "V1N08A", 0).otherwise(col("Total_IR")))
      .withColumn("Total_IR", when(col("Account") === "Office Depot-Max" && col("SKU") === "V1N07A", 0).otherwise(col("Total_IR")))
      .withColumn("Total_IR", when(col("Account") === "Staples" && col("SKU") === "K4T97A", 0).otherwise(col("Total_IR")))
//      .withColumn("Product", lower(col("Product")))
    /*
     * Because Product is in lower case in S-Print_Master_Calendar source.
     * Uncomment when same implemented in R
    .withColumn("Product", upper(col("Product")))*/

    val adPositionDF = GAP1.filter(col("Brand").isin("HP", "Samsung"))
      .select("SKU", "Week_End_Date", "Account", "Online", "Ad_Location", "Brand", "Product")
      .filter(col("Ad_Location").isNotNull)
      .filter(col("Ad_Location") =!= "0")
      .distinct()

    val adPositionJoinSKUMappingDF = adPositionDF.join(SKUMapping, Seq("Product"), "left")
      .withColumn("SKU", when(col("Brand").isin("Samsung"), col("SKU_HP")).otherwise(col("SKU")))
      .drop("Brand", "Product", "SKU_HP")
      .distinct()

    val retailJoinAdPositionDF = retailJoinRetailTreatmentAndAggregatePOSDF
      .join(adPositionJoinSKUMappingDF, Seq("SKU", "Account", "Week_End_Date", "Online"), "left")

    val GAP1JoinSKUMappingDF = GAP1.join(SKUMapping, Seq("Product"), "left")
      .withColumn("SKU", when(col("Brand").isin("Samsung"), col("SKU_HP").cast("string")).otherwise(col("SKU").cast("string")))
      .drop("Product", "SKU_HP")
      //      .withColumn("SKU_HP", lit(null).cast(StringType))
      .distinct()

    retailJoinAdPositionDF.write.mode(SaveMode.Overwrite).option("header", true).csv("/etherData/retailTemp/RetailFeatEngg/retail-r-retailJoinAdPositionDF-PART01.csv")
    GAP1JoinSKUMappingDF.write.mode(SaveMode.Overwrite).option("header", true).csv("/etherData/retailTemp/RetailFeatEngg/retail-r-GAP1JoinSKUMappingDF-PART01.csv")
    // Part 1 Ends here

  }
}
