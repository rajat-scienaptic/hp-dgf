package com.scienaptic.jobs.core

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.Utils.renameColumns
import org.apache.spark.sql.functions.{col, lit, sum, when, round}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.joda.time.DateTime

import scala.collection.mutable.ListBuffer

object RCodeDQValidation {

  case class CommercialCheckDF(Dataset: String, Duplicate_Check_On: String, Number_of_Records: Long, FilePath: String)

  case class CheckSchemaDF(Dataset: String, Check: String, Variables: String)

  /* R-Code Conversion*/
  def execute(executionContext: ExecutionContext): Unit = {
    val spark: SparkSession = executionContext.spark
    val sourceMap = executionContext.configuration.sources
    val reportingBasePaths = executionContext.configuration.reporting

    import spark.implicits._

    val currentTS = spark.read.json("/etherData/state/currentTS.json").select("ts").head().getString(0)

    //Avik: no need to use this. Already present in utility
    def convertListToDFColumn(columnList: List[String], dataFrame: DataFrame) = {
      columnList.map(name => dataFrame.col(name))
    }

    def summarizeColumns(df: DataFrame, cols: List[String]) = {
      df.select(convertListToDFColumn(cols, df): _*).summary()
    }

    val inputRetailLocation = reportingBasePaths("retail-preregression-basepath")
    val inputCommercialLocation = reportingBasePaths("commercial-preregression-basepath")
    val outputLocation = reportingBasePaths("output-basepath").format(currentTS, "R-code")

    val seasons = {
      val yy: Int = DateTime.now().getYear.toString.slice(2, 4).toInt
      val mm: Int = DateTime.now().getMonthOfYear.toInt
      var listMonth = new ListBuffer[String]
      if (mm >= 1 && mm <= 3) {
        listMonth += "BTB'" + yy
        listMonth += "HOL'" + (yy - 1)
        listMonth += "BTS'" + (yy - 1)
        listMonth += "STS'" + (yy - 1)
        listMonth += "BTB'" + (yy - 1)
      } else if (mm >= 4 && mm <= 6) {
        listMonth += "BTB'" + yy
        listMonth += "STS'" + yy
        listMonth += "HOL'" + (yy - 1)
        listMonth += "BTS'" + (yy - 1)
        listMonth += "STS'" + (yy - 1)
      } else if (mm >= 7 && mm <= 9) {
        listMonth += "BTB'" + yy
        listMonth += "STS'" + yy
        listMonth += "BTS'" + yy
        listMonth += "HOL'" + (yy - 1)
        listMonth += "BTS'" + (yy - 1)
      } else if (mm >= 10 && mm <= 12) {
        listMonth += "BTB'" + yy
        listMonth += "STS'" + yy
        listMonth += "BTS'" + yy
        listMonth += "HOL'" + yy
        listMonth += "HOL'" + (yy - 1)
      }
      listMonth
    }

    /* Source Map with all sources' information */
    println("Retail validation Started.")

    /*----------------------------------------------------Retail Starts ---------------------------------------------------*/
    //Load The Retail CSV
    var retailDF = renameColumns(spark.read.option("header", "true").option("inferSchema", "true")
      .csv(inputRetailLocation + "preregression_output_retail_"+ currentTS +".csv"))
    retailDF = retailDF
      .where((retailDF("Account") === "Best Buy")
        || (retailDF("Account") === "Office Depot-Max")
        || (retailDF("Account") === "Staples")
        || (retailDF("Account") === "Amazon-Proper"))

    //    1. Duplicates in the data - check if dplyr makes sense
    var groupedRetailDF = retailDF
      .groupBy("SKU", "Account", "Week_End_Date", "Online", "Special_Programs").count().as("count")
      .filter("count > 1")
    val retDupCount = groupedRetailDF.count();
    if (retDupCount > 0) {
      groupedRetailDF.coalesce(1).write.option("header", "true")
        .mode(SaveMode.Overwrite).csv(outputLocation + "final/Retail_Duplicate_Data.csv")
    }
    val RetailDuplicateDF = Seq(CommercialCheckDF("Retail", "SKU, Account, Week_End_Date, Online, Special_Programs", retDupCount, if (retDupCount > 0) "Retail_Duplicate_Data.csv" else "")).toDF


    //    #2. Check format of all modelled variables
    val dataTypeArray1 = Array("IntegerType", "FloatType", "DoubleType", "LongType")
    val cols = List("trend", "InStock", "DelayDel", "OutofStock", "OnlyInStore", "Street_Price", "Distribution_Inv", "POS_Qty", "Ad", "Ad_Staples",
      "Ad_Best Buy", "Ad_Office Depot-Max", "NP_IR", "ASP_IR", "Total_IR", "Other_IR", "Merchant_Gift_Card", "Flash_IR", "Promo_Pct",
      "NoAvail", "ImpAve", "ImpMin", "LBB", "AMZ_Sales_Price", "L1_competition", "L2_competition", "L1_cannibalization", "L2_cannibalization",
      "Direct_Cann_201", "Direct_Cann_225", "Direct_Cann_252", "Direct_Cann_277", "Direct_Cann_Weber", "Direct_Cann_Palermo", "Direct_Cann_Muscatel_Weber",
      "Direct_Cann_Muscatel_Palermo", "seasonality_npd2", "Promo_Pct_Min", "Promo_Pct_Ave", "L1_cannibalization_OnOffline_Min", "L2_cannibalization_OnOffline_Min",
      "L1_Innercannibalization_OnOffline_Min", "L2_Innercannibalization_OnOffline_Min", "PriceBand_cannibalization_OnOffline_Min",
      "PriceBand_Innercannibalization_OnOffline_Min", "Cate_cannibalization_OnOffline_Min", "Cate_Innercannibalization_OnOffline_Min", /*"BOPISbtbhol", "BOPISbts",*/
      "BOPIS", "LBB_adj", "exclude")
    var dataFormatColumnsVaue = retailDF.select(cols.head, cols.tail: _*).schema.fields

    val invalidDataTypes = dataFormatColumnsVaue
      .filter(dataType => !dataTypeArray1.contains(dataType.dataType.toString))
      .collect { case invalidColumns: StructField => invalidColumns.name }
    val dataTypeCheckDF = Seq(("Retail", cols.mkString(","), "Integer, Double", invalidDataTypes.mkString(",")))
      .toDF("DataSet", "Colums_Validated", "Types_Validated", "Validation_Failed_For")

    val dataTypeArray2 = Array("StringType")
    val dataTypeColumnCheckList2 = List(
      "L1_Category"
      , "L2_Category"
      , "Season_most_recent")
    val factorNames = retailDF.select(dataTypeColumnCheckList2.head, dataTypeColumnCheckList2.tail: _*).schema.fields

    val invalidDataTypes1 = factorNames
      .filter(dataType => !dataTypeArray2.contains(dataType.dataType.toString))
      .collect { case invalidColumns: StructField => invalidColumns.name }

    val dataTypeCheckDF1 = Seq(("Retail", dataTypeColumnCheckList2.mkString(","), "String", invalidDataTypes1.mkString(",")))
      .toDF("DataSet", "Colums_Validated", "Types_Validated", "Validation_Failed_For")

    /*#3. Check ranges of all modelled variables*/
    val RetailSummery = summarizeColumns(retailDF, List("trend", "InStock", "DelayDel", "OutofStock", "OnlyInStore", "Street_Price",
      "Distribution_Inv", "POS_Qty", "Ad", "Ad_Staples", "Ad_Best Buy", "Ad_Office Depot-Max", "NP_IR", "ASP_IR", "Total_IR", "Other_IR", "Merchant_Gift_Card",
      "Flash_IR", "Promo_Pct", "NoAvail", "ImpAve", "ImpMin", "LBB", "AMZ_Sales_Price", "L1_competition", "L2_competition", "L1_cannibalization",
      "L2_cannibalization", "Direct_Cann_201", "Direct_Cann_225", "Direct_Cann_252", "Direct_Cann_277", "Direct_Cann_Weber", "Direct_Cann_Palermo",
      "Direct_Cann_Muscatel_Weber", "Direct_Cann_Muscatel_Palermo", "seasonality_npd2", "Promo_Pct_Min", "Promo_Pct_Ave", "L1_cannibalization_OnOffline_Min",
      "L2_cannibalization_OnOffline_Min", "L1_Innercannibalization_OnOffline_Min", "L2_Innercannibalization_OnOffline_Min", "PriceBand_cannibalization_OnOffline_Min",
      "PriceBand_Innercannibalization_OnOffline_Min", "Cate_cannibalization_OnOffline_Min", "Cate_Innercannibalization_OnOffline_Min",/* "BOPISbtbhol", "BOPISbts",*/
      "BOPIS", "LBB_adj", "exclude", "L1_Category", "L2_Category", "Season_most_recent"))

    //    #4. Check AE variables
    var uniqueAE = retailDF .na.fill(0.0,Seq("NP_IR", "AE_NP_IR")).withColumn("AE_temp", when(col("NP_IR") === col("AE_NP_IR"), lit("true")).otherwise(lit("false")))
    val NPIR = uniqueAE.where(col("AE_temp") === false).count()

    var uniqueAsp = retailDF.where(retailDF("Account") =!= "Amazon-Proper")
    uniqueAsp = uniqueAsp.withColumn("ASP_temp", when(col("ASP_IR") === col("AE_ASP_IR"), lit("true")).otherwise(lit("false")))
    val aspCheck = uniqueAsp.where(col("ASP_temp") === false).count()

    var uniqueIR = retailDF.where(retailDF("Account") =!= "Amazon-Proper")
    uniqueIR = uniqueIR.withColumn("IR_temp", when(col("Other_IR") === col("AE_Other_IR"), lit("true")).otherwise(lit("false")))
    val uniqueIRCheck = uniqueIR.where(col("IR_temp") === false).count()
    var uniqueAE_IR = retailDF.filter(retailDF("Account") === "Amazon-Proper").cache()
    uniqueAE_IR = uniqueAE_IR .na.fill(0.0,Seq("NP_IR", "ASP_IR", "AE_NP_IR", "AE_ASP_IR")).withColumn("AE_IR_temp", when(round((col("NP_IR") + col("ASP_IR"))) === (round(col("AE_NP_IR") + col("AE_ASP_IR") + col("AE_Other_IR"))), lit("true")).otherwise(lit("false")))
    val uniqueAEIRCheck = uniqueAE_IR.where(col("AE_IR_temp") === false).count()

    import spark.implicits._
    val checkDF = Seq((NPIR, aspCheck, uniqueIRCheck, uniqueAEIRCheck)).toDF("AE_variables_Check", "ASP_AE_ASP_Check", "Other_IR_AE_Other_IR", "NP_IR_ASP_check").withColumn("Equality_check", lit("Number of record failing check")).select("Equality_check", "AE_variables_Check", "ASP_AE_ASP_Check", "Other_IR_AE_Other_IR", "NP_IR_ASP_check")
    checkDF.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(outputLocation + "Retail_check_AE_variable_Report.csv")

    //    #4. Check promo.pct for NP.IR, ASP.IR, Other.IR, Gift.Card and its summery
    retailDF = retailDF.withColumn("NP_IR/Street_Price", col("NP_IR") / col("Street_Price"))
    retailDF = retailDF.withColumn("ASP_IR/Street_Price", col("ASP_IR") / col("Street_Price"))
    retailDF = retailDF.withColumn("Other_IR/Street_Price", col("Other_IR") / col("Street_Price"))
    retailDF = retailDF.withColumn("Merchant_Gift_Card/Street_Price", col("Merchant_Gift_Card") / col("Street_Price"))
    val RetailSummery1 = summarizeColumns(retailDF, List("NP_IR/Street_Price", "ASP_IR/Street_Price", "Other_IR/Street_Price", "Merchant_Gift_Card/Street_Price"))

    // #5. Revise distribution=0 and qty>0 to offline retail
    val dist = retailDF.where((retailDF("Online") === 0) && (retailDF("Distribution_Inv") === 0) && (retailDF("POS_Qty") > 0))
      .withColumnRenamed("POS_Qty", "OfflineRetailPOS_QTY")
    val RetailSummery2 = summarizeColumns(dist, List("OfflineRetailPOS_QTY"))

    //    #6. Seasonwise Comparison of Ad count (last 5 seasons)
    var adCountDFForLast5Season = retailDF.where((retailDF("Season") === seasons(0)) ||
      (retailDF("Season") === seasons(1)) ||
      (retailDF("Season") === seasons(2)) ||
      (retailDF("Season") === seasons(3)) ||
      (retailDF("Season") === seasons(4)))
      .groupBy("HPS/OPS", "Season")
      .agg(sum("Ad_Best Buy").alias("Ad_BB"),
        sum("Ad_Staples").alias("Ad_ST"),
        sum("Ad_Office Depot-Max").alias("Ad_ODOM"))
      .sort("HPS/OPS", "Season")
    adCountDFForLast5Season.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(outputLocation + "Retail_Seasonwise_Comparison_for_Ad_count.csv")

    //All Summery Report for Retail
    var totalRetailSummaryList = RetailSummery.join(RetailSummery1, Seq("summary"), "inner").join(RetailSummery2, Seq("summary"), "inner")
    import scala.collection.JavaConversions._
    for (column <- totalRetailSummaryList.columns) {
      totalRetailSummaryList = totalRetailSummaryList.withColumnRenamed(column, column.concat("_Retail"))
    }
    totalRetailSummaryList = totalRetailSummaryList.withColumnRenamed("summary_Retail", "summary")


    /*----------------------------------------------------Commercial Starts ---------------------------------------------------*/

    val commercialDF = renameColumns(spark.read.option("header", "true").option("inferSchema", "true")
      .csv(inputCommercialLocation + "preregresion_commercial_output_"+currentTS+".csv"))

    val duplicateData = commercialDF.groupBy("SKU", "Reseller_Cluster", "Week_End_Date", "Special_Programs", "Brand").count().as("count")
      .filter("count > 1")
    val commDupCount = duplicateData.count();
    if (commDupCount > 0) {
      duplicateData.coalesce(1).write.option("header", "true")
        .mode(SaveMode.Overwrite).csv(outputLocation + "final/Commercial_Duplicate_Data.csv")
    }
    val CommercialDuplicateDF = Seq(CommercialCheckDF("Commercial", "SKU, Reseller_Cluster, Week_End_Date, Special_Programs, Brand", commDupCount, if (commDupCount > 0) "Commercial_Duplicate_Data.csv" else "")).toDF
    CommercialDuplicateDF.union(RetailDuplicateDF).coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(outputLocation + "commercial_And_Retail_Duplicate_Check_Report.csv")

    val dataTypeArray = Array("IntegerType", "FloatType", "DoubleType", "LongType")
    val dataTypeColumnCheckList3 = List("exclude"
      , "Inv_Qty_log"
      , "seasonality_npd"
      , "price"
      , "Street_Price"
      , "L1_competition"
      , "Direct_Cann_201"
      , "Direct_Cann_225"
      , "Direct_Cann_252"
      , "Direct_Cann_277")
    val floatFieldDatatypes2 = commercialDF.select(dataTypeColumnCheckList3.head, dataTypeColumnCheckList3.tail: _*).schema.fields

    val invalidDataTypesComm = floatFieldDatatypes2
      .filter(dataType => !dataTypeArray1.contains(dataType.dataType.toString))
      .collect { case invalidColumns: StructField => invalidColumns.name }

    val dataTypeCheckCommDF = Seq(("Commercial", dataTypeColumnCheckList3.mkString(","), "Integer, Double", invalidDataTypesComm.mkString(",")))
      .toDF("DataSet", "Colums_Validated", "Types_Validated", "Validation_Failed_For")

    dataTypeCheckDF.union(dataTypeCheckCommDF).union(dataTypeCheckDF1).coalesce(1).write.option("header", "true")
      .mode(SaveMode.Overwrite).csv(outputLocation + "Commercial_And_Retail_Check_Column_format_Report.csv")

    /*#3. Check ranges of all modelled variables*/
    val commercial = summarizeColumns(commercialDF, List("exclude", "Inv_Qty_log", "seasonality_npd", "price", "Street_Price", "L1_competition", "Direct_Cann_201", "Direct_Cann_225", "Direct_Cann_252", "Direct_Cann_277"))
    commercial.join(totalRetailSummaryList, Seq("summary"), "inner").coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(outputLocation + "commercial_and_Retail_Summary_Report.csv")
  }
}
