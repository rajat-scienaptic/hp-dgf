package com.scienaptic.jobs.core

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.Utils.renameColumns
import org.apache.spark.sql.functions.{avg, col, current_date, date_add, lit, max, mean, min, sum, to_date, unix_timestamp, when}
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.joda.time.DateTime
import org.apache.spark.sql.functions.{from_unixtime, unix_timestamp}


import scala.collection.mutable.ListBuffer

object ExcelDQValidation {

  def execute(executionContext: ExecutionContext): Unit = {
    val spark: SparkSession = executionContext.spark
    val sourceMap = executionContext.configuration.sources
    val reportingBasePaths : Map[String,String] = executionContext.configuration.reporting
    import spark.implicits._
    val currentTS = spark.read.json("/etherData/state/currentTS.json").select("ts").head().getString(0)
    // Method for generating the summarise value
    def convertListToDFColumn(columnList: List[String], dataFrame: DataFrame) = {
      columnList.map(name => dataFrame.col(name))
    }

    def summarizeColumns(df: DataFrame, cols: List[String]) = {
      df.select(convertListToDFColumn(cols, df): _*).summary()
    }

    val retailInputLocation = reportingBasePaths("retail-preregression-basepath")
    val commercialInputLocation = reportingBasePaths("commercial-preregression-basepath")
    val outputLocation = reportingBasePaths("output-basepath").format(currentTS, "excel")
    val alteryxRetailInputLocation = reportingBasePaths("retail-alteryx-basepath")
    val alteryxCommercialInputLocation = reportingBasePaths("commercial-alteryx-basepath")
    val auxSkuHierarchyLocation = reportingBasePaths("sku-hierarchy-basepath")

    var minVal = 0.0
    var maxVal = 0.0
    val trueFlag = "TRUE"
    val falseFlag = "FALSE"


    val seasons = {
      val yy: Int = DateTime.now().getYear.toString.slice(2, 4).toInt
      val mm: Int = DateTime.now().getMonthOfYear.toInt
      var listMonth = new ListBuffer[String]
      if (mm >= 1 && mm <= 3) {
        listMonth += "BTB'" + yy
        listMonth += "HOL'" + (yy - 1)
        listMonth += "BTS'" + (yy - 1)
        listMonth += "STS'" + (yy - 1)
      } else if (mm >= 4 && mm <= 6) {
        listMonth += "STS'" + yy
        listMonth += "BTB'" + yy
        listMonth += "HOL'" + (yy - 1)
        listMonth += "BTS'" + (yy - 1)
      } else if (mm >= 7 && mm <= 9) {
        listMonth += "BTS'" + yy
        listMonth += "STS'" + yy
        listMonth += "BTB'" + yy
        listMonth += "HOL'" + (yy - 1)
      } else if (mm >= 10 && mm <= 12) {
        listMonth += "HOL'" + yy
        listMonth += "BTS'" + yy
        listMonth += "STS'" + yy
        listMonth += "BTB'" + yy
      }
      listMonth
    }

    //---------------------------------------Retail validation starts (Excel)---------------------------------------

   /* val retailDataFrame = renameColumns(spark.read.option("header", "true").option("inferSchema", "true")
      .csv(retailInputLocation + "preregression_output_retail_"+ currentTS +".csv"))

    //    Duplicate Check
    val retailDuplicateData = retailDataFrame.groupBy("SKU", "Account", "Week_End_Date", "online", "Special_Programs").count.filter("count>1")
    val retDupCount = retailDuplicateData.count();
    if (retDupCount > 0) {
      retailDuplicateData.coalesce(1).write.option("header", "true")
        .mode(SaveMode.Overwrite).csv(outputLocation + "Retail_Duplicate_Data_new.csv")
    }
    val retailDuplicateDataDF = Seq(("Retail", "SKU, Account, Week_End_Date, Online, Special_Programs", retDupCount, if (retDupCount > 0) "Retail_Duplicate_Data_new.csv" else ""))
      .toDF("Dataset", "Duplicate_Check_On", "Number_of_Records", "FilePath")

    //    #2. Check format of all modelled variables
    val dataTypeArray1 = Array("IntegerType", "FloatType", "DoubleType", "LongType")
    val cols = List("AMZ_Sales_Price", "ASP_IR", "Ad", "Ad_Best Buy", "Ad_Staples", "Cate_Innercannibalization_OnOffline_Min",
      "Cate_cannibalization_OnOffline_Min", "Direct_Cann_201", "Direct_Cann_225", "Direct_Cann_252", "Direct_Cann_277",
      "Direct_Cann_Muscatel_Palermo", "Direct_Cann_Muscatel_Weber", "Direct_Cann_Palermo", "Direct_Cann_Weber", "Distribution_Inv",
      "Flash_IR", "ImpAve", "ImpMin", "InStock", "L1_Innercannibalization_OnOffline_Min", "L1_cannibalization", "L1_cannibalization_OnOffline_Min",
      "L1_competition", "L1_competition_Brother", "L1_competition_Canon", "L1_competition_Epson", "L1_competition_HP_ssmodel", "L1_competition_Lexmark",
      "L1_competition_Samsung", "L1_competition_ssdata_HPmodel", "L2_Innercannibalization_OnOffline_Min", "L2_cannibalization", "L2_cannibalization_OnOffline_Min", "L2_competition",
      "L2_competition_Brother", "L2_competition_Canon", "L2_competition_Epson", "L2_competition_HP_ssmodel",
      "L2_competition_Lexmark", "L2_competition_Samsung", "L2_competition_ssdata_HPmodel", "LBB", "Merchant_Gift_Card", "NP_IR",
      "NoAvail", "OnlyInStore", "Other_IR", "OutofStock", "POS_Qty", "PriceBand_Innercannibalization_OnOffline_Min", "PriceBand_cannibalization_OnOffline_Min",
      "Promo_Pct", "Promo_Pct_Ave", "Promo_Pct_Min", "Street_Price", "Total_IR", "seasonality_npd2",
      "trend")
    val columnGroups = cols.grouped(10).toList
    val dataFormatColumnsVaue = retailDataFrame.select(cols.head, cols.tail: _*).schema.fields
    val invalidDataTypes = dataFormatColumnsVaue
      .filter(dataType => !dataTypeArray1.contains(dataType.dataType.toString))
      .collect { case invalidColumns: StructField => invalidColumns.name }
    var invalidColimns = ""
    if (invalidDataTypes.length > 0) {
      invalidColimns = invalidDataTypes.mkString(",")
    }
    var dataTypeCheckDF = Seq.empty[(String, String, String, String)].toDF("DataSet", "Colums_Validated", "Types_Validated", "Validation_Failed_For")
    for (x <- columnGroups) {
      dataTypeCheckDF = dataTypeCheckDF.union(Seq(("Retail", x.mkString(","), "Integer, Double", invalidColimns))
        .toDF("DataSet", "Colums_Validated", "Types_Validated", "Validation_Failed_For"))
    }
    dataTypeCheckDF.coalesce(1).write.option("header", "true")
      .mode(SaveMode.Overwrite).csv(outputLocation + "Retail_Column_format_Check_Report.csv")

    //Range Check
    val rangeMappings = Map("0-7" -> List("InStock", "OutofStock", "OnlyInStore"),
      "0_or_1" -> List("Ad", "Ad_Staples", "Ad_Best Buy", "Ad_Office Depot-Max", "NoAvail"),
      "gt_0" -> List("Street_Price", "AMZ_Sales_Price", "ImpAve", "ImpMin", "Direct_Cann_201", "Direct_Cann_225", "Direct_Cann_252", "Direct_Cann_277", "Direct_Cann_Weber", "Direct_Cann_Palermo", "Direct_Cann_Muscatel_Weber", "Direct_Cann_Muscatel_Palermo"),
      "geq_0" -> List("NP_IR", "ASP_IR", "Total_IR", "Other_IR", "Merchant_Gift_Card", "Flash_IR"),
      "geq_0_leq_1" -> List("Distribution_Inv", "LBB"),
      "geq_neg_1_leq_1" -> List("seasonality_npd2"),
      "geq_neg_1_lt_1" -> List("L1_cannibalization_OnOffline_Min", "L2_cannibalization_OnOffline_Min", "L1_Innercannibalization_OnOffline_Min",
        "L2_Innercannibalization_OnOffline_Min", "PriceBand_cannibalization_OnOffline_Min", "PriceBand_Innercannibalization_OnOffline_Min",
        "Cate_cannibalization_OnOffline_Min", "Cate_Innercannibalization_OnOffline_Min"),
      "geq_0_lt_1" -> List("Promo_Pct", "L1_cannibalization", "L2_cannibalization", "L1_competition_Brother", "L1_competition_Canon",
        "L1_competition_Epson", "L1_competition_Lexmark", "L1_competition_Samsung", "L2_competition_Brother", "L2_competition_Canon",
        "L2_competition_Epson", "L2_competition_Lexmark", "L2_competition_Samsung", "L1_competition", "L2_competition", "L1_competition_HP_ssmodel",
        "L2_competition_HP_ssmodel", "L1_competition_ssdata_HPmodel", "L2_competition_ssdata_HPmodel"),
      "leq_1" -> List("Promo_Pct_Min", "Promo_Pct_Ave"),
      "any_number" -> List("POS_Qty")
    )
    import spark.implicits._
    var invalidRangeDF = Seq(("Retail", "RangeCheck")).toDF("DataSet", "Check")
    val keys = rangeMappings.keySet
    // retail df
    for ((key, value) <- rangeMappings) {
      for (columnInList <- value) {
        key match {
          case "0-7" => {
            val minHead = retailDataFrame.agg(min(col(columnInList))).head()
            val maxHead = retailDataFrame.agg(max(col(columnInList))).head()
            try {
              minVal = minHead.getDouble(0)
              maxVal = maxHead.getDouble(0)
            } catch {
              case ex: Exception => {
                minVal = minHead.getInt(0)
                maxVal = maxHead.getInt(0)
              }
            }
            if (minVal >= 0 && maxVal <= 7) {
              invalidRangeDF = invalidRangeDF.withColumn(columnInList + "_" + key, lit(trueFlag))
            } else {
              invalidRangeDF = invalidRangeDF.withColumn(columnInList + "_" + key, lit(falseFlag + "(" + minVal + " - " + maxVal + ")"))
            }
          }
          case "0_or_1" => {
            val distinctVal = retailDataFrame.select(col(columnInList)).distinct().collect().map(_ (0).asInstanceOf[Int]).toList
            for (value <- distinctVal) {
              if (value == 0 || value == 1) {
                invalidRangeDF = invalidRangeDF.withColumn(columnInList + "_" + key, lit(trueFlag))
              } else {
                invalidRangeDF = invalidRangeDF.withColumn(columnInList + "_" + key, lit(falseFlag + "(" + value + ")"))
              }
            }
          }
          case "gt_0" => {
            val minHead = retailDataFrame.agg(min(col(columnInList))).head()
            try {
              minVal = minHead.getDouble(0)
            } catch {
              case ex: Exception => {
                minVal = minHead.getInt(0)
              }
            }
            if (minVal > 0) {
              invalidRangeDF = invalidRangeDF.withColumn(columnInList + "_" + key, lit(trueFlag))
            } else {
              invalidRangeDF = invalidRangeDF.withColumn(columnInList + "_" + key, lit(falseFlag + "(" + ">" + minVal + ")"))
            }
          }
          case "geq_0" => {
            val minHead = retailDataFrame.agg(min(col(columnInList))).head()
            try {
              minVal = minHead.getDouble(0)
            } catch {
              case ex: Exception => {
                minVal = minHead.getInt(0)
              }
            }
            if (minVal >= 0) {
              invalidRangeDF = invalidRangeDF.withColumn(columnInList + "_" + key, lit(trueFlag))
            } else {
              invalidRangeDF = invalidRangeDF.withColumn(columnInList + "_" + key, lit(falseFlag + "(" + minVal + ")"))
            }
          }
          case "geq_0_leq_1" => {
            val minHead = retailDataFrame.agg(min(col(columnInList))).head()
            val maxHead = retailDataFrame.agg(max(col(columnInList))).head()
            try {
              minVal = minHead.getDouble(0)
              maxVal = maxHead.getDouble(0)
            } catch {
              case ex: Exception => {
                minVal = minHead.getInt(0)
                maxVal = maxHead.getInt(0)
              }
            }
            if (minVal >= 0 && maxVal <= 1) {
              invalidRangeDF = invalidRangeDF.withColumn(columnInList + "_" + key, lit(trueFlag))
            } else {
              invalidRangeDF = invalidRangeDF.withColumn(columnInList + "_" + key, lit(falseFlag + "(" + minVal + " - " + maxVal + ")"))
            }
          }
          case "geq_neg_1_leq_1" => {
            val minHead = retailDataFrame.agg(min(col(columnInList))).head()
            val maxHead = retailDataFrame.agg(max(col(columnInList))).head()
            try {
              minVal = minHead.getDouble(0)
              maxVal = maxHead.getDouble(0)
            } catch {
              case ex: Exception => {
                minVal = minHead.getInt(0)
                maxVal = maxHead.getInt(0)
              }
            }
            if (minVal >= -1 && maxVal <= 1) {
              invalidRangeDF = invalidRangeDF.withColumn(columnInList + "_" + key, lit(trueFlag))
            } else {
              invalidRangeDF = invalidRangeDF.withColumn(columnInList + "_" + key, lit(falseFlag + "(" + minVal + " - " + maxVal + ")"))
            }
          }
          case "geq_neg_1_lt_1" => {
            val minHead = retailDataFrame.agg(min(col(columnInList))).head()
            val maxHead = retailDataFrame.agg(max(col(columnInList))).head()
            try {
              minVal = minHead.getDouble(0)
              maxVal = maxHead.getDouble(0)
            } catch {
              case ex: Exception => {
                minVal = minHead.getInt(0)
                maxVal = maxHead.getInt(0)
              }
            }
            if (minVal >= -1 && maxVal < 1) {
              invalidRangeDF = invalidRangeDF.withColumn(columnInList + "_" + key, lit(trueFlag))
            } else {
              invalidRangeDF = invalidRangeDF.withColumn(columnInList + "_" + key, lit(falseFlag + "(" + minVal + " - " + maxVal + ")"))
            }
          }
          case "geq_0_lt_1" => {
            val minHead = retailDataFrame.agg(min(col(columnInList))).head()
            val maxHead = retailDataFrame.agg(max(col(columnInList))).head()
            try {
              minVal = minHead.getDouble(0)
              maxVal = maxHead.getDouble(0)
            } catch {
              case ex: Exception => {
                minVal = minHead.getInt(0)
                maxVal = maxHead.getInt(0)
              }
            }
            if (minVal >= 0 && maxVal < 1) {
              invalidRangeDF = invalidRangeDF.withColumn(columnInList + "_" + key, lit(trueFlag))
            } else {
              invalidRangeDF = invalidRangeDF.withColumn(columnInList + "_" + key, lit(falseFlag + "(" + minVal + " - " + maxVal + ")"))
            }
          }
          case "leq_1" => {
            val maxHead = retailDataFrame.agg(max(col(columnInList))).head()
            try {
              maxVal = maxHead.getDouble(0)
            } catch {
              case ex: Exception => {
                maxVal = maxHead.getInt(0)
              }
            }
            if (maxVal <= 1) {
              invalidRangeDF = invalidRangeDF.withColumn(columnInList + "_" + key, lit(trueFlag))
            } else {
              invalidRangeDF = invalidRangeDF.withColumn(columnInList + "_" + key, lit(falseFlag + "(" + maxVal + ")"))
            }
          }
          case "any_number" => {
            var flag = false
            if (!invalidDataTypes.isEmpty) {
              if (invalidDataTypes.contains("POS_Qty")) {
                invalidRangeDF = invalidRangeDF.withColumn(columnInList + "_" + key, lit(falseFlag + "(" + "Not number type" + ")"))
                flag = true
              }
            }
            if (!flag) invalidRangeDF = invalidRangeDF.withColumn(columnInList + "_" + key, lit(trueFlag))
          }
        }
      }
    }

    invalidRangeDF.coalesce(1).write.option("header", "true")
      .mode(SaveMode.Overwrite).csv(outputLocation + "Retail_Invalid_Range_Check_Report.csv")*/

    val retailDataFrame = renameColumns(spark.read.option("header", "true").option("inferSchema", "true")
      .csv(alteryxRetailInputLocation + "posqty_output_retail_" + currentTS + ".csv"))

    /*val retailTwoSeasonDf = retailDataFrame.filter(col("season") === seasons(0) || col("season") === seasons(1))
    val retailFourSeasonDf = retailDataFrame.filter(col("season") === seasons(0) || col("season") === seasons(1) || col("season") === seasons(2) || col("season") === seasons(3))

    var retailDistInv = retailFourSeasonDf.groupBy("Category", "Account").agg(min("Distribution_Inv") as ("min_Distribution_Inv"),
      max("Distribution_Inv") as ("max_Distribution_Inv"), avg("Distribution_Inv") as ("avg_Distribution_Inv"))
    retailFourSeasonDf.createOrReplaceTempView("retail")
    var percentilDF = spark.sql("select Category, Account, " +
      "PERCENTILE(Distribution_Inv, 0.75) OVER (PARTITION BY Category, Account) as percentile_0_75, " +
      "PERCENTILE(Distribution_Inv, 0.50) OVER (PARTITION BY Category, Account) as percentile_0_50, " +
      "PERCENTILE(Distribution_Inv, 0.25) OVER (PARTITION BY Category, Account) as percentile_0_25 from retail")
      .dropDuplicates("Category", "Account")
    retailDistInv = retailDistInv /*.withColumn("Category", col("Category")).withColumn("Account", col("Account"))*/
      .join(percentilDF /*.withColumn("Category", col("Category")).withColumn("Account", col("Account"))*/ , Seq("Category", "Account"), "left")
    retailDistInv.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(outputLocation + "Retail_Summary_of_Distribution_Inv_Report.csv")
    spark.catalog.dropTempView("retail")

    //>0 for all promo.flag>0
    val promoFlagDf = retailDataFrame.filter(col("Promo_Flag") > 0)
    val daysOfPromoFlagDF = promoFlagDf.filter(col("days_on_promo") > 0)
    var promoFlagDiffCheckDF = Seq.empty[(String, String, String, String)].toDF("Dataset", "Validation_Rule", "Number_OF_Maching_Record", "FilePath")
    if (promoFlagDf.count() == daysOfPromoFlagDF.count()) {
      promoFlagDiffCheckDF = Seq(("Retail", "Promo_Flag>0 then Days_on_Promo>0", "ALL", ""))
        .toDF("Dataset", "Validation_Rule", "Number_OF_Maching_Record", "FilePath")
      /*promoFlagDiffCheckDF.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite)
        .csv(outputLocation + "Retail_Summary_days_on_promo_Report.csv")*/
    } else {
      val diffInPromoAndDaysPromo = promoFlagDf.filter(col("days_on_promo") <= 0)
      promoFlagDiffCheckDF = Seq(("Retail", "Promo_Flag>0 then Days_on_Promo>0", diffInPromoAndDaysPromo.count().toString, "Retail_Days_On_Promo_Not_maching_Record.csv"))
        .toDF("Dataset", "Validation_Rule", "Number_OF_Maching_Record", "FilePath")
      /*promoFlagDiffCheckDF.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite)
        .csv(outputLocation + "Retail_Days_On_Promo_Not_maching_Record.csv")*/
    }

    //Month *L2 Category (last 12 months,AMZ only)
    val lBbDf = retailDataFrame.
      withColumn("Week_End_Date_Modified", to_date(unix_timestamp(retailDataFrame.col("Week_End_Date"), "MM/dd/yyyy").cast("timestamp")))
      .orderBy(col("Week_End_Date_Modified").desc)
      .withColumn("date_last12_month", date_add(col("Week_End_Date_Modified"), 365))
      .withColumn("month_wise", from_unixtime(unix_timestamp($"Week_End_Date", "MM/dd/yyyy"), "MMMMM"))
      .drop("Week_End_Date_Modified", "date_last12_month")
    val filterLBbDf = lBbDf.filter(lBbDf("account") === "Amazon-Proper")
      .groupBy("L2_Category", "month_wise").agg(avg("LBB") as ("LBB_Avg"))
    filterLBbDf.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(outputLocation + "Retail_Summary_Avg_For_LBB_Monthly_Report.csv")

    //SKU*Account*WED last 2 seasons and where exclude flag  == 0
    val promoDF = retailTwoSeasonDf.where(retailTwoSeasonDf("exclude") === 0).where(col("Promo_Pct") > 0.7)
      .select("SKU", "Brand", "Account", "Online", "Week_End_Date", "Special_Programs", "Exclude", "Promo_Pct")
    val promoCount = promoDF.count()
    if (promoCount > 0) {
      promoDF.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite)
        .csv(outputLocation + "Retail_Last_Two_Season_Promo_Pct_Exclude.csv")
    }
    promoFlagDiffCheckDF = promoFlagDiffCheckDF.union(Seq(("Retail", "exclude = 0 and Promo_Pct > 0.7", promoCount.toString, if (promoCount > 0) "Retail_Last_Two_Season_Promo_Pct_Exclude.csv" else ""))
      .toDF("Dataset", "Validation_Rule", "Number_of_Records", "FilePath"))
    promoFlagDiffCheckDF.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(outputLocation + "Retail_Days_On_Promo_and_Promo_Pct_Report.csv")

    //    EOL_criterion(Retail)
    /*val eolCriterionDf = retailTwoSeasonDf.filter(col("Total_IR") > 0 && col("EOL_criterion") === 1).select("SKU").distinct()
    val eolDF = Seq(("Retail", "Promoted_Product_Flagged_EOL", eolCriterionDf.count(), eolCriterionDf.collect().map(_ (0)).mkString(",")))
      .toDF("Dataset", "Validation_Rule", "Count", "List_Of_Products")

    //    BOL_criterion(Retail)
    val bolCriterionDf = retailTwoSeasonDf.filter(col("Total_IR") > 0 && col("BOL_criterion") === 1).select("SKU").distinct()
    val bolDF = Seq(("Retail", "Promoted_Product_Flagged_BOL", bolCriterionDf.count(), bolCriterionDf.collect().map(_ (0)).mkString(",")))
      .toDF("Dataset", "Validation_Rule", "Count", "List_Of_Products")

    //    low_baseline(Retail)
    val lowBaseLineDf = retailTwoSeasonDf.filter(col("Total_IR") > 0 && col("low_baseline") === 1).select("SKU").distinct()
    val lowDF = Seq(("Retail", "Promoted_Product_Flagged_Low_Baseline", lowBaseLineDf.count(), lowBaseLineDf.collect().map(_ (0)).mkString(",")))
      .toDF("Dataset", "Validation_Rule", "Count", "List_Of_Products")*/


    // Report for Promoted products having Total_IR > 0 and agg sum for EOL, BOL, POS_Qty, NP_IR, Total_IR for (Account,SKU.Name,Season,Online) combination
    val promotedProductsDF = retailTwoSeasonDf.filter(col("Total_IR") > 0)
      .groupBy("Account", "SKU_Name", "Season", "Online")
      .agg(sum("POS_Qty").as("Total_qty"),
        sum("low_baseline").as("Low_base"),
        sum("EOl_criterion").as("EOL_drop"),
        sum("BOL_criterion").as("BOL_drop"),
        sum("NP_IR").as("NP_IR"),
        sum("Total_IR").as("Total_IR")
      )
      .sort(col("Account"),col("Total_qty").desc)

    promotedProductsDF.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv(outputLocation + "Excluded_promotedProducts_Retail_report.csv")


    //Missing value (null/NA) check for Hardware_GM(Retailer) for 4 season
    val naCheckForHardwareGMDf = retailFourSeasonDf.filter(col("Hardware_GM").isNull)
      .withColumn("Feature_checked", lit("Hardware_GM"))
      .select("Feature_checked", "SKU", "SKU_Name", "Account").distinct()

    //Missing value (null/NA) check for Supplies_GM(Retailer) for 4 season
    val naCheckForSupplierGMDf = retailFourSeasonDf.filter(col("Supplies_GM").isNull)
      .withColumn("Feature_checked", lit("Supplies_GM"))
      .select("Feature_checked", "SKU", "SKU_Name", "Account").distinct()

    val retailHardwareSupplies = naCheckForSupplierGMDf.union(naCheckForHardwareGMDf)
      .withColumn("Dataset", lit("Retail"))
      .withColumnRenamed("SKU", "SKU/Category")
      .withColumnRenamed("Account", "Account/Reseller_Cluster")
      .select("Dataset", "Feature_checked", "SKU/Category", "SKU_Name", "Account/Reseller_Cluster").distinct()


    //LBB_adj and Exclude summary
    var sumLLBDf = summarizeColumns(retailDataFrame, List("LBB_adj", "exclude"))
    for (column <- sumLLBDf.columns) {
      sumLLBDf = sumLLBDf.withColumnRenamed(column, column.concat("_Retail"))
    }
    sumLLBDf = sumLLBDf.withColumnRenamed("summary_Retail", "summary")

    /*-------------------------------Commercial Validation starts--------------------------------------------*/

    val commercialDF = renameColumns(spark.read.option("header", "true").option("inferSchema", "true")
      .csv(commercialInputLocation + "preregresion_commercial_output_"+currentTS+".csv"))

    val commercialDuplicateData = commercialDF.groupBy("SKU", "Reseller_Cluster", "Week_End_Date").count.filter("count>1")
    val commDupCount = commercialDuplicateData.count();
    if (commDupCount > 0) {
      commercialDuplicateData.coalesce(1).write.option("header", "true")
        .mode(SaveMode.Overwrite).csv(outputLocation + "Commercial_Duplicate_Data_new.csv")
    }
    val commercialDuplicateDataDF = Seq(("Commercial", "SKU, Reseller_Cluster, Week_End_Date", commDupCount, if (commDupCount > 0) "Commercial_Duplicate_Data_new.csv" else ""))
      .toDF("Dataset", "Duplicate_Check_On", "Number_of_Records", "FilePath")
    commercialDuplicateDataDF.union(retailDuplicateDataDF).coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(outputLocation + "Commercial_And_Retail_Duplicate_Check_Report.csv")

    //last 4/2 Season data
    val commercialTwoSeasonDf = commercialDF.filter(col("season") === seasons(0) || col("season") === seasons(1))
    val commercialFourSeasonDf = commercialDF.filter(col("season") === seasons(0) || col("season") === seasons(1) || col("season") === seasons(2) || col("season") === seasons(3))

    //last 4 season*Category*Reseller Cluster
    commercialFourSeasonDf.createOrReplaceTempView("commercial")
    var fourSeaCatResel = commercialFourSeasonDf.groupBy("Category", "Reseller_Cluster").agg(
      max("Inv_Qty_log").as("max_Inv_Qty_log"),
      min("Inv_Qty_log").as("min_Inv_Qty_log"),
      avg("Inv_Qty_log").as("avg_Inv_Qty_log"))
    val commercialPercentilDF = spark.sql("select Category, Reseller_Cluster, " +
      "PERCENTILE(Inv_Qty_log, 0.75) OVER (PARTITION BY Category, Reseller_Cluster) as percentile_0_75, " +
      "PERCENTILE(Inv_Qty_log, 0.50) OVER (PARTITION BY Category, Reseller_Cluster) as percentile_0_50, " +
      "PERCENTILE(Inv_Qty_log, 0.25) OVER (PARTITION BY Category, Reseller_Cluster) as percentile_0_25 from commercial")
      .dropDuplicates("Category", "Reseller_Cluster")
    fourSeaCatResel = fourSeaCatResel /*.withColumn("Category", col("Category")).withColumn("Reseller_Cluster", col("Reseller_Cluster"))*/
      .join(commercialPercentilDF /*.withColumn("Category", col("Category")).withColumn("Reseller_Cluster", col("Reseller_Cluster"))*/ ,
      Seq("Category", "Reseller_Cluster"), "left")
    fourSeaCatResel.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(outputLocation + "Commercial_Summary_of_Inv_Qty_log_Report.csv")

    //VPA & Reseller cluster
    val vpaResellDf = commercialDF.groupBy("Reseller_Cluster", "VPA").count().as("VPA_Count")
    vpaResellDf.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(outputLocation + "Commercial_VPA_and_Reseller_Cluster_Report.csv")

    //Summary of last 4 Season
    val summarizeColumn = summarizeColumns(commercialFourSeasonDf, List("outlier", "spike", "opposite", "no_promo_sales"))

    //Summarise exclude
    val summariseExclude = summarizeColumns(commercialDF, List("exclude"))

    //    EOL_criterion(Commercial)
    /*val eolCriterionForCommDf = commercialTwoSeasonDf.filter(col("IR") > 0 && col("EOL") === 1).select("SKU").distinct()
    val eolCommDF = Seq(("Commercial", "Promoted_Product_Flagged_EOL", eolCriterionForCommDf.count(), eolCriterionForCommDf.collect().map(_ (0)).mkString(",")))
      .toDF("Dataset", "Validation_Rule", "Count", "List_Of_Products")

    //    BOL_criterion(Commercial)
    val bolCriterionForCommDf = commercialTwoSeasonDf.filter(col("IR") > 0 && col("BOL") === 1).select("SKU").distinct()
    val bolCommDF = Seq(("Commercial", "Promoted_Product_Flagged_BOL", bolCriterionForCommDf.count(), bolCriterionForCommDf.collect().map(_ (0)).mkString(",")))
      .toDF("Dataset", "Validation_Rule", "Count", "List_Of_Products")

    //    low_baseline(Commercial)
    val lowBaseLineForCommDf = commercialTwoSeasonDf.filter(col("IR") > 0 && col("low_baseline") === 1).select("SKU").distinct()
    val lowCommDF = Seq(("Commercial", "Promoted_Product_Flagged_Low_Baseline", lowBaseLineForCommDf.count(), lowBaseLineForCommDf.collect().map(_ (0)).mkString(",")))
      .toDF("Dataset", "Validation_Rule", "Count", "List_Of_Products")*/

    // Report for Promoted products having IR > 0 and agg sum for EOL, BOL, Qty, NP_IR, IR for (SKU.Name,Reseller_Cluster,Season) combination

    val promotedProductsCommercialDF = commercialTwoSeasonDf.filter(col("IR") > 0)
      .groupBy("Reseller_Cluster", "SKU_Name", "Season")
      .agg(sum("Qty").as("Total_qty"),
        sum("low_baseline").as("Low_base"),
        sum("EOl").as("EOL_drop"),
        sum("BOL").as("BOL_drop"),
        sum("NP_IR").as("NP_IR"),
        sum("IR").as("IR")
      )
      .sort(col("Total_qty").desc)

    promotedProductsCommercialDF.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv(outputLocation + "Excluded_promotedProducts_Commercial_report.csv")
    /*eolDF.union(bolDF).union(lowDF).union(eolCommDF).union(bolCommDF).union(lowCommDF).coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(outputLocation + "Commercial_and Retail_PromoteProduct.csv")*/

    //ToDO: Merge both commercial and retail Hardware_GM , Supplies_GM ,add one more column Dataset for retail and commercial amd merge both together
    //Missing value (null/NA) check for Hardware_GM(Commercial) for 4 season Commercial
    val naCheckForHardwareGMForCommDf = commercialFourSeasonDf.filter(col("Hardware_GM").isNull)
      .withColumn("Feature_checked", lit("Hardware_GM"))
      .select("Feature_checked", "Category","SKU_Name", "Reseller_Cluster").distinct()

    //Missing value (null/NA) check for Supplies_GM(Commercial) for 4 season commercial
    val naCheckForSupplierGMForCommDf = commercialFourSeasonDf.filter(col("Supplies_GM").isNull)
      .withColumn("Feature_checked", lit("Supplies_GM"))
      .select("Feature_checked", "Category","SKU_Name", "Reseller_Cluster").distinct()

    val commercialHardwareSupplies = naCheckForSupplierGMForCommDf.union(naCheckForHardwareGMForCommDf)
      .withColumn("Dataset", lit("Commercial"))
      .withColumnRenamed("Category", "SKU/Category")
      .withColumnRenamed("Reseller_Cluster", "Account/Reseller_Cluster")
      .select("Dataset", "Feature_checked", "SKU/Category","SKU_Name", "Account/Reseller_Cluster").distinct()

    commercialHardwareSupplies.union(retailHardwareSupplies).coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(outputLocation + "Commercial_and_Retail_Null_Check_for_Supplies_GM_and_Hardware_GM.csv")

    // Summarise for  Retail and Commercial
    summarizeColumn.join(summariseExclude, Seq("summary"), "inner")
      .join(sumLLBDf, Seq("summary"), "inner")
      .coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(outputLocation + "Commercial_and_Retail_Summary_Excel_Report.csv")*/


    //-------------------------------------------------Alteryx  for Retail----------------------------------------------


    val recentSeasonRetailData = retailDataFrame.orderBy(col("season").desc)
      .select("SKU", "Account", "season", "POS_Qty", "Raw_POS_Qty", "Distribution_Inv")

    val previousSeasonRetalData = retailDataFrame.orderBy(col("season").desc)
      .withColumnRenamed("POS_Qty", "Pre_POS_Qty")
      .withColumnRenamed("Raw_POS_Qty", "Pre_Raw_POS_Qty")
      .withColumnRenamed("Distribution_Inv", "Pre_Distribution_Inv")
      .select("SKU", "Account", "season", "Pre_POS_Qty", "Pre_Raw_POS_Qty", "Pre_Distribution_Inv")

    val latestSeaName = seasons(0)

    //TODO: Change week to season
    val joinPreAndPresentDf = previousSeasonRetalData.join(recentSeasonRetailData, Seq("SKU", "Account", "season"), "right")

    val retailLatestSeasonFilterDF = joinPreAndPresentDf.filter(col("season") === latestSeaName)
      .withColumn("POS_Qty_Flag", col("POS_Qty") === col("Pre_POS_Qty"))
      .withColumn("Raw_POS_Qty_Flag", col("Raw_POS_Qty") === col("Pre_Raw_POS_Qty"))
      .withColumn("Distribution_Inv_Flag", col("Distribution_Inv") === col("Pre_Distribution_Inv"))
      .select("SKU", "Account", "season", "POS_Qty", "POS_Qty_Flag", "Pre_POS_Qty", "Raw_POS_Qty", "Raw_POS_Qty_Flag", "Pre_Raw_POS_Qty", "Distribution_Inv", "Distribution_Inv_Flag", "Pre_Distribution_Inv")

    val retailLatSeasonPosQtyFlag = retailLatestSeasonFilterDF.filter(col("POS_Qty_Flag") === false)
    //retailLatSeasonPosQtyFlag.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite)
    //.csv(outputLocation + "Alteryx_Retail_POS_Qty_Current_Season.csv")
    val retailLatSeasonRawPosQtyFlag = retailLatestSeasonFilterDF.filter(col("Raw_POS_Qty_Flag") === false)
    //retailLatSeasonRawPosQtyFlag.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite)
    //.csv(outputLocation + "Alteryx_Retail_Raw_POS_Qty_Current_Season.csv")
    val retailLatSeasonDistributionInvFlag = retailLatestSeasonFilterDF.filter(col("Distribution_Inv_Flag") === false)
    //retailLatSeasonDistributionInvFlag.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite)
    //.csv(outputLocation + "Alteryx_Retail_Distribution_Inv_Current_Season.csv")

    val commercialPreviousSeasonFilterDF = joinPreAndPresentDf.filter(col("season") =!= latestSeaName)
      .withColumn("POS_Qty_Flag", (col("POS_Qty") > (col("Pre_POS_Qty") * 1.05)) || (col("Pre_POS_Qty") > (col("POS_Qty") * 1.05)))
      .withColumn("Raw_POS_Qty_Flag", (col("Raw_POS_Qty") > (col("Pre_Raw_POS_Qty") * 1.05)) || (col("Pre_Raw_POS_Qty") > (col("Raw_POS_Qty") * 1.05)))
      .withColumn("Distribution_Inv_Flag", (col("Distribution_Inv") > (col("Pre_Distribution_Inv") * 1.05)) || (col("Pre_Distribution_Inv") > (col("Distribution_Inv") * 1.05)))
      .select("SKU", "Account", "season", "POS_Qty", "POS_Qty_Flag", "Pre_POS_Qty", "Raw_POS_Qty", "Raw_POS_Qty_Flag", "Pre_Raw_POS_Qty", "Distribution_Inv", "Distribution_Inv_Flag", "Pre_Distribution_Inv")

    val retailNonLatSeasonPosQtyFlag = commercialPreviousSeasonFilterDF.filter(col("POS_Qty_Flag") === false)
    //retailNonLatSeasonPosQtyFlag.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite)
    //.csv(outputLocation + "Alteryx_Retail_POS_Qty_Non_Current_Season.csv")
    val retailNonLatSeasonRawPosQtyFlag = commercialPreviousSeasonFilterDF.filter(col("Raw_POS_Qty_Flag") === false)
    //retailNonLatSeasonRawPosQtyFlag.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite)
    //.csv(outputLocation + "Alteryx_Retail_Raw_POS_Qty_Non_Current_Season.csv")
    val retailNonLatSeasonDistributionInvFlag = commercialPreviousSeasonFilterDF.filter(col("Distribution_Inv_Flag") === false)
    //retailNonLatSeasonDistributionInvFlag.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite)
    //.csv(outputLocation + "Alteryx_Retail_Distribution_Inv_Non_Current_Season.csv")

    //TODO: Debug why number larger than total rows in input
    val retailFlag = Seq(
      ("Retail", "POS_Qty_Current_Season", retailLatSeasonPosQtyFlag.count(), "Alteryx_Retail_POS_Qty_Current_Season.csv"),
      ("Retail", "POS_Qty_Non_Current_Season", retailNonLatSeasonPosQtyFlag.count(), "Alteryx_Retail_POS_Qty_Non_Current_Season.csv"),
      ("Retail", "Raw_POS_Qty_Current_Season", retailLatSeasonRawPosQtyFlag.count(), "Alteryx_Retail_Raw_POS_Qty_Current_Season.csv"),
      ("Retail", "Raw_POS_Qty_Non_Current_Season", retailNonLatSeasonRawPosQtyFlag.count(), "Alteryx_Retail_Raw_POS_Qty_Non_Current_Season.csv"),
      ("Retail", "Distribution_Inv_Current_Season", retailLatSeasonDistributionInvFlag.count(), "Alteryx_Retail_Distribution_Inv_Current_Season.csv"),
      ("Retail", "Distribution_Inv_Non_Current_Season", retailNonLatSeasonDistributionInvFlag.count(), "Alteryx_Retail_Distribution_Inv_Non_Current_Season.csv"))
      .toDF("DataSet", "Column_Name", "Records_Not_matching_Previous_Run", "Filepath")
    /*retailFlag.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(outputLocation + "Alteryx_Retail_Comparison_Report.csv")*/

    val checkDiffRetailDF = retailLatestSeasonFilterDF.withColumn("Season_Validated", lit("Current"))
      .unionByName(commercialPreviousSeasonFilterDF.withColumn("Season_Validated", lit("Non_Current")))
      .filter((col("POS_Qty_Flag") === false) || (col("Raw_POS_Qty_Flag") === false) || (col("Distribution_Inv_Flag") === false))
    checkDiffRetailDF.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(outputLocation + "Alteryx_Retail_Latest_season_Comparison.csv")

    //-------------------------------------------------Alteryx  for Commercial----------------------------------------------
    val recentCommercialData = renameColumns(spark.read.option("header", "true").option("inferSchema", "true")
      .csv(alteryxCommercialInputLocation + "posqty_commercial_output_" +currentTS + ".csv")).orderBy(col("season").desc)
      .select("SKU", "Reseller Cluster", "season", "Qty", "Inv_Qty")
    val previousWeekCommercialData = renameColumns(spark.read.option("header", "true").option("inferSchema", "true")
      .csv(alteryxCommercialInputLocation + "posqty_commercial_output_" + currentTS + ".csv")).orderBy(col("season").desc)
      .withColumnRenamed("Qty", "Pre_Qty")
      .withColumnRenamed("Inv_Qty", "Pre_Inv_Qty")
      .select("SKU", "Reseller Cluster", "season", "Pre_Qty", "Pre_Inv_Qty")

    val latestCommercialSeaName = seasons(0)

    val joinPreAndPresentCommDf = previousWeekCommercialData.join(recentCommercialData, Seq("SKU", "Reseller Cluster", "season"), "inner").orderBy(col("season").desc)

    val latestSeasonFilterDF = joinPreAndPresentCommDf.filter(col("season") === latestCommercialSeaName)
      .withColumn("Qty_Flag", col("Qty") === col("Pre_Qty"))
      .withColumn("Inv_Qty_Flag", col("Inv_Qty") === col("Pre_Inv_Qty"))
      .select("SKU", "Reseller Cluster", "season", "Qty", "Qty_Flag", "Pre_Qty", "Inv_Qty", "Inv_Qty_Flag", "Pre_Inv_Qty")
    val commercialLatSeasonQtyFlag = latestSeasonFilterDF.filter(col("Qty_Flag") === false)
    //    commercialLatSeasonQtyFlag.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite)
    //      .csv(outputLocation + "Alteryx_Commercial_Qty_Current_Season.csv")
    val commercialLatSeasonInvQtyFlag = latestSeasonFilterDF.filter(col("Inv_Qty_Flag") === false)
    //    commercialLatSeasonInvQtyFlag.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite)
    //      .csv(outputLocation + "Alteryx_Commercial_Inv_Qty_Current_Season.csv")

    val previousSeasonFilterDF = joinPreAndPresentCommDf.filter(col("season") =!= latestCommercialSeaName).orderBy(col("season").desc)
      .withColumn("Qty_Flag", (col("Qty") > (col("Pre_Qty") * 1.05)) || (col("Pre_Qty") > (col("Qty") * 1.05)))
      .withColumn("Inv_Qty_Flag", (col("Inv_Qty") > (col("Pre_Inv_Qty") * 1.05)) || (col("Pre_Inv_Qty") > (col("Inv_Qty") * 1.05)))
      .select("SKU", "Reseller Cluster", "season", "Qty", "Qty_Flag", "Pre_Qty", "Inv_Qty", "Inv_Qty_Flag", "Pre_Inv_Qty")
    val commercialNonLatSeasonQtyFlag = previousSeasonFilterDF.filter(col("Qty_Flag") === false)
    //    commercialNonLatSeasonQtyFlag.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite)
    //      .csv(outputLocation + "Alteryx_Commercial_Qty_Non_Current_Season.csv")
    val commercialNonLatSeasonInvQtyFlag = previousSeasonFilterDF.filter(col("Inv_Qty_Flag") === false)
    //    commercialNonLatSeasonInvQtyFlag.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite)
    //      .csv(outputLocation + "Alteryx_Commercial_Inv_Qty_Non_Current_Season.csv")

    //TODO: merge two report together(Alteryx_Commercial_Comparison_Report.csv)
    val commercialFlag = Seq(
      ("Commercial", "Qty_Current_Season", commercialLatSeasonQtyFlag.count(), "Alteryx_Commercial_Qty_Current_Season.csv"),
      ("Commercial", "Qty_Non_Current_Season", commercialNonLatSeasonQtyFlag.count(), "Alteryx_Commercial_Qty_Non_Current_Season.csv"),
      ("Commercial", "Inv_Qty_Current_Season", commercialLatSeasonInvQtyFlag.count(), "Alteryx_Commercial_Inv_Qty_Current_Season.csv"),
      ("Commercial", "Inv_Qty_Non_Current_Season", commercialNonLatSeasonInvQtyFlag.count(), "Alteryx_Commercial_Inv_Qty_Non_Current_Season.csv"))
      .toDF("DataSet", "Column_Name", "Records_Not_matching_Previous_Run", "Filepath")
    commercialFlag.union(retailFlag).coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(outputLocation + "Alteryx_Retail_and Commercial_Comparison_Report.csv")

    val checkDiffCommercialDF = latestSeasonFilterDF.union(previousSeasonFilterDF)
      .filter((col("Qty_Flag") === false) || (col("Inv_Qty_Flag") === false))
    checkDiffCommercialDF.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(outputLocation + "Alteryx_Commercial_Latest_season_Comparison.csv")


    //-------------------------------------------------Alteryx  for Retail SKU----------------------------------------------
    //TODO: Debug this
    /*val recentRetailDataForSkuDF = renameColumns(spark.read.option("header", "true").option("inferSchema", "true")
      .csv(alteryxRetailInputLocation + "posqty_output_retail.csv"))
    val skuHierarchyDF = renameColumns(spark.read.option("header", "true").option("inferSchema", "true")
      .csv(auxSkuHierarchyLocation + "Aux_sku_hierarchy.csv")).select("SKU")
      .withColumn("Flag", lit(true))

    var dataTypeSkuDF = Seq.empty[(String, String, Long, String)].toDF("Dataset", "Validation_Rule", "Count", "Fallout SKUs")

    val joinTableDF = recentRetailDataForSkuDF.join(skuHierarchyDF, Seq("SKU"), "left")
      .filter(col("Flag").isNull)

    val skus = joinTableDF.select("SKU").collect().map(_ (0)).mkString(",")
    val retailSKU = Seq(("Retail", "Fallout SKUs", joinTableDF.count(), skus.toString)).toDF("Dataset", "Validation_Rule", "Count", "Fallout SKUs")
    dataTypeSkuDF = dataTypeSkuDF.union(retailSKU)*/


    //-------------------------------------------------Alteryx  for Commercial SKU----------------------------------------------

    //TODO: Debug this
    /*val recentCommercialDataForSkuDF = renameColumns(spark.read.option("header", "true").option("inferSchema", "true")
      .csv(alteryxCommercialInputLocation + "posqty_output_commercial.csv"))

    val comJoinTableDF = recentCommercialDataForSkuDF.join(skuHierarchyDF, Seq("SKU"), "left")
      .filter(col("Flag").isNull)

    val comsSkus = joinTableDF.select("SKU").collect().map(_ (0)).mkString(",")
    val commercialSKU = Seq(("Commercial", "Fallout SKUs", comJoinTableDF.count(), comsSkus.toString)).toDF("Dataset", "Validation_Rule", "Count", "Fallout SKUs")
    dataTypeSkuDF = dataTypeSkuDF.union(commercialSKU)

    dataTypeSkuDF.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(outputLocation + "Alteryx_SKU_Commercial_and_retail_report.csv")*/

    //-------------------------------------------------Merged GeneratedReport----------------------------------------------

  }

}
