package com.scienaptic.jobs.core

import java.util.UUID

import com.scienaptic.jobs.ExecutionContext
import org.apache.spark.sql.functions.{to_date, _}
import com.scienaptic.jobs.bean.UnionOperation.doUnion
import com.scienaptic.jobs.core.RetailTransform.NUMERAL0
import com.scienaptic.jobs.utility.CommercialUtility.{addDaystoDateStringUDF, checkPrevQtsGTBaseline, concatenateRank, createlist}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.expressions.Window
import com.scienaptic.jobs.utility.Utils.renameColumns
import org.apache.spark.sql.SaveMode


object RetailPreRegressionTransform {

  val Cat_switch = 1
  val dat2000_01_01 = to_date(unix_timestamp(lit("2000-01-01"), "yyyy-MM-dd").cast("timestamp"))
  val dat9999_12_31 = to_date(unix_timestamp(lit("9999-12-31"), "yyyy-MM-dd").cast("timestamp"))
  val maximumRegressionDate = "2017-01-01"
  val minimumRegressionDate = "2014-01-01"

  val stability_weeks = 4
  val stability_range = 0.7
  val intro_weeks = 8

  val indexerForAdLocation = new StringIndexer().setInputCol("Ad_Location").setOutputCol("Ad_Location_fact")
  val pipelineForForAdLocation = new Pipeline().setStages(Array(indexerForAdLocation))

  val pmax = udf((col1: Double, col2: Double, col3: Double) => math.max(col1, math.max(col2, col3)))

  val checkPrevDistInvGTBaseline = udf((distributions: List[Int], rank: Int, baseline: Int) => {
    var totalGt = 0
    for (i <- 1 until rank) {
      if (distributions(i) > baseline) {
        totalGt += 1
      }
    }
    if (totalGt >= 1)
      1
    else
      0
  })

  def execute(executionContext: ExecutionContext): Unit = {

    val retail = renameColumns(executionContext.spark.read.option("header", "true").option("inferSchema", "true").csv("/etherData/Pricing/Output/POS_ Retail/posqty_output_retail.csv")).cache()
      .withColumn("Account", when(col("Account") === "Costco Wholesale", "Costco")
        .when(col("Account").isin("Wal-Mart Online"), "Walmart")
        .when(col("Account").isin("Amazon.Com"), "Amazon-Proper")
        .when(col("Account").isin("Micro Electronics Inc", "Fry's Electronics Inc", "Target Stores"), "Rest of Retail")
      )
    //remove
//    val IFS2 = renameColumns(executionContext.spark.read.option("header", "true").option("inferSchema", "true").csv("/etherData/ManagedSources/IFS2/IFS2_most_recent.csv"))
//    val SKUMapping = renameColumns(executionContext.spark.read.option("header", "true").option("inferSchema", "true").csv("/etherData/ManagedSources/S-Print/SKU_mapping/s-print_SKU_mapping.csv"))
//    val calendar = renameColumns(executionContext.spark.read.option("header", "true").option("inferSchema", "true").csv("/etherData/ManagedSources/Calendar/Retail/master_calendar_retail.csv"))
//    retail.coalesce(1).write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/Pricing/Output/Preregression_Retail/regression_data_retail.csv")
//    retail.coalesce(1).write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/Pricing/Output/Preregression_Retail/regression_data_retail_"+java.time.LocalDate.now.toString+".csv")
    //remove ends
      .withColumn("Changed_Street_Price", when(col("Changed_Street_Price").isNull, 0).otherwise(col("Changed_Street_Price")))
      .withColumn("Special_Programs", lit("None"))
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .where(col("Week_End_Date") >= minimumRegressionDate)
      .where(col("Week_End_Date") <= maximumRegressionDate)
      .withColumnRenamed("Street_Price_Org", "Street_Price")

    val IFS2 = renameColumns(executionContext.spark.read.option("header", "true").option("inferSchema", "true").csv("/etherData/ManagedSources/IFS2/IFS2_most_recent.csv")).cache()
      .withColumn("Valid_Start_Date", to_date(unix_timestamp(col("Valid_Start_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("Valid_End_Date", to_date(unix_timestamp(col("Valid_End_Date"), "yyyy-MM-dd").cast("timestamp")))

    val retailMergeIFS2DF = retail
      .join(IFS2.dropDuplicates("SKU", "Street_Price").select("SKU", "Changed_Street_Price", "Street_Price", "Valid_Start_Date", "Valid_End_Date"), Seq("SKU"), "left")
      .withColumn("Valid_Start_Date", when(col("Valid_Start_Date").isNull, dat2000_01_01).otherwise(col("Valid_Start_Date")))
      .withColumn("Valid_End_Date", when(col("Valid_End_Date").isNull, dat9999_12_31).otherwise(col("Valid_End_Date")))
      .withColumn("Street_Price", when(col("Street_Price").isNull, col("Street_Price_Org")).otherwise(col("Street_Price")))
      .where((col("Week_End_Date") >= col("Valid_Start_Date")) && (col("Week_End_Date") < col("Valid_End_Date")))
      .drop("Street_Price_Org", "Valid_Start_Date", "Valid_End_Date")

    val retailBBYBundleAccountDF = retailMergeIFS2DF.filter((col("Account") === "Best Buy") &&
      (col("Raw_POS_Qty") > col("POS_Qty")) && col("SKU").isNotNull)
      .withColumn("Special_Programs", lit("BBY Bundle"))
      .withColumn("POS_Qty", lit(col("Raw_POS_Qty") - col("POS_Qty")))

    val retailAndBBYBundleUnionWithTreatmentDF = doUnion(retailMergeIFS2DF, retailBBYBundleAccountDF).get
      .withColumn("Raw_POS_Qty", lit(null: String))
      .withColumn("POS_Qty", when(col("POS_Qty").isNull, 0))
      .withColumn("Distribution_Inv", when(col("Distribution_Inv").isNull, 0))
      .withColumn("Distribution_Inv", when(col("Distribution_Inv") < 0, 0).otherwise(col("Distribution_Inv")))
      .withColumn("Distribution_Inv", when(col("Distribution_Inv") > 1, 1).otherwise(col("Distribution_Inv")))
      .withColumn("SKU", when(col("SKU") === "J9V91A", "J9V90A").otherwise(col("SKU")))
      .withColumn("SKU", when(col("SKU") === "J9V92A", "J9V90A").otherwise(col("SKU")))
      .withColumn("SKU", when(col("SKU") === "M9L74A", "M9L75A").otherwise(col("SKU")))

    val retailAggregatePOSDF = retailAndBBYBundleUnionWithTreatmentDF
      .groupBy("SKU", "Account", "Week_End_Date", "Online", "Special_Programs")
      .agg(sum(col("POS_Qty")).as("POS_Qty"))
      .withColumn("POS_Qty", lit(null: String)) // Set POS_Qty to Null after Agg ?


    val retailJoinRetailTreatmentAndAggregatePOSDF = retailAndBBYBundleUnionWithTreatmentDF
      .join(retailAggregatePOSDF, Seq("SKU", "Account", "Week.End.Date", "Online", "Special.Programs"), "left")
      .dropDuplicates("SKU", "Account", "Week_End_Date", "Online", "Special+Programs") // duplicated line:112 R

    val SKUMapping = renameColumns(executionContext.spark.read.option("header", "true").option("inferSchema", "true").csv("/etherData/ManagedSources/S-Print/SKU_mapping/s-print_SKU_mapping.csv"))

    val GAP1 = renameColumns(executionContext.spark.read.option("header", "true").option("inferSchema", "true").csv("/etherData/ManagedSources/GAP/gap_data_full.csv")).cache()
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("Total_IR", when(col("Account") === "Best Buy" && col("SKU") === "V1N08A", 0).otherwise(col("Total_IR")))

    val adPositionDF = GAP1.filter(col("Brand").isin("HP", "Samsung"))
      .select("SKU", "Week_End_Date", "Account", "Online", "Ad_Location", "Brand", "Product")
      .filter(col("Ad_Location") =!= "NA" && col("Ad_Location") =!= 0)
      .dropDuplicates()

    val adPositionJoinSKUMappingDF = adPositionDF.join(SKUMapping, Seq("Product"), "left")
      .withColumn("SKU", when(col("Brand").isin("Samsung"), col("SKU_HP")).otherwise(col("SKU")))
      .withColumn("Product", lit(null: String))
      .withColumn("SKU_HP", lit(null: String))
      .withColumn("Brand", lit(null: String))
      .dropDuplicates()

    // TODO: data type conversion to Character
    // Ad_position$SKU <- as.character(Ad_position$SKU)
    // Ad_position$SKU_HP <- as.character(Ad_position$SKU_HP)


    val retailJoinAdPositionDF = retailJoinRetailTreatmentAndAggregatePOSDF
      .join(adPositionJoinSKUMappingDF, Seq("SKU", "Account", "Week_End_Date", "Online"), "left")

    val GAP1JoinSKUMappingDF = GAP1.join(SKUMapping, Seq("Product"), "left")
      .withColumn("SKU", when(col("Brand").isin("Samsung"), col("SKU_HP").cast("string")).otherwise(col("SKU").cast("string")))
      .withColumn("Product", lit(null: String))
      .withColumn("SKU_HP", lit(null: String))
      .dropDuplicates()

    // TODO : data type conversion to Character
    //GAP_1$SKU <- as.character(GAP_1$SKU)
    //GAP_1$SKU_HP <- as.character(GAP_1$SKU_HP)

    val GAP1AggregateDF = GAP1JoinSKUMappingDF
      .filter(col("Account").isin("Best Buy", "Office Depot-Max", "Staples") && col("Brand").isin("HP", "Samsung"))
      .groupBy("SKU", "Account", "Week_End_Date", "Online")
      .agg(max(col("Ad")).as("Ad"), max(col("Total_IR")).as("GAP_IR"))
      .withColumn("GAP_IR", lit(null: String))
      .withColumn("Days_on_Promo", lit(null: String))

    val retailJoinGAP1AggregateDF = retailJoinAdPositionDF
      .join(GAP1AggregateDF.select("SKU", "Account", "Week_End_Date", "Online", "GAP_IR", "Ad"), Seq("SKU", "Account", "Week_End_Date", "Online"), "left")
      .withColumn("GAP_IR", when(col("GAP_IR").isNull, 0).otherwise(col("GAP_IR")))
      .withColumn("Ad", when(col("Ad").isNull, 0).otherwise(col("Ad")))

    val aggregateGAP1Days = GAP1JoinSKUMappingDF
      .filter(col("Account").isin("Costco", "Sam's Club") && col("Brand").isin("HP"))
      .groupBy("SKU", "Account", "Week_End_Date")
      .agg(max(col("Days_on_Promo")).as("Days_on_Promo"))

    val retailJoinAggregateGAP1DaysDF = retailJoinGAP1AggregateDF
      .join(aggregateGAP1Days.select("SKU", "Account", "Week_End_Date", "Days_on_Promo"), Seq("SKU", "Account", "Week_End_Date"), "left")
      .withColumn("Days_on_Promo", when(col("Days_on_Promo").isNull, 0).otherwise(col("Days_on_Promo")))

    val generateUUID = udf(() => UUID.randomUUID().toString)
    var adAccountDF = GAP1AggregateDF // TODO: spread(aggregate1_GAP,Account,Ad)
      .withColumn("uuid", lit(generateUUID))
      .groupBy("SKU", "uuid", "Week_End_Date", "Online")
      .pivot("Account")
      .agg(first(col("Ad")))
      .drop("uuid")

    val accountList = List("Best Buy", "Office Depot-Max", "Staples")
    val adAccountDFColumns = adAccountDF.columns

    // check if all columns exists and if not then assign null
    accountList.foreach(x => {
      if (!adAccountDFColumns.contains(x))
        adAccountDF = adAccountDF.withColumn(x, lit(null))
    })

    // NA treatment
    accountList.foreach(account => {
      adAccountDF = adAccountDF.withColumn(account, when(col(account).isNull, 0).otherwise(col(account)))
        .withColumnRenamed(account, "Ad_" + account)
    })

    // TODO : is NA treatment necessary here ?
    val retailJoinAdAccountDF = retailJoinAggregateGAP1DaysDF
      .join(adAccountDF, Seq("SKU", "Week_End_Date", "Online"), "left")
      .withColumn("Ad_Best Buy", when(col("Ad_Best Buy").isNull, 0).otherwise(col("Ad_Best Buy")))
      .withColumn("Ad_Office Depot-Max", when(col("Ad_Office Depot-Max").isNull, 0).otherwise(col("Ad_Office Depot-Max")))
      .withColumn("Ad_Staples", when(col("Ad_Staples").isNull, 0).otherwise(col("Ad_Staples")))

    val GAP2 = GAP1JoinSKUMappingDF
      .groupBy("Bran", "SKU", "Account", "Week_End_Date", "L2_Category", "L1_Category", "Category_1", "Category_2")
      .agg(max(col("Ad")).as("Ad"))

    val compAd = GAP2
      .groupBy("L2_Category", "Brand", "Week_End_Date", "Account")
      .agg(mean(col("Ad")).as("Ad_avg"), sum(col("Ad")).as("Ad_total"))
      .filter(!col("Brand").isin("Xerox"))
      .withColumn("Ad_total", lit(null: String))

    var compAdAvgDF = compAd.withColumn("uuid", lit(generateUUID))
      .groupBy("L2_Category", "uuid", "Week_End_Date", "Account")
      .pivot("Brand")
      .agg(first("Ad_avg"))
      .drop("uuid")

    val allBrands = List("Brother", "Canon", "Epson", "Lexmark", "Samsung", "HP")
    val compAd2Columns = compAdAvgDF.columns
    allBrands.foreach(x => {
      if (!compAd2Columns.contains(x))
        compAdAvgDF = compAdAvgDF.withColumn(x, lit(null))
    })
    allBrands.foreach(brand => {
      compAdAvgDF = compAdAvgDF.withColumn(brand, when(col(brand).isNull, 0).otherwise(col(brand)))
        .withColumnRenamed(brand, "Ad_ratio_" + brand)
    })

    var retailJoinCompAdAvgDF = retailJoinAdAccountDF
      .join(compAdAvgDF, Seq("L2_Category", "Week_End_Date", "Account"), "left")

    allBrands.foreach(brand => {
      retailJoinCompAdAvgDF = retailJoinCompAdAvgDF.withColumn(brand, when(col(brand).isNull, 0).otherwise(col(brand)))
    })

    compAdAvgDF = compAdAvgDF.withColumn("Ad_avg", lit(null: String))

    // TODO : Need to verify Ad_total for aggregation
    var compAdTotalDF = compAd.withColumn("uuid", lit(generateUUID))
      .groupBy("L2_Category", "uuid", "Week_End_Date", "Account")
      .pivot("Brand")
      .agg(first("Ad_total"))
      .drop("uuid")

    val compAdTotalColumns = compAdTotalDF.columns
    allBrands.foreach(x => {
      if (!compAdTotalColumns.contains(x))
        compAdTotalDF = compAdTotalDF.withColumn(x, lit(null))
    })
    allBrands.foreach(brand => {
      compAdTotalDF = compAdTotalDF.withColumn(brand, when(col(brand).isNull, 0).otherwise(col(brand)))
        .withColumnRenamed(brand, "Ad_total_" + brand)
    })

    var retailJoincompAdTotalDFDF = retailJoinCompAdAvgDF
      .join(compAdTotalDF, Seq("L2_Category", "Week_End_Date", "Account"), "left")

    allBrands.foreach(brand => {
      retailJoincompAdTotalDFDF = retailJoincompAdTotalDFDF.withColumn(brand, when(col(brand).isNull, 0).otherwise(col(brand)))
    })

    retailJoincompAdTotalDFDF.withColumn("Total_Ad_No_", lit(col("Ad_total_Brother") +
      col("Ad_total_Canon") + col("Ad_total_Lexmark") + col("Ad_total_Samsung") + col("Ad_total_Epson") + col("Ad_total_HP")))


    allBrands.foreach(brand => {
      retailJoincompAdTotalDFDF = retailJoincompAdTotalDFDF.withColumn("Ad_share_" + brand, when((col(brand) / col("Total_Ad_No_")).isNull, 0)
        .otherwise(col(brand) / col("Total_Ad_No_")))
    })

    retailJoincompAdTotalDFDF.withColumn("Total_Ad_No_", lit(null: String))
    retailJoincompAdTotalDFDF.withColumn("Ad_Location", when(col("Ad_Location").isNull, "No_Ad").otherwise(col("Ad_Location").cast("string")))
    retailJoincompAdTotalDFDF.withColumn("Ad_Location", when(col("Ad_Best Buy") === 1 && col("Ad_Location") === "No_Ad", "No_Info")
      .when(col("Ad_Office Depot-Max") === 1 && col("Ad_Location") === "No_Ad", "No_Info")
      .when(col("Ad_Staples") === 1 && col("Ad_Location") === "No_Ad", "No_Info")
      .otherwise(col("Ad_Location").cast("string")))
      .withColumn("Ad_Location_LEVELS", col("Ad_Location"))
      .withColumn("Ad_Location", when(col("Ad_Location") === "No_Ad", "AAANo_Ad").otherwise(col("Ad_Location"))) // relevel first then apply string indexer
    val indexer = new StringIndexer().setInputCol("Ad_Location").setOutputCol("Ad_Location_fact")
    val pipeline = new Pipeline().setStages(Array(indexer))
    retailJoincompAdTotalDFDF = pipeline.fit(retailJoincompAdTotalDFDF).transform(retailJoincompAdTotalDFDF)
      .drop("Ad_Location").withColumnRenamed("Ad_Location_fact", "Ad_Location").drop("Ad_Location_fact")


    //    val retailAdLocationmFactDF = pipelineForForAdLocation.fit(retailJoincompAdTotalDFDF).transform(retailJoincompAdTotalDFDF)
    //      .drop("Ad_Location").withColumnRenamed("Ad_Location_fact", "Ad_Location").drop("Ad_Location_fact")
    //      .withColumn("Ad_Location", (col("Ad_Location") + lit(1)).cast("int"))
    //      .withColumn("Ad_Location_LEVELS", col("Ad_Location"))

    val calendar = renameColumns(executionContext.spark.read.option("header", "true").option("inferSchema", "true").csv("/etherData/ManagedSources/Calendar/Retail/master_calendar_retail.csv")).cache()
      .filter(!col("Account").isin("Rest of Retail"))
      .withColumn("Account", when(col("Account").isin("Amazon.Com"), "Amazon-Proper").otherwise(col("Account")))
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("NP_IR_original", lit(col("NP_IR")))
      .withColumn("ASP_IR_original", lit("ASP.IR"))
      .withColumn("Season", lit(null: String))

    var retailJoinCalendarDF = retailJoincompAdTotalDFDF
      .join(calendar, Seq("Account", "SKU", "Week_End_Date", "left"))
      .withColumn("Merchant_Gift_Card", when(col("Merchant_Gift_Card").isNull, 0).otherwise(col("Merchant_Gift_Card")))
      .withColumn("Flash_IR", when(col("Online") === 0, 0).otherwise(col("Flash_IR")))
      .withColumn("Flash_IR", when(col("Flash_IR").isNull, 0).otherwise(col("Flash_IR")))
      .filter(col("Merchant_Gift_Card") > 0)

    val uniqueSKUNames = retailJoinCalendarDF.select("SKU_Name").distinct().collectAsList()

    retailJoinCalendarDF = retailJoinCalendarDF.withColumn("GC_SKU_Name", when(col("SKU_Name").isin(uniqueSKUNames), col("SKU_Name").cast("string"))
      .otherwise(lit("NA")))

    val SKUWhoChange = retailJoinCalendarDF.filter(col("Changed_Street_Price") =!= 0).select("SKU").distinct().collectAsList()

    // TODO : check col data type for pmax UDF
    retailJoinCalendarDF = retailJoinCalendarDF.withColumn("GAP_IR", when((col("SKU").isin(SKUWhoChange)) && (col("Season").isin("BTB'18")), col("NP_IR"))
      .otherwise(col("GAP_IR")))
      .withColumn("GAP_IR", when(col("GAP_IR").isNull, 0).otherwise(col("GAP_IR")))
      .withColumn("Other_IR_original", when(col("NP_IR_original").isNull && col("ASP.IR.original").isNull, col("GAP_IR")).otherwise(0))
      .withColumn("NP_IR_original", when(col("NP_IR_original").isNull, 0).otherwise(col("NP_IR_original")))
      .withColumn("ASP_IR_original", when(col("ASP_IR_original").isNull, 0).otherwise(col("NP_IR_original")))
      .withColumn("Total_IR_original", lit(pmax(col("NP_IR_original"), col("ASP_IR_original"), col("Other_IR_original"))))
      .withColumn("NP_IR_original", when(col("Flash_IR") > 0, col("Flash__IR")).otherwise(col("NP_IR_original")))
      .withColumn("NP_IR", lit(col("NP_IR_original")))
      .withColumn("Total_IR", lit(pmax(col("NP_IR_original"), col("ASP_IR_original"), col("GAP_IR"))))
      .withColumn("ASP_IR", when(col("ASP_IR_original") >= col("Total_IR") - col("NP_IR"), lit(col("Total_IR") - col("NP_IR"))).otherwise(col("ASP_IR_original")))
      .withColumn("Other_IR", lit(col("Total_IR") - (col("NP_IR") + col("ASP_IR"))))
      .withColumn("Ad", when(col("Total_IR") === 0, 0).otherwise(col("Ad")))

    val masterSprintCalendar = renameColumns(executionContext.spark.read.option("header", "true").option("inferSchema", "true").csv("/etherData/ManagedSources/S-Print/Master_Calendar/Master_Calender_s-print.csv"))
      .select("Account", "SKU", "Rebate_SS", "Week_End_Date")

    val retailJoinMasterSprintCalendarDF = retailJoinCalendarDF
      .join(masterSprintCalendar, Seq("SKU", "Account", "Week_End_Date"), "left")
      .withColumn("NP_IR", when(col("Brand").isin("Samsung"), col("Rebate_SS")).otherwise(col("NP_IR")))
      .withColumn("NP_IR", when(col("Brand").isNull, 0).otherwise(col("NP_IR")))
      .withColumn("Rebate_SS", lit(null: String))

    val aggUpstream = renameColumns(executionContext.spark.read.option("header", "true").option("inferSchema", "true").csv("/etherData/ManagedSources/Upstream/AggUpstream.csv")).cache()

    val focusedAccounts = List("HP Shopping", "Amazon-Proper", "Best Buy", "Office Depot-Max", "Staples")

    val retailJoinAggUpstreamDF = retailJoinMasterSprintCalendarDF
      .join(aggUpstream, Seq("SKU", "Account", "Week_End_Date", "Online"), "right")
      .withColumn("NoAvail", when(col("InStock").isNull && col("OnlyInStore").isNull && col("OutofStock").isNull && col("DelayDel").isNull, 1).otherwise(0))
      .withColumn("NoAvail", when(col("POS_Qty") > 0 && col("OutofStock").isin(7), 1).otherwise(col("NoAvail")))
      .withColumn("OutofStock", when(col("POS_Qty") > 0 && col("OutofStock").isin(7), 0).otherwise(col("OutofStock")))
      .withColumn("InStock", when(col("InStock").isNull, 0).otherwise(col("InStock")))
      .withColumn("DelayDel", when(col("DelayDel").isNull, 0).otherwise(col("DelayDel")))
      .withColumn("OutofStock", when(col("OutofStock").isNull, 0).otherwise(col("OutofStock")))
      .withColumn("OnlyInStore", when(col("OnlyInStore").isNull, 0).otherwise(col("OnlyInStore")))
      .withColumn("NoAvail", when(col("NoAvail").isNull, 0).otherwise(col("NoAvail")))
      .withColumn("GAP_Price", lit(col("Street_Price") - col("Total_IR")))
      .withColumn("ImpAve", when(col("Ave").isNull && col("GAP_Price").isNotNull, col("GAP_Price")).otherwise(col("Ave")))
      .withColumn("ImpMin", when(col("Min").isNull && col("GAP_Price").isNotNull, col("GAP_Price")).otherwise(col("Min")))
      .filter(col("Account").isin(focusedAccounts))

    val spreadPriceDF = retailJoinAggUpstreamDF
      .filter(col("Special.Programs").isin("None"))
      .select("SKU,Account", "Week_End_Date", "Online", "ImpAve", "ImpMin")
      // TODO : reshape(timevar = "Account", idvar = c("SKU", "Week.End.Date","Online"), direction = "wide")
      .select("SKU", "Week_End_Date", "Online", "ImpAve_AmazonProper", "ImpMin_AmazonProper", "ImpAve_BestBuy", "ImpMin_BestBuy", "ImpAve_HPShopping", "ImpMin_HPShopping", "ImpAve_OfficeDepotMax", "ImpMin_OfficeDepotMax", "ImpAve_Staples", "ImpMin_Staples")

    val retailJoinSpreadPrice = retailJoinAggUpstreamDF
      .join(spreadPriceDF, Seq("SKU", "Week_End_Date", "Online"), "left")
      .withColumn("ImpAve_AmazonProper", when(col("ImpAve_AmazonProper").isNull, col("Street_Price")).otherwise(col("ImpAve_AmazonProper")))
      .withColumn("ImpMin_AmazonProper", when(col("ImpMin_AmazonProper").isNull, col("Street_Price")).otherwise(col("ImpMin_AmazonProper")))
      .withColumn("ImpAve_BestBuy", when(col("ImpAve_BestBuy").isNull, col("Street_Price")).otherwise(col("ImpAve_AmazonProper")))
      .withColumn("ImpMin_BestBuy", when(col("ImpMin_BestBuy").isNull, col("Street_Price")).otherwise(col("ImpMin_BestBuy")))
      .withColumn("ImpAve_HPShopping", when(col("ImpAve_HPShopping").isNull, col("Street_Price")).otherwise(col("ImpAve_HPShopping")))
      .withColumn("ImpMin_HPShopping", when(col("ImpMin_HPShopping").isNull, col("Street_Price")).otherwise(col("ImpMin_HPShopping")))
      .withColumn("ImpAve_OfficeDepotMax", when(col("ImpAve_OfficeDepotMax").isNull, col("Street_Price")).otherwise(col("ImpAve_OfficeDepotMax")))
      .withColumn("ImpMin_OfficeDepotMax", when(col("ImpMin_OfficeDepotMax").isNull, col("Street_Price")).otherwise(col("ImpMin_OfficeDepotMax")))
      .withColumn("ImpAve_Staples", when(col("ImpAve_Staples").isNull, col("Street_Price")).otherwise(col("ImpAve_Staples")))
      .withColumn("ImpMin_Staples", when(col("ImpMin_Staples").isNull, col("Street_Price")).otherwise(col("ImpMin_Staples")))


      val amz = renameColumns(executionContext.spark.read.option("header", "true").option("inferSchema", "true").csv("/etherData/ManagedSources/Sales/Amazon_Sales/amazon_sales_price.csv")).cache()

    val retailAMZMergeDF = retailJoinSpreadPrice
      .join(amz, Seq("SKU", "Week_End_Date", "Account", "Online"), "left")
      .withColumn("AMZ_Sales_Price", when(col("AMZ_Sales_Price") < 10 || col("AMZ_Sales_Price").isNull, col("ImpMin_AmazonProper")).otherwise(col("AMZ_Sales_Price")))
      .withColumn("ImpMin", when(col("Account").isin("Amazon-Proper"), col("AMZ_Sales_Price")).otherwise(col("ImpMin")))
      .withColumn("ImpMin", when(col("Flash_IR") > 0 && (col("Flash_IR") =!= (col("Street_Price") - col("ImpMin"))), col("Street_Price") - col("Flash_IR")).otherwise(col("ImpMin")))
      .withColumn("Other_IR", when(col("Online") === 1,
        when((col("Street.Price") - col("ImpMin") - col("NP_IR") - col("ASP.IR")) > 5, col("Street.Price") - col("ImpMin") - col("NP_IR") - col("ASP.IR")).otherwise(0))
        .otherwise(col("Other_IR")))
      .withColumn("Total_IR", lit(col("NP_IR") + col("ASP_IR") + col("Other_IR")))

    var retailUnionRetailOtherAccountsDF = retailAMZMergeDF.union(retailJoinAggUpstreamDF)

    retailUnionRetailOtherAccountsDF = retailUnionRetailOtherAccountsDF
      .withColumn("Promo_Flag", when(col("Total_IR") > 0, 1).otherwise(0))
      .withColumn("NP_Flag", when(col("NP_IR") > 0, 1).otherwise(0))
      .withColumn("ASP_Flag", when(col("ASP_IR") > 0, 1).otherwise(0))
      .withColumn("Other_IR_Flag", when(col("ASP_IR") > 0, 1).otherwise(0))
      .withColumn("Promo_Pct", lit(col("Total_IR")) / col("Street_Price"))
      .withColumn("Promo_Pct", when(col("Promo_Pct") === 0, lit("No Discount")
        .when(col("Promo_Pct") <= 0.2, lit("Very Low"))
        .when(col("Promo_Pct") <= 0.3, lit("Low"))
        .when(col("Promo_Pct") <= 0.4, lit("Moderate"))
        .when(col("Promo_Pct") <= 0.5, lit("Heavy"))
        .otherwise(lit("Very Heavy"))))
      .withColumn("log_POS_Qty", log(col("POS_Qty")))
      .withColumn("log_POS_Qty", when(col("log_POS_Qty").isNull, 0).otherwise(col("log_POS_Qty")))
      .withColumn("log_POS_Qty", log(lit(1) - col("POS_Qty")))
      .withColumn("price", when(col("price").isNull, 0).otherwise(col("price")))

    var retailEOL = retailUnionRetailOtherAccountsDF.select("SKU", "ES_date", "GA_date").dropDuplicates()
      .filter(col("ES_date").isNotNull && col("GA_date").isNotNull)
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date_wday", dayofweek(col("ES_date")).cast("int")) //As dayofweek returns in range 1-7 we want 0-6
      .withColumn("GA_date_wday", dayofweek(col("GA_date")).cast("int"))
      .withColumn("GA_date", addDaystoDateStringUDF(col("GA_date").cast("timestamp").cast("string"), lit(7) - col("GA_date_wday")))
      .withColumn("ES_date", addDaystoDateStringUDF(col("ES_date").cast("timestamp").cast("string"), lit(7) - col("ES_date_wday")))
      .drop("GA_date_wday", "ES_date_wday")

    val windForSKUAndAccount = Window.partitionBy("SKU&Account").orderBy("SKU", "Account", "Week_End_Date")
    var EOLcriterion = retailUnionRetailOtherAccountsDF
      .groupBy("SKU", "Account", "Week_End_Date")
      .agg(max("Distribution_Inv").as("Distribution_Inv"))
      .join(retailUnionRetailOtherAccountsDF, Seq("SKU", "Account", "Week_End_Date"), "right")
      .sort("SKU", "Account", "Week_End_Date")
      .withColumn("SKU&Account", concat(col("SKU"), col("Account")))

    EOLcriterion = EOLcriterion
      .withColumn("dist_threshold", lit(col("Distribution_Inv") * (lit(1) - stability_range)))
      .withColumn("rank", rank().over(windForSKUAndAccount))
      .withColumn("EOL_criterion", when(col("rank") <= stability_weeks || col("Distribution_Inv") < col("dist_threshold"), 0).otherwise(1))

    val EOLWithCriterion1 = EOLcriterion.orderBy("SKU_Name", "Account", "Week_End_Date").groupBy("SKU&Account").agg(collect_list(struct(col("rank"), col("Distribution_Inv"))).as("DistInvArray"))
      .withColumn("DistInvArray", concatenateRank(col("DistInvArray")))

    // TODO : check EOL UDF for else condition
    EOLcriterion = EOLcriterion.join(EOLWithCriterion1, Seq("SKU&Account"), "left")
      .where(col("EOL_criterion")===1)
      .withColumn("EOL_criterion", checkPrevDistInvGTBaseline(col("DistInvArray"), col("rank"), col("no_promo_med")))
      .drop("rank", "DistInvArray", "SKU&Account", "Qty&no_promo_med")

    val EOLCriterionLast = EOLcriterion.where(col("EOL_criterion") === 1)
      .groupBy("SKU_Name", "Account")
      .agg(max("Week_End_Date").as("last_date"))
      .join(EOLcriterion.where(col("EOL_criterion") === 1), Seq("SKU_Name", "Account"), "right")

    val EOLCriterionMax = retailEOL
      .groupBy("SKU_Name", "Account")
      .agg(max("Week_End_Date").as("max_date"))
      .join(retailEOL, Seq("SKU_Name", "Account"), "right")

    val EOLCriterionMin = retailEOL
      .groupBy("SKU_Name", "Account")
      .agg(min("Week_End_Date").as("max_date"))
      .join(retailEOL, Seq("SKU_Name", "Account"), "right")

    val EOLMaxJoinLastDF = EOLCriterionMax
      .join(EOLCriterionLast, Seq("SKU", "Account"), "left")

    val EOLMaxLastJoinMinDF = EOLMaxJoinLastDF
      .join(EOLCriterionMin, Seq("SKU", "Account"), "left")

    val maxMaxDate = EOLMaxLastJoinMinDF.agg(max("max_date")).head().getDate(0)

    var EOLNATreatmentDF = EOLMaxLastJoinMinDF
      .withColumn("last_date", when(col("last_date").isNull, col("max_date")).otherwise(col("last_date")))
      .withColumn("min_date", lit(null: String))
      .filter(col("max_date") === col("last_date") && col("max_date") === maxMaxDate)
      .withColumn("diff_weeks", ((col("max_date") - col("last_date")) / 7) + 1)

//    EOL_criterion <- EOL_criterion[rep(row.names(EOL_criterion), EOL_criterion$diff_weeks),]
//      EOL_criterion$add <- t(as.data.frame(strsplit(row.names(EOL_criterion), "\\.")))[,2]# t = transpose
//      EOL_criterion$add <- ifelse(grepl("\\.",row.names(EOL_criterion))==FALSE,0,as.numeric(EOL_criterion$add))
//      EOL_criterion$add <- EOL_criterion$add*7
//      EOL_criterion$Week.End.Date <- EOL_criterion$last_date+EOL_criterion$add

    EOLNATreatmentDF = EOLNATreatmentDF.withColumn("diff_weeks", when(col("diff_weeks").isNull || col("diff_weeks") <= 0, 0).otherwise(col("diff_weeks")))
      .withColumn("repList", createlist(col("diff_weeks"))).withColumn("add", explode(col("repList"))).drop("repList")
      .withColumn("add", col("add") * lit(7))
      .withColumn("Week_End_Date", addDaystoDateStringUDF(col("last_date"), col("add"))) //CHECK: check if min_date is in timestamp format!
      .drop("max_date", "last_date", "diff_weeks", "add")
      .withColumn("EOL_criterion", lit(1))


    retailEOL = retailEOL
      .join(EOLNATreatmentDF, Seq("SKU", "Account", "Week_End_Date"), "left")
      .withColumn("EOL_criterion", when(col("EOL_criterion").isNull, 0).otherwise(col("EOL_criterion")))
      .withColumn("EOL_criterion_old", col("EOL_criterion"))

    var retailEOLDates = renameColumns(executionContext.spark.read.option("header", "true").option("inferSchema", "true").csv("/etherData/ManagedSources/Calendar/EOL_Dates/EOL_Dates_Retail.csv")).cache()

    retailEOL = retailEOL
      .join(retailEOLDates, Seq("Account", "SKU"), "left")
      .withColumn("EOL_Date", to_date(unix_timestamp(col("EOL_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("EOL_criterion", when(col("WeeK_End_Date") >= col("EOL_Date"), 0).otherwise(lit(0)))
      .withColumn("EOL_criterion", when(col("EOL_criterion").isNull, col("EOL_criterion_old")).otherwise(col("EOL_criterion")))
      .withColumn("EOL_criterion", when(col("WeeK_End_Date") >= col("ES_date"), 1).otherwise(col("EOL_criterion")))
      .withColumn("EOL_criterion", when(col("Account") === "Sam's Club",0).otherwise(col("EOL_criterion")))
      .withColumn("EOL_criterion", when(col("Online") === 1 && col("POS_Qty") > 0,0).otherwise(col("EOL_criterion")))

    var BOL = retailEOL.select("SKU","ES_date","GA_date")
      .dropDuplicates()
      .where((col("ES_date").isNotNull) || (col("GA_date").isNotNull))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"),"yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"),"yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date_wday", dayofweek(col("ES_date")).cast("int"))  //As dayofweek returns in range 1-7 we want 0-6
      .withColumn("GA_date_wday", dayofweek(col("GA_date")).cast("int"))
      .withColumn("GA_date", addDaystoDateStringUDF(col("GA_date").cast("timestamp").cast("string"), lit(7)-col("GA_date_wday")))
      .withColumn("ES_date", addDaystoDateStringUDF(col("ES_date").cast("timestamp").cast("string"), lit(7)-col("ES_date_wday")))
      .drop("GA_date_wday","ES_date_wday")

    val windForSKUnReseller = Window.partitionBy("SKU$Reseller")
    var BOLCriterion = retailEOL
      .groupBy("SKU","Account","Week_End_Date")
      .agg(sum("Distribution_Inv").as("Distribution_Inv"))
      .sort("SKU","Account","Week_End_Date")
      .withColumn("rank", rank().over(windForSKUnReseller))
      .withColumn("BOL_criterion", when(col("rank")<intro_weeks, 0).otherwise(1)) //BOL_criterion$BOL_criterion <- ave(BOL_criterion$Qty, paste0(BOL_criterion$SKU, BOL_criterion$Reseller.Cluster), FUN = BOL_criterion_v3)
      .drop("rank")
      .drop("Distribution_Inv")

    BOLCriterion = BOLCriterion.join(BOL.select("SKU","GA_date"), Seq("SKU"), "left")
    val minWEDDate = to_date(unix_timestamp(lit(BOLCriterion.agg(min("Week_End_Date")).head().getString(0)),"yyyy-MM-dd").cast("timestamp"))

    BOLCriterion = BOLCriterion.withColumn("GA_date", when(col("GA_date").isNull, minWEDDate).otherwise(col("GA_date")))
      .where(col("Week_End_Date")>=col("GA_date"))

    ////////////

    val BOLCriterionFirst = BOLCriterion.where(col("BOL_criterion")===1)
      .groupBy("SKU","Account")
      .agg(min("Week_End_Date").as("first_date"))
      .join(BOLCriterion.where(col("BOL_criterion")===1), Seq("SKU","Account"), "right")

    val BOLCriterionMax = retailEOL
      .groupBy("SKU","Account")
      .agg(min("Week_End_Date").as("max_date"))
      .join(retailEOL, Seq("SKU","Account"), "right")

    val BOLCriterionMin = BOLCriterion.where(col("BOL_criterion")===1)
      .groupBy("SKU","Account")
      .agg(min("Week_End_Date").as("min_date"))
      .join(retailEOL, Seq("SKU","Account"), "right")

    BOLCriterion =  BOLCriterionMax.join(BOLCriterionFirst, Seq("SKU","Account"), "left")
      .join(BOLCriterionMin, Seq("SKU","Account"), "left")

    BOLCriterion = BOLCriterion.withColumn("first_date", when(col("first_date").isNull, col("max_date")).otherwise(col("first_date")))
      .drop("max_date")
    val minMinDateBOL = BOLCriterion.agg(min("min_date")).head().getString(0)
    BOLCriterion = BOLCriterion
      .where(!((col("min_date")===col("first_date")) && (col("first_date")===minMinDateBOL)))
      .withColumn("diff_weeks", ((col("first_date")-col("min_date"))/7)+1)

    /*BOL_criterion <- BOL_criterion[rep(row.names(BOL_criterion), BOL_criterion$diff_weeks),]
    * BOL_criterion$add <- t(as.data.frame(strsplit(row.names(BOL_criterion), "\\.")))[,2]
    BOL_criterion$add <- ifelse(grepl("\\.",row.names(BOL_criterion))==FALSE,0,as.numeric(BOL_criterion$add))
    BOL_criterion$add <- BOL_criterion$add*7
    BOL_criterion$Week.End.Date <- BOL_criterion$min_date+BOL_criterion$add
    **/
    BOLCriterion = BOLCriterion.withColumn("diff_weeks", when(col("diff_weeks").isNull || col("diff_weeks")<=0, 0).otherwise(col("diff_weeks")))
      .withColumn("repList", createlist(col("diff_weeks"))).withColumn("add", explode(col("repList"))).drop("repList")
      .withColumn("add", col("add")*lit(7))
      .withColumn("Week_End_Date", addDaystoDateStringUDF(col("min_date"), col("add")))   //CHECK: check if min_date is in timestamp format!

    BOLCriterion = BOLCriterion.drop("min_date","fist_date","diff_weeks","add")
      .withColumn("BOL_criterion", lit(1))


    retailEOL = retailEOL.join(BOLCriterion, Seq("SKU","Account","Week_End_Date"), "left")
      .withColumn("BOL_criterion", when(col("BOL_criterion").isNull, 0).otherwise(col("BOL_criterion")))
      .withColumn("BOL_criterion", when(datediff(col("Week_End_Date"),col("GA_date"))<(7*6),1).otherwise(col("BOL_criterion")))

  }
}
