package com.scienaptic.jobs.core.pricing.retail

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.CommercialUtility.createlist
import com.scienaptic.jobs.utility.Utils.renameColumns
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object RetailPreRegressionPart22 {

  val TEMP_OUTPUT_DIR = "/etherData/retailTemp/RetailFeatEngg/temp/preregression_output_retail.csv"

  def execute(executionContext: ExecutionContext): Unit = {
    val spark: SparkSession = executionContext.spark

    var retailWithCompCann3DF = renameColumns(spark.read.option("header",true).option("inferSchema",true).csv("/etherData/retailTemp/RetailFeatEngg/preregression__before_output_retail.csv"))
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("EOL_Date", to_date(unix_timestamp(col("EOL_Date"), "yyyy-MM-dd").cast("timestamp"))).cache()

    // walmart
    val skuWalmart = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/managedSources/Walmart_link_checks/walmart_link.csv")
      .withColumnRenamed("retail_sku", "SKU")
      .withColumnRenamed("retail_sku_desc", "SKU_Name")

    retailWithCompCann3DF = retailWithCompCann3DF
      .join(skuWalmart.select("SKU", "walmart_sku"), Seq("SKU"), "left")

    /* CR1 - wmt_prereg_w_dropins source removed. Retail filtered for Walmart Account for this - Start */
    var walmart = retailWithCompCann3DF.where(col("Account")==="Walmart")
    /*var walmart = renameColumns(executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/managedSources/Walmart/wmt_prereg_w_dropins_1_30_19.csv"))
    walmart.columns.toList.foreach(x => {
      walmart = walmart.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })*/
    /* CR1 - wmt_prereg_w_dropins source removed. Retail filtered for Walmart Account for this - End   */

    walmart = walmart
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
      .select("SKU", "Week_End_Date", "avg_price")
      .cache()

    val walmartGroupAndAgg = walmart.groupBy("SKU", "Week_End_Date").agg(min(col("avg_price")).as("Walmart_Price"))
        .withColumnRenamed("SKU","walmart_sku")     // CR1 - walmart_sku created from SKU.

    retailWithCompCann3DF = retailWithCompCann3DF
      .join(walmartGroupAndAgg, Seq("walmart_sku", "Week_End_Date"), "left")    // CR1 - SKU modified to walmart_sku for joining criteria
      .withColumn("Delta_Price_Walmart", log("Selling_Price") - log("Walmart_Price"))
      .withColumn("Price_Gap_Walmart", when((col("Selling_Price") - col("Walmart_Price")) > 0, col("Selling_Price") - col("Walmart_Price")).otherwise(lit(0)))
      .withColumn("Price_Gap_Walmart", when(col("Price_Gap_Walmart").isNull, 0).otherwise(col("Price_Gap_Walmart")))
      .withColumn("Ad", when(col("Ad").isNull, 0).otherwise(col("Ad")))
      .withColumn("exclude", when(col("exclude").isNull, 0).otherwise(col("exclude")))
      .withColumn("Days_on_Promo", when(col("Total_IR") > 0 && col("Days_on_Promo") === 0, 7).otherwise(col("Days_on_Promo")))

    /* CR1 - New source added EndCap - Start */
    var endcap = renameColumns(spark.read.option("header", true).option("inferSchema", true).csv("/etherData/managedSources/Walmart_link_checks/endcap_weekly_prereg.csv"))
      //TODO: Check format in production
        .withColumn("Week_End_Date", to_date(col("Week_End_Date")))
    endcap = endcap.groupBy("Account","SKU","Week_End_Date")
        .agg(max("weekly_endcap_flag").as("weekly_endcap_flag"))
    retailWithCompCann3DF = retailWithCompCann3DF.join(endcap, Seq("Account","SKU","Week_End_Date"), "left")
        .withColumn("weekly_endcap_flag", when(col("Online")===1, 0).otherwise(col("weekly_endcap_flag")))
        .na.fill(0, Seq("weekly_endcap_flag"))
    /* CR1 - New source added EndCap - End */

    retailWithCompCann3DF = retailWithCompCann3DF.withColumn("Week_End_Date", date_format(to_date(col("Week_End_Date"), "yyyy-MM-dd"), "MM/dd/yyyy"))
      .withColumn("Week_End_Date", col("Week_End_Date").cast("string"))

    //val currentTS = spark.read.json("/etherData/state/currentTS.json").select("ts").head().getString(0)

    retailWithCompCann3DF = retailWithCompCann3DF           //CR1 - Confirmed by Sagnika. Special_Programs will remain as 'None' for Walmart Account.
        .withColumn("Special_Programs", when(col("Account")==="Walmart", "None").otherwise(col("Special_Programs")))

    val currentTS = spark.read.json("/etherData/state/currentTS.json").select("ts").head().getString(0)

    /* CR1 - New features added; some removed */
    //TODO: Uncomment Special_Programs_y (Created in part 10)
    retailWithCompCann3DF = retailWithCompCann3DF.select("Account", "SKU", "Week_End_Date" /*"mnth"*/, "walmart_sku", "Online", "trend", "SKU_Name", "L1_Category",
      "Category", "L2_Category", "Special_Programs"/*, "Special_Programs_y"*/,"SKU_number", "Ave", "Min", "Max", "InStock", "DelayDel", "OutofStock", "OnlyInStore",
      "IPSLES", "Season", "Category_Subgroup", "Line", "PL", "GA_date", "ES_date", "Category_1", "Category_2", "Category_3",
      "HPS/OPS", "Series", "Category Custom", "Brand", "Max_Week_End_Date", "Season_Ordered", "Cal_Month", "Cal_Year", "Fiscal_Qtr", "Fiscal_Year",
      "Changed_Street_Price", "POS_Qty", "Distribution_Inv", "Ad_Location", "GAP_IR", "Ad", "Days_on_Promo", "Ad_Best Buy", "Ad_Office Depot-Max", "Ad_Staples",
      "Ad_ratio_Brother", "Ad_ratio_Canon", "Ad_ratio_Epson", "Ad_ratio_HP", "Ad_ratio_Lexmark", "Ad_ratio_Samsung", "Ad_total_Brother",
      "Ad_total_Canon", "Ad_total_Epson", "Ad_total_HP", "Ad_total_Lexmark", "Ad_total_Samsung", "Ad_share_Brother", "Ad_share_Canon",
      "Ad_share_Lexmark", "Ad_share_Samsung", "Ad_share_Epson", "Ad_share_HP", "NP_IR", "ASP_IR", "IPS", "NP_Program_ID", "ASP_Program_ID",
      "ASP_Planned_Units", "ASP_Planned_Spend", "BOPIS", "Merchant_Gift_Card", "Flash_IR", "NP_IR_original", "ASP_IR_original",
      "GC_SKU_Name", "Bundle_Flag","Bundle_IR" ,"HP_Funding" ,"Other_IR_original", "Total_IR_original", "Total_IR", "Other_IR", "NoAvail", "GAP_Price", "ImpAve", "ImpMin",
      "ImpAve_AmazonProper", "ImpMin_AmazonProper", "ImpAve_BestBuy", "ImpMin_BestBuy", "ImpAve_HPShopping", "ImpMin_HPShopping",
      "ImpAve_OfficeDepotMax", "ImpMin_OfficeDepotMax", "ImpAve_Staples", "ImpMin_Staples", "AMZ_Sales_Price", "LBB", "Promo_Flag",
      "NP_Flag", "ASP_Flag", "Other_IR_Flag", "Promo_Pct", "Discount_Depth_Category", "price", "EOL_criterion", "EOL_criterion_old",
      "EOL_Date", "BOL_criterion", "USChristmasDay", "USColumbusDay", "USIndependenceDay", "USLaborDay", "USLincolnsBirthday",
      "USMemorialDay", "USMLKingsBirthday", "USNewYearsDay", "USPresidentsDay", "USThanksgivingDay", "USCyberMonday", "USVeteransDay",
      "USWashingtonsBirthday", "Lag_USMemorialDay", "Lag_USPresidentsDay", "Lag_USThanksgivingDay", "Lag_USChristmasDay",
      "Lag_USLaborDay", "Amazon_Prime_Day", "L1_competition_Brother", "L1_competition_Canon", "L1_competition_Epson",
      "L1_competition_Lexmark", "L1_competition_Samsung", "L2_competition_Brother", "L2_competition_Canon", "L2_competition_Epson",
      "L2_competition_Lexmark", "L2_competition_Samsung", "L1_competition", "L2_competition", "L1_competition_HP_ssmodel",
      "L2_competition_HP_ssmodel", "L1_competition_ssdata_HPmodel", "L2_competition_ssdata_HPmodel", "BOGO_dummy", "BOGO", "cSKU", "Walmart_Qty","retail_price",
      "avg_price", "min_price", "avg_cost", "total_sales", "vis_SKU_Name", "new_sku_low_vol", "data_useful_vis", "BOL_vis", "EOL_vis", "data_useful_online",
      "BOL_vis_online", "EOL_vis_online", "color_variant", "low_online_qty", "drop_in_ex", "drop_in_flag", "Notes", "EOL_vis_flag", "BOL_vis_flag", "c_quantity",
      "c_avg_price", "c_Street_Price", "avg_quantity", "avg_c_quantity", "c_quantity_f", "drop_in_week", "exclude", "c_discount", "c_discount_perc",
      "L1_cannibalization", "L2_cannibalization", "Sale_Price", "Price_Range_20_Perc_high", "seasonality_npd", "seasonality_npd2",
      "Hardware_GM", "Supplies_GM", "Hardware_Rev", "Supplies_Rev", "Valid_Start_Date", "Valid_End_Date",
      "supplies_GM_scaling_factor", "Fixed_Cost", "aveHWGM", "aveSuppliesGM", "avg_discount_SKU_Account", "Supplies_GM_unscaled", "Supplies_GM_no_promo", "Supplies_Rev_unscaled",
      "Supplies_Rev_no_promo", "SKU_category", /*"L1_cannibalization_log", "L2_cannibalization_log", "L1_competition_log", "L2_competition_log",*/
      "no_promo_avg", "no_promo_med", "no_promo_sd", "no_promo_min", "no_promo_max", "low_baseline", "low_volume",
      "raw_bl_avg", "raw_bl_med", "low_confidence", "high_disc_Flag", "always_promo_Flag", "Season_most_recent", "Promo_Pct_Ave",
      "Promo_Pct_Min", "L1_cannibalization_OnOffline_Min", "L1_Innercannibalization_OnOffline_Min", "L2_cannibalization_OnOffline_Min",
      "L2_Innercannibalization_OnOffline_Min", "PriceBand", "PriceBand_cannibalization_OnOffline_Min", "PriceBand_Innercannibalization_OnOffline_Min",
      "Cate", "Cate_cannibalization_OnOffline_Min", "Cate_Innercannibalization_OnOffline_Min", /*"BOPISbtbhol", "BOPISbts",*/
      "cann_group", "cann_receiver", "Direct_Cann_201", "Direct_Cann_225", "Direct_Cann_252", "Direct_Cann_277", "Direct_Cann_Weber",
      "Direct_Cann_Muscatel_Weber", "Direct_Cann_Muscatel_Palermo", "Direct_Cann_Palermo", "LBB_adj", "Amount", "NP_Type",
      "Street_PriceWhoChange_log", "SKUWhoChange", "PriceChange_HPS_OPS", "Org_SP", "Pec_Street_Price_Changed_BeforeSqr",
      "Pec_Street_Price_Changed", "AE_NP_IR", "AE_ASP_IR", "AE_Other_IR",/* "instore_labor",*/ "Selling_Price", /*"Amazon-Proper0",*/
      "Price_Amazon_com", "Price_Best_Buy", "Price_Best_Buy_com", "Price_Office_Depot_Max", "Price_Office_Depot_Max_com",
      "Price_Staples", "Price_Staples_com", "Price_Min_Online", "Price_Min_Offline", "Delta_Price_Online", "Delta_Price_Offline",
      "Price_Gap_Online", "Price_Gap_Offline", "Street_Price", "Walmart_Price", "Delta_Price_Walmart", "Price_Gap_Walmart", "weekly_endcap_flag")


    retailWithCompCann3DF.coalesce(1).write.mode(SaveMode.Overwrite).option("header", true).csv("/etherData/Pricing/Outputs/Preregression_Retail/preregression_output_retail_"+currentTS+".csv")

    val regData = retailWithCompCann3DF
      .withColumn("MasterIR", col("NP_IR") + col("ASP_IR"))
      .withColumn("MasterPrice", col("Street_Price") - col("MasterIR"))
      .withColumn("EffectDiscount", col("Street_Price") + col("ImpMin"))
      .withColumn("ExecutionRatio", when(col("MasterIR") === 0, 0).otherwise((col("EffectDiscount") - col("MasterIR")) / col("MasterIR")))


    var discount2 = regData
      .filter(col("USCyberMonday") === 0 && col("USThanksgivingDay") === 0 && col("ASP_IR") === 0)
      .select("Account", "SKU", "POS_Qty", "Street_Price", "MasterPrice", "NP_IR", "ASP_IR", "MasterIR", "ImpMin", "EffectDiscount", "ExecutionRatio")
      .filter(col("MasterIR") =!= 0)
      .groupBy("Account", "SKU")
      // summarise_at(vars(-POS.Qty), funs(weighted.mean(.,POS.Qty, na.rm = T))) %>% -> is simplified to calculate just mean.
      // Note that weighted mean takes data and weight columns but in this case as only one column is specified so its just the MEAN that we need to calculate
      .agg(mean(col("POS_Qty")).as("POS_Qty"))
      .drop("POS_Qty")
        //.where(col("MasterIR")=!=0)   // CR1 - Filter was missing

    discount2 = discount2
      .join(regData.dropDuplicates("SKU").select("SKU", "SKU_Name"), Seq("SKU"), "left")
    //discount2.coalesce(1).write.mode(SaveMode.Overwrite).option("header", true).csv("/etherData/Pricing/Outputs/Preregression_Retail/ExecutivePromo2_" + currentTS + ".csv")

    val excluded = retailWithCompCann3DF
      .filter(col("exclude") === 1 && col("low_volume") === 0 && col("Special_Programs").isin("None", "BOPIS") &&
        col("Season").isin("HOL'18", "BTS'18", "STS'18", "BTB'18"))
      .groupBy("Account", "SKU_Name", "Season", "Online")
      .agg(
        sum(col("POS_Qty")).as("Total_qty"),
        sum(col("low_baseline")).as("Low_base"),
        sum(col("EOL_criterion")).as("EOL_drop"),
        sum(col("BOL_criterion")).as("BOL_drop"),
        sum(col("NP_Flag")).as("NP_IR"),
        sum(col("Promo_Flag")).as("Total_IR")
      )
      .orderBy(col("Account"), col("Total_qty").desc)
    //excluded.coalesce(1).write.mode(SaveMode.Overwrite).option("header", true).csv("/etherData/Pricing/Outputs/Preregression_Retail/Excluded_" + currentTS + ".csv")
  }
}
