package com.scienaptic.jobs.core.pricing.retail

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.CommercialUtility.createlist
import com.scienaptic.jobs.utility.Utils.renameColumns
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object RetailPreRegressionPart21 {

  val TEMP_OUTPUT_DIR = "/home/avik/Scienaptic/HP/data/May31_Run/spark_out_retail/temp/preregression_output_retail.csv"

  def execute(executionContext: ExecutionContext): Unit = {
    val spark: SparkSession = executionContext.spark

    val npd = renameColumns(executionContext.spark.read.option("header", "true").option("inferSchema", "true").csv("/home/avik/Scienaptic/HP/data/May31_Run/inputs/NPD_weekly.csv"))
    val maximumRegressionDate = npd.select("Week_End_Date").withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "MM/dd/yyyy").cast("timestamp")))
      .withColumn("Week_End_Date", date_sub(col("Week_End_Date"), 7)).agg(max("Week_End_Date")).head().getDate(0)

    //var retailWithCompCann3DF = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/home/avik/Scienaptic/HP/data/Retail/April13_inputs_for_spark/Preregression_Inputs/Intermediate/retail-DirectCann-PART20.csv")
    var retailWithCompCann3DF = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/home/avik/Scienaptic/HP/data/May31_Run/spark_out_retail/retail-DirectCann-PART20.csv")
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("EOL_Date", to_date(unix_timestamp(col("EOL_Date"), "yyyy-MM-dd").cast("timestamp"))).cache()

    val tableLBB = retailWithCompCann3DF
      .filter(col("Account") === "Amazon-Proper" && col("NP_IR") === 0)
      .groupBy("Account", "SKU", "Online")
      .agg(mean(col("LBB")).as("LBB_adj"))

    retailWithCompCann3DF = retailWithCompCann3DF
      .join(tableLBB, Seq("Account", "SKU", "Online"), "left")
      .withColumn("LBB", when(col("Account") === "Amazon-Proper", col("LBB")).otherwise(lit(0)))
    /* commented out as per Canon Funding change
    .withColumn("Hardware_GM", when(col("Category_1") === "Value" && col("Week_End_Date") >= "2016-07-01", col("Hardware_GM") + lit(68)).otherwise(col("Hardware_GM")))
    .withColumn("Hardware_GM", when(col("Category_1") === "Value" && col("Week_End_Date") >= "2017-05-01", col("Hardware_GM") + lit(8)).otherwise(col("Hardware_GM")))
    .withColumn("Hardware_GM", when(col("Category_1") === "Value" && col("Week_End_Date") >= "2017-05-01", col("Hardware_GM") + lit(8)).otherwise(col("Hardware_GM"))
    .withColumn("Hardware_GM", when(col("Category Custom").isin("A4 SMB", "A4 Enterprise") && col("Week_End_Date") >= "2016-07-01" && col("Week_End_Date") <= "2017-04-30", col("Hardware_GM") + 68).otherwise(col("Hardware_GM")))
    .withColumn("Hardware_GM", when(col("Category Custom").isin("A4 SMB", "A4 Enterprise") && col("Week_End_Date") >= "2017-05-01" && col("Week_End_Date") <= "2017-10-30", col("Hardware_GM") + 76).otherwise(col("Hardware_GM")))
    .withColumn("Hardware_GM", when(col("Category Custom") === "A4 SMB" && col("Week_End_Date") >= "2017-11-01" && col("Week_End_Date") <= "2018-10-30", col("Hardware_GM") + 68.49).otherwise(col("Hardware_GM")))
    .withColumn("Hardware_GM", when(col("Category Custom") === "A4 Enterprise" && col("Week_End_Date") >= "2017-11-01" && col("Week_End_Date") <= "2018-10-30", col("Hardware_GM") + 101.77).otherwise(col("Hardware_GM")))
    .withColumn("Hardware_GM", when(col("Category Custom") === "A4 SMB" && col("Week_End_Date") >= "2018-11-01" && col("Week_End_Date") <= "2019-10-30", col("Hardware_GM") + 49.35).otherwise(col("Hardware_GM")))
    .withColumn("Hardware_GM", when(col("Category Custom").isin("A4 Enterprise") && col("Week_End_Date") >= "2018-11-01" && col("Week_End_Date") <= "2019-10-30", col("Hardware_GM") + 150).otherwise(col("Hardware_GM")))
    */

    /* canon funding change starts May 23rd 2019*/
    var canon = renameColumns(executionContext.spark.read.option("header","true").option("inferSchema","true").csv("/home/avik/Scienaptic/HP/data/May31_Run/inputs/canon_fund.csv"))
    canon = canon
      .withColumn("Start_date", to_date(unix_timestamp(col("Start Date"),"dd-MM-yyyy").cast("timestamp")))
      .withColumn("End_date", to_date(unix_timestamp(col("End Date"),"dd-MM-yyyy").cast("timestamp")))
      .withColumn("wk_day_start", dayofweek(col("Start_date")))
      .withColumn("wk_day_end", dayofweek(col("End_date")))
      .withColumn("WSD", date_add(expr("date_sub(Start_date, wk_day_start)"),7))
      .withColumn("WED", when(col("wk_day_end")>3,date_add(expr("date_sub(End_date, wk_day_end)"),7)).otherwise(expr("date_sub(End_date,wk_day_end)")))
      .withColumn("WED",to_date(col("WED")))
      .withColumn("Amount", regexp_replace(col("Amount"),lit("\\$"),lit("")).cast("double"))
      .withColumn("week_diff", abs(datediff(col("WSD"), col("WED"))/7)+1)
      .withColumn("repList", createlist(col("week_diff")))
      .withColumn("repeatNum", explode(col("repList")))
      .withColumn("repeatNum", (col("repeatNum")+1)*7)
      .withColumn("WED", expr("date_add(WSD,repeatNum)"))
    canon = canon.select("Category Custom","WED","Amount")
      .withColumnRenamed("WED","Week_End_Date")
      .withColumnRenamed("Category Custom","Category_Custom")
    retailWithCompCann3DF = retailWithCompCann3DF.withColumnRenamed("Category Custom","Category_Custom").join(canon, Seq("Category_Custom","Week_End_Date"), "left")
      .withColumn("Amount",when(col("Amount").isNull, 0).otherwise(col("Amount")))
      .withColumn("Hardware_GM", col("Hardware_GM")+col("Amount"))
      .withColumnRenamed("Category_Custom","Category Custom")
    /* canon funding change ends  */

    /*  CR1 - Added new variable NP.Type - start  */
    retailWithCompCann3DF = retailWithCompCann3DF
        .withColumn("NP_Type", when(col("Flash_IR")>0, "Flash").otherwise(when(col("Bundle_Flag")===1, "Bundle").otherwise("No_IR")))
        .withColumn("NP_Type", when(col("NP_Type").isNull, "No_IR").otherwise(col("NP_Type")))
        .withColumn("Days_on_Promo", when(col("Flash_IR") === 0, col("Days_on_Promo")).otherwise(4))
    /*  CR1 - Added new variable NP.Type - End    */

    /*  CR1 - Adjust Total_IR, Promo_Flag, NP_Flag - Start */
    var retailWM = retailWithCompCann3DF.where(col("Account")==="Walmart")
    retailWM = retailWM.withColumn("Total_IR", col("NP_IR"))
        .withColumn("Promo_Flag", when(col("Total_IR")>0, 1).otherwise(0))
        .withColumn("NP_Flag", col("Promo_Flag"))

    retailWithCompCann3DF = retailWithCompCann3DF.where(col("Account")=!="Walmart")
    /*  CR1 - Adjust Total_IR, Promo_Flag, NP_Flag - Start */

    retailWithCompCann3DF = retailWithCompCann3DF
      .withColumn("exclude", when(col("low_volume") === 0 && col("EOL_criterion") === 0 && col("BOL_criterion") === 0 &&
        col("Week_End_Date") <= maximumRegressionDate && col("low_baseline") === 0 && col("Special_Programs").isin("None", "BOPIS"), lit(0)).otherwise(lit(1)))
      .withColumn("exclude", when((!col("Account").isin("Amazon-Proper", "Staples")) && col("PL").isin("4X"), 1).otherwise(col("exclude")))
      .withColumn("exclude", when((col("Account").isin("Costco", "Sam's Club")) && col("PL").isin("3Y"), 1).otherwise(col("exclude")))
      .withColumn("exclude", when((col("PL").isin("3Y")) && (col("low_baseline") === 1) && (col("POS_Qty") > 0), 0).otherwise(col("exclude")))
      .withColumn("exclude", when((col("SKU_Name").isin("LJP M426fdn", "LJP M477fdw")) && (col("low_baseline") === 1) && (col("POS_Qty") > 0), 0).otherwise(col("exclude")))
      .withColumn("exclude", when(col("Brand").isin("Samsung"), 1).otherwise(col("exclude")))
      .withColumn("exclude", when(col("Account").isin("Sam's Club"), 1).otherwise(col("exclude")))
      .withColumn("exclude", when(col("SKU_Name").contains("Sprocket"), 1).otherwise(col("exclude")))
      .withColumn("exclude", when(col("Account").isin(/*"Walmart", */"HP Shopping", "Rest of Retail"), 1).otherwise(col("exclude"))) //CR1 - Remove Walmart from filter
      .withColumn("exclude", when((col("SKU").isin("V1N07A") && col("Season").isin("HOL'18")), 1).otherwise(col("exclude")))

    /* CR1 - Bind retail with retailWM - Start */
    retailWithCompCann3DF = retailWithCompCann3DF.unionByName(retailWM)
    /* CR1 - Bind retail with retailWM - End */

    retailWithCompCann3DF = retailWithCompCann3DF
      .withColumn("ImpMin_AmazonProper", when(col("Account").isin("Amazon-Proper"), col("ImpMin")).otherwise(col("ImpMin_AmazonProper")))
      .withColumn("Street_PriceWhoChange_log", when(col("Changed_Street_Price") === 0, 0).otherwise(log(col("Street_Price") * col("Changed_Street_Price"))))
      .withColumn("SKUWhoChange", when(col("Changed_Street_Price") === 0, 0).otherwise(col("SKU")))
      .withColumn("PriceChange_HPS_OPS", when(col("Changed_Street_Price") === 0, 0).otherwise(col("HPS/OPS")))

    val streetPriceChanged = retailWithCompCann3DF
      .groupBy("SKU")
      .agg(max("Street_Price").as("Org_SP"))

    retailWithCompCann3DF = retailWithCompCann3DF
      .join(streetPriceChanged, Seq("SKU"), "left")
      .withColumn("Pec_Street_Price_Changed_BeforeSqr", col("Street_Price") / col("Org_SP"))
      .withColumn("Pec_Street_Price_Changed", pow(col("Pec_Street_Price_Changed_BeforeSqr"), lit(2)))

    retailWithCompCann3DF = retailWithCompCann3DF
      .withColumn("ImpMin", when(col("Street_Price") - col("ImpMin") < 0, col("Street_Price")).otherwise(col("ImpMin")))
      .withColumn("ImpMin_AmazonProper", when(col("Street_Price") - col("ImpMin_AmazonProper") < 0, col("Street_Price")).otherwise(col("ImpMin_AmazonProper")))
      .withColumn("ImpMin_BestBuy", when(col("Street_Price") - col("ImpMin_BestBuy") < 0, col("Street_Price")).otherwise(col("ImpMin_BestBuy")))
      .withColumn("ImpMin_HPShopping", when(col("Street_Price") - col("ImpMin_HPShopping") < 0, col("Street_Price")).otherwise(col("ImpMin_HPShopping")))
      .withColumn("ImpMin_OfficeDepotMax", when(col("Street_Price") - col("ImpMin_OfficeDepotMax") < 0, col("Street_Price")).otherwise(col("ImpMin_OfficeDepotMax")))
      .withColumn("ImpMin_Staples", when(col("Street_Price") - col("ImpMin_Staples") < 0, col("Street_Price")).otherwise(col("ImpMin_Staples")))
      .withColumn("ImpAve", when(col("Street_Price") - col("ImpAve") < 0, col("Street_Price")).otherwise(col("ImpAve")))
      .withColumn("ImpAve_AmazonProper", when(col("Street_Price") - col("ImpAve_AmazonProper") < 0, col("Street_Price")).otherwise(col("ImpAve_AmazonProper")))
      .withColumn("ImpAve_BestBuy", when(col("Street_Price") - col("ImpAve_BestBuy") < 0, col("Street_Price")).otherwise(col("ImpAve_BestBuy")))
      .withColumn("ImpAve_HPShopping", when(col("Street_Price") - col("ImpAve_HPShopping") < 0, col("Street_Price")).otherwise(col("ImpAve_HPShopping")))
      .withColumn("ImpAve_OfficeDepotMax", when(col("Street_Price") - col("ImpAve_OfficeDepotMax") < 0, col("Street_Price")).otherwise(col("ImpAve_OfficeDepotMax")))
      .withColumn("ImpAve_Staples", when(col("Street_Price") - col("ImpAve_Staples") < 0, col("Street_Price")).otherwise(col("ImpAve_Staples")))
      .withColumn("AE_NP_IR", col("NP_IR"))
      .withColumn("AE_ASP_IR", col("ASP_IR"))
      .withColumn("AE_Other_IR", col("Other_IR"))
      .withColumn("ASP_IR", when(col("Account").isin("Amazon-Proper"), col("ASP_IR") + col("Other_IR")).otherwise(col("ASP_IR")))
      .withColumn("Other_IR", when(col("Account").isin("Amazon-Proper"), 0).otherwise(col("Other_IR")))
      .withColumn("ASP_Flag", when(col("ASP_IR") > 0, 1).otherwise(lit(0)))
      .withColumn("Other_IR_Flag", when(col("Other_IR") > 0, 1).otherwise(lit(0)))

    retailWithCompCann3DF.coalesce(1).write.mode(SaveMode.Overwrite).option("header", true).csv("/home/avik/Scienaptic/HP/data/May31_Run/spark_out_retail/preregression__before_output_retail.csv")

    /*  CR1 - Code removed in R Code - Start  */
    /*var inStore = renameColumns(executionContext.spark.read.option("header", "true").option("inferSchema", "true").csv("/etherData/managedSources/Instore/instore_labor_final.csv"))
    inStore.columns.toList.foreach(x => {
      inStore = inStore.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    inStore = inStore.cache()
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "MM/dd/yyyy").cast("timestamp"))) // check date format

    var extraPol = renameColumns(executionContext.spark.read.option("header", "true").option("inferSchema", "true").csv("/etherData/managedSources/Instore/instore_labor_proxy.csv"))
    extraPol.columns.toList.foreach(x => {
      extraPol = extraPol.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    extraPol = extraPol.cache()

    retailWithCompCann3DF = retailWithCompCann3DF
      .join(inStore, Seq("Account", "Week_End_Date"), "left")
      .withColumn("mnth", month(col("Week_End_Date")))

    retailWithCompCann3DF = retailWithCompCann3DF
      .join(extraPol, Seq("Account", "mnth"), "left")
    retailWithCompCann3DF = retailWithCompCann3DF
      /* commented the weekend date check as R code does't reflect the same change
      .withColumn("instore_labor", when(col("Week_End_date") <= to_date(unix_timestamp(lit("2015-12-05"), "yyyy-MM-dd").cast("timestamp")), col("proxy_labor")).otherwise(col("instore_labor")))
      .withColumn("instore_labor", when(col("proxy_labor").isNull || col("proxy_labor") === "", null).otherwise(col("instore_labor")))*/
      .withColumn("instore_labor", when(col("Account").isin("Best Buy", "Office Depot-Max", "Staples"), col("instore_labor")).otherwise(lit(0)))
      .withColumn("instore_labor", when(col("Online") === 1, 0).otherwise(col("instore_labor")))
      .withColumn("instore_labor", when(col("instore_labor").isNull || col("instore_labor") === "", 0).otherwise(col("instore_labor")))
      .drop("proxy_labor")
      .withColumn("GC_SKU_Name", when(col("GC_SKU_Name").isNull, "NA").otherwise(col("GC_SKU_Name")))*/
    /*  CR1 - Code removed in R Code - End  */

    /*retailWithCompCann3DF = retailWithCompCann3DF
      //AVIK Change: When total ir is null, it gives Selling price as null too instead of Street Price
      .withColumn("Total_IR", when(col("Total_IR").isNull, 0).otherwise(col("Total_IR")))
      .withColumn("Selling_Price", col("Street_Price") - col("Total_IR"))


    var retail_acc = retailWithCompCann3DF
      .filter(col("Special_Programs").isin("None", "BOPIS") && col("Brand") === "HP")
      .filter(col("Account").isin("Amazon-Proper", "Best Buy", "Office Depot-Max", "Staples"))  // CR1 - Account negation filter removed.
      //.filter(!col("Account").isin("Rest of Retail", "Costco", "Sam's Club", "Walmart", "HP Shopping")) // CR1 - Negation removed.
      .select("Account", "Online", "SKU", "Street_Price", "Week_End_Date", "Selling_Price")
      .withColumn("Account", concat(col("Account"), col("Online")))
      .drop("Online")

    val spreadAccount = retail_acc
      .groupBy("SKU", "Street_Price", "Week_End_Date")
      .pivot("Account")
      .agg(first(col("Selling_Price")).as("Selling_Price"))
      //AVIK Change: Best Buy1 and Amazon-Proper0 were not present
      .na.fill(0, Seq("Staples0", "Staples1", "Best Buy0", "Best Buy1", "Amazon-Proper0", "Amazon-Proper1", "Office Depot-Max0", "Office Depot-Max1"))
    retail_acc = spreadAccount

    retail_acc = retail_acc
      .withColumnRenamed("Staples0", "Price_Staples")
      .withColumnRenamed("Staples1", "Price_Staples_com")
      .withColumnRenamed("Best Buy0", "Price_Best_Buy")
      .withColumnRenamed("Best Buy1", "Price_Best_Buy_com")
      //AVIK Change: Rename of Amazon-Proper0 not present
      //.withColumnRenamed("Amazon-Proper0", "Price_Amazon")
      .withColumnRenamed("Amazon-Proper1", "Price_Amazon_com")
      .withColumnRenamed("Office Depot-Max0", "Price_Office_Depot_Max")
      .withColumnRenamed("Office Depot-Max1", "Price_Office_Depot_Max_com")
      .withColumn("Price_Amazon_com", when(col("Price_Amazon_com") === 0, col("Street_Price")).otherwise(col("Price_Amazon_com")))
      .withColumn("Price_Staples", when(col("Price_Staples") === 0, col("Street_Price")).otherwise(col("Price_Staples")))
      .withColumn("Price_Staples_com", when(col("Price_Staples_com") === 0, col("Street_Price")).otherwise(col("Price_Staples_com")))
      .withColumn("Price_Best_Buy", when(col("Price_Best_Buy") === 0, col("Street_Price")).otherwise(col("Price_Best_Buy")))
      .withColumn("Price_Best_Buy_com", when(col("Price_Best_Buy_com") === 0, col("Street_Price")).otherwise(col("Price_Best_Buy_com")))
      .withColumn("Price_Office_Depot_Max", when(col("Price_Office_Depot_Max") === 0, col("Street_Price")).otherwise(col("Price_Office_Depot_Max")))
      .withColumn("Price_Office_Depot_Max_com", when(col("Price_Office_Depot_Max_com") === 0, col("Street_Price")).otherwise(col("Price_Office_Depot_Max_com")))


    retailWithCompCann3DF = retailWithCompCann3DF.withColumn("Week_End_Date", col("Week_End_Date"))
      .join(retail_acc.withColumn("Week_End_Date", col("Week_End_Date")).drop("Street_Price"), Seq("SKU", "Week_End_Date"), "left")
      .withColumn("Price_Min_Online", when(
        col("Account") === "Amazon-Proper", least(col("Price_Staples_com"), col("Price_Best_Buy_com"), col("Price_Office_Depot_Max_com")))
        .otherwise(
          when(col("Account") === "Best Buy" && col("Online") === 1, least(col("Price_Amazon_com"), col("Price_Staples_com"), col("Price_Office_Depot_Max_com")))
            .otherwise(when(col("Account") === "Office Depot-Max" && col("Online") === 1, least(col("Price_Amazon_com"), col("Price_Best_Buy_com"), col("Price_Staples_com")))
              //AVIK Change: Online filter missing
              .otherwise(when(col("Account") === "Staples" && col("Online") === 1, least(col("Price_Amazon_com"), col("Price_Best_Buy_com"), col("Price_Office_Depot_Max_com")))
              .otherwise(least(col("Price_Best_Buy_com"), col("Price_Office_Depot_Max_com"), col("Price_Amazon_com"), col("Price_Staples_com")))))
        ))
      .withColumn("Price_Min_Offline", when(
        col("Account") === "Best Buy" && col("Online") === 0, least(col("Price_Staples"), col("Price_Office_Depot_Max")))
        .otherwise(
          when(col("Account") === "Office Depot-Max" && col("Online") === 0, least(col("Price_Best_Buy"), col("Price_Staples")))
            //AVIK Change: Online filter missing
            .otherwise(when(col("Account") === "Staples" && col("Online") === 0, least(col("Price_Best_Buy"), col("Price_Office_Depot_Max")))
            .otherwise(least(col("Price_Best_Buy"), col("Price_Office_Depot_Max"), col("Price_Staples"))))
        ))
      .withColumn("Delta_Price_Online", log("Selling_Price") - log("Price_Min_Online"))
      .withColumn("Delta_Price_Offline", log("Selling_Price") - log("Price_Min_Offline"))
      .withColumn("Price_Gap_Online", when((col("Selling_Price") - col("Price_Min_Online")) > 0, col("Selling_Price") - col("Price_Min_Online")).otherwise(lit(0)))
      .withColumn("Price_Gap_Offline", when((col("Selling_Price") - col("Price_Min_Offline")) > 0, col("Selling_Price") - col("Price_Min_Offline")).otherwise(lit(0)))
      .withColumn("Price_Gap_Online", when(col("Price_Gap_Online").isNull, 0).otherwise(col("Price_Gap_Online")))
      .withColumn("Price_Gap_Offline", when(col("Price_Gap_Offline").isNull, 0).otherwise(col("Price_Gap_Offline")))

    // walmart
    val skuWalmart = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/home/avik/Scienaptic/HP/data/May31_Run/inputs/walmart_link.csv")
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
    var endcap = renameColumns(spark.read.option("header", true).option("inferSchema", true).csv("/home/avik/Scienaptic/HP/data/May31_Run/inputs/endcap_weekly_prereg_2019-03-06_074509.csv"))
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


    retailWithCompCann3DF.coalesce(1).write.mode(SaveMode.Overwrite).option("header", true).csv("/home/avik/Scienaptic/HP/data/May31_Run/spark_out_retail/preregression_output_retail.csv")

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
        .where(col("MasterIR")=!=0)   // CR1 - Filter was missing

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
    //excluded.coalesce(1).write.mode(SaveMode.Overwrite).option("header", true).csv("/etherData/Pricing/Outputs/Preregression_Retail/Excluded_" + currentTS + ".csv")*/
  }
}
