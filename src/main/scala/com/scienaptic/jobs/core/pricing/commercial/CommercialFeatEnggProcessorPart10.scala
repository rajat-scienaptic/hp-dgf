package com.scienaptic.jobs.core.pricing.commercial

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.CommercialUtility.createlist
import com.scienaptic.jobs.utility.Utils.renameColumns
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

object CommercialFeatEnggProcessor10 {
  val Cat_switch=1
  val min_baseline = 2
  val stability_weeks = 4
  val intro_weeks = 6

  val dat2000_01_01 = to_date(unix_timestamp(lit("2000-01-01"),"yyyy-MM-dd").cast("timestamp"))
  val dat9999_12_31 = to_date(unix_timestamp(lit("9999-12-31"),"yyyy-MM-dd").cast("timestamp"))
  val TEMP_OUTPUT_DIR = "/etherData/Pricing/Outputs/Preregression_Commercial/temp/preregresion_commercial_output.csv"
  //val TEMP_OUTPUT_DIR = "E:\\Scienaptic\\HP\\Pricing\\R\\SPARK_DEBUG_OUTPUTS\\preregression_commercial_temp.csv"

  def execute(executionContext: ExecutionContext): Unit = {

    val sparkConf = new SparkConf().setAppName("Test")
    val spark = SparkSession.builder
      .master("yarn-client")
      //.master("local[*]")
      .appName("Commercial-R-10")
      .config(sparkConf)
      .getOrCreate

    //val baselineThreshold = if (min_baseline/2 > 0) min_baseline/2 else 0
    val currentTS = spark.read.json("/etherData/state/currentTS.json").select("ts").head().getString(0)

    var commercial = spark.read.option("header","true").option("inferSchema","true").csv("/etherData/commercialTemp/CommercialFeatEngg/commercialBeforeCannGroups.csv")
      .withColumn("ES date", to_date(col("ES date")))
      .withColumn("Week_End_Date", to_date(col("Week_End_Date")))
      .withColumn("Valid_Start_Date", to_date(col("Valid_Start_Date")))
      .withColumn("Valid_End_Date", to_date(col("Valid_End_Date")))
      .withColumn("GA date", to_date(unix_timestamp(col("GA date"),"yyyy-MM-dd").cast("timestamp")))
      .persist(StorageLevel.MEMORY_AND_DISK)
      .cache()
    commercial = commercial
      .withColumn("cann_group", lit("None"))
      .withColumn("cann_group", when(col("SKU_Name").contains("M20") || col("SKU_Name").contains("M40"),"M201_M203/M402").otherwise(col("cann_group")))
      .withColumn("cann_group", when(col("SKU_Name").contains("M22") || col("SKU_Name").contains("M42"),"M225_M227/M426").otherwise(col("cann_group")))
      .withColumn("cann_group", when(col("SKU_Name").contains("M25") || col("SKU_Name").contains("M45"),"M252_M254/M452").otherwise(col("cann_group")))
      .withColumn("cann_group", when(col("SKU_Name").contains("M27") || col("SKU_Name").contains("M28"),"M277_M281/M477").otherwise(col("cann_group")))
      .withColumn("cann_group", when(col("SKU_Name").contains("8710") || col("SKU_Name").contains("8720") || col("SKU_Name").contains("6968") || col("SKU_Name").contains("6978"),"Weber/Muscatel").otherwise(col("cann_group")))
      .withColumn("cann_group", when(col("SKU_Name").contains("6255") || col("SKU_Name").contains("7155") || col("SKU_Name").contains("7855") || col("SKU_Name").contains("6988") || col("SKU_Name").contains("6978"),"Palermo/Muscatel").otherwise(col("cann_group")))
      .withColumn("cann_receiver", lit("None"))
      .withColumn("cann_receiver", when(col("SKU_Name").contains("M40"), "M402").otherwise(col("cann_receiver")))
      .withColumn("cann_receiver", when(col("SKU_Name").contains("M42"), "M426").otherwise(col("cann_receiver")))
      .withColumn("cann_receiver", when(col("SKU_Name").contains("M45"), "M452").otherwise(col("cann_receiver")))
      .withColumn("cann_receiver", when(col("SKU_Name").contains("M47"), "M477").otherwise(col("cann_receiver")))
      .withColumn("cann_receiver", when(col("SKU_Name").contains("8710") || col("SKU_Name").contains("8720"), "Weber").otherwise(col("cann_receiver")))
      .withColumn("cann_receiver", when(col("SKU_Name").contains("6968") || col("SKU_Name").contains("6978"), "Muscatel").otherwise(col("cann_receiver")))
      .withColumn("cann_receiver", when(col("SKU_Name").contains("6255") || col("SKU_Name").contains("7155") || col("SKU_Name").contains("7855"), "Palermo").otherwise(col("cann_receiver")))

    commercial=commercial
      .withColumn("is201",when(col("SKU_Name").contains("M201") or col("SKU_Name").contains("M203"),1).otherwise(0))
      .withColumn("is225",when(col("SKU_Name").contains("M225") or col("SKU_Name").contains("M227"),1).otherwise(0))
      .withColumn("is252",when(col("SKU_Name").contains("M252") or col("SKU_Name").contains("M254"),1).otherwise(0))
      .withColumn("is277",when(col("SKU_Name").contains("M277") or col("SKU_Name").contains("M281"),1).otherwise(0))
      .withColumn("isM40",when(col("SKU_Name").contains("M40"),1).otherwise(0))
      .withColumn("isM42",when(col("SKU_Name").contains("M42"),1).otherwise(0))
      .withColumn("isM45",when(col("SKU_Name").contains("M45"),1).otherwise(0))
      .withColumn("isM47",when(col("SKU_Name").contains("M47"),1).otherwise(0))

    val commWeek1=commercial.where(col("isM40")===1).groupBy("Week_End_Date").agg(mean(col("IR")).alias("Direct_Cann_201"))
      .withColumn("is201",lit(1))
    val commWeek2=commercial.where(col("isM42")===1).groupBy("Week_End_Date").agg(mean(col("IR")).alias("Direct_Cann_225"))
      .withColumn("is225",lit(1))
    val commWeek3=commercial.where(col("isM45")===1).groupBy("Week_End_Date").agg(mean(col("IR")).alias("Direct_Cann_252"))
      .withColumn("is252",lit(1))
    val commWeek4=commercial.where(col("isM47")===1).groupBy("Week_End_Date").agg(mean(col("IR")).alias("Direct_Cann_277"))
      .withColumn("is277",lit(1))
    commercial=commercial
      .withColumn("Week_End_Date",col("Week_End_Date")).withColumn("is201",col("is201"))
      .join(commWeek1.withColumn("is201",col("is201")).withColumn("Week_End_Date",col("Week_End_Date")),
        Seq("is201", "Week_End_Date"), "left")
      .withColumn("Week_End_Date",col("Week_End_Date")).withColumn("is225",col("is225"))
      .join(commWeek2.withColumn("is225",col("is225")).withColumn("Week_End_Date",col("Week_End_Date")),
        Seq("is225", "Week_End_Date"), "left")
      .withColumn("Week_End_Date",col("Week_End_Date")).withColumn("is252",col("is252"))
      .join(commWeek3.withColumn("is252",col("is252")).withColumn("Week_End_Date",col("Week_End_Date")),
        Seq("is252", "Week_End_Date"), "left")
      .withColumn("Week_End_Date",col("Week_End_Date")).withColumn("is277",col("is277"))
      .join(commWeek4.withColumn("is277",col("is277")).withColumn("Week_End_Date",col("Week_End_Date")),
        Seq("is277", "Week_End_Date"), "left")
      .withColumn("Direct_Cann_201", when(col("Direct_Cann_201").isNull,0).otherwise(col("Direct_Cann_201")))
      .withColumn("Direct_Cann_225", when(col("Direct_Cann_225").isNull,0).otherwise(col("Direct_Cann_225")))
      .withColumn("Direct_Cann_252", when(col("Direct_Cann_252").isNull,0).otherwise(col("Direct_Cann_252")))
      .withColumn("Direct_Cann_277", when(col("Direct_Cann_277").isNull,0).otherwise(col("Direct_Cann_277")))

    commercial = commercial.drop("is225","is201","is252","is277","isM40","isM42","isM45","isM47")

    /* CR1 - Direct Cann optimization - Start */
    //Direct_Cann_Weber
    var tempMeanIR = commercial.where(col("cann_group")==="Muscatel").select("cann_group","Week_End_Date","IR")
      .groupBy("Week_End_Date").agg(mean("IR").as("Direct_Cann_Weber"))
    commercial = commercial.join(tempMeanIR, Seq("Week_End_Date"), "left")
      .withColumn("Direct_Cann_Weber", when(col("cann_group")==="Weber", col("Direct_Cann_Weber")).otherwise(0))

    //Direct_Cann_Muscatel_Weber
    tempMeanIR = commercial.where(col("cann_group")==="Weber").select("cann_group","Week_End_Date","IR")
      .groupBy("Week_End_Date").agg(mean("IR").as("Direct_Cann_Muscatel_Weber"))
    commercial = commercial.join(tempMeanIR, Seq("Week_End_Date"), "left")
      .withColumn("Direct_Cann_Muscatel_Weber", when(col("cann_group")==="Muscatel", col("Direct_Cann_Muscatel_Weber")).otherwise(0))

    //Direct_Cann_Muscatel_Palermo
    tempMeanIR = commercial.where(col("cann_group")==="Palermo").select("cann_group","Week_End_Date","IR")
      .groupBy("Week_End_Date").agg(mean("IR").as("Direct_Cann_Muscatel_Palermo"))
    commercial = commercial.join(tempMeanIR, Seq("Week_End_Date"), "left")
      .withColumn("Direct_Cann_Muscatel_Palermo", when(col("cann_group")==="Muscatel", col("Direct_Cann_Muscatel_Palermo")).otherwise(0))

    //Direct_Cann_Palermo
    tempMeanIR = commercial.where(col("cann_group")==="Muscatel").select("cann_group","Week_End_Date","IR")
      .groupBy("Week_End_Date").agg(mean("IR").as("Direct_Cann_Palermo"))
    commercial = commercial.join(tempMeanIR, Seq("Week_End_Date"), "left")
      .withColumn("Direct_Cann_Palermo", when(col("cann_group")==="Palermo", col("Direct_Cann_Palermo")).otherwise(0))

    commercial = commercial
        .na.fill(0, Seq("Direct_Cann_201","Direct_Cann_225","Direct_Cann_252","Direct_Cann_277","Direct_Cann_Weber","Direct_Cann_Muscatel_Weber","Direct_Cann_Muscatel_Palermo","Direct_Cann_Palermo"))
    /* CR1 - Direct Cann optimization - End */

    /* Code Change: Avik April 6: VApr6: No more required to check these */
      /*
      .withColumn("Hardware_GM", when(col("Category_1")==="Value" && col("Week_End_Date")>="2016-07-01", col("Hardware_GM")+lit(68)).otherwise(col("Hardware_GM")))
      .withColumn("Hardware_GM", when(col("Category_1")==="Value" && col("Week_End_Date")>="2017-05-01", col("Hardware_GM")+lit(8)).otherwise(col("Hardware_GM")))
      .withColumn("Hardware_GM", when(col("Category Custom")==="A4 SMB" && col("Week_End_Date")>="2017-11-01", col("Hardware_GM")-lit(7.51)).otherwise(col("Hardware_GM")))
      .withColumn("Hardware_GM", when(col("Category Custom").isin("A4 Value","A3 Value") && col("Week_End_Date")>="2017-11-01", col("Hardware_GM")+lit(33.28)).otherwise(col("Hardware_GM")))
      */

    /* Code Change: Avik April 6: VApr6: Use new source, Canon funding code (Aux table file's worksheet: canon_fund) - Start */
    var canon = renameColumns(spark.read.option("header","true").option("inferSchema","true").csv("/etherData/managedSources/AUX/Aux_canon.csv"))
    canon = canon
      //TODO: Check format in production
      .withColumn("Start_Date", to_date(col("Start Date")))
      .withColumn("End_Date", to_date(col("End Date")))
      /*.withColumn("Start_date", to_date(unix_timestamp(col("Start Date"),"dd-MM-yyyy").cast("timestamp")))
      .withColumn("End_date", to_date(unix_timestamp(col("End Date"),"dd-MM-yyyy").cast("timestamp")))*/
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
    commercial = commercial.withColumnRenamed("Category Custom","Category_Custom").join(canon, Seq("Category_Custom","Week_End_Date"), "left")
      .withColumn("Amount",when(col("Amount").isNull, 0).otherwise(col("Amount")))
      .withColumn("Hardware_GM", col("Hardware_GM")+col("Amount"))
        .withColumnRenamed("Category_Custom","Category Custom")
    /* Code change END: Avik April 6 : VApr6: Use new source, Canon funding code (Aux table file's worksheet: canon_fund) - End */

    commercial = commercial
      .withColumn("Supplies_GM", when(col("L1_Category")==="Scanners",0).otherwise(col("Supplies_GM")))

    //TODO: Remove this when 2017-07-10 was modified to 2017-10-07 in part6 as part of calculation of Spike variable.
    val Oct72017Date = to_date(unix_timestamp(lit("2017-10-07"),"yyyy-MM-dd").cast("timestamp"))
    commercial = commercial
      .withColumn("spike",
        when(col("SKU_Name")==="LJP M426fdn" && col("Reseller_Cluster")==="Other - Option B" && col("Week_End_Date")===Oct72017Date && col("SKU")==="F6W14A",1)
          .otherwise(col("spike")))
      .withColumn("spike2",
        when(col("SKU_Name")==="LJP M426fdn" && col("Reseller_Cluster")==="Other - Option B" && col("Week_End_Date")===Oct72017Date && col("SKU")==="F6W14A",1)
          .otherwise(col("spike2")))

    commercial = commercial
      .withColumn("exclude",
          when(!col("PL").isin("3Y"),
            when(col("Reseller_Cluster").isin("Other - Option C", "eTailerOther - Option C", "Stockpiler & Deal Chaser", "eTailerStockpiler & Deal Chaser") ||
                col("low_volume")===1 || col("low_baseline")===1 || col("spike")===1 || col("opposite")===1 || col("EOL")===1 ||
                col("BOL")===1 || col("no_promo_sales")===1,1)
            .otherwise(0))
          .otherwise(0))
      .withColumn("exclude",
          when(col("PL").isin("3Y"),
            when(col("Reseller_Cluster").isin("Other - Option C", "eTailerOther - Option C", "Stockpiler & Deal Chaser", "eTailerStockpiler & Deal Chaser") ||
              col("low_volume")===1 || /*col("low_baseline")===1 ||*/ col("spike")===1 || col("opposite")===1 || col("EOL")===1 ||
              col("BOL")===1 || col("no_promo_sales")===1,1)
            .otherwise(0))
          .otherwise(col("exclude")))
      .withColumn("exclude", when(col("SKU_Name").contains("Sprocket"), 1)
          .otherwise(col("exclude")))
      .withColumn("AE_NP_IR", col("NP_IR"))
      .withColumn("AE_ASP_IR", lit(0))
      .withColumn("AE_Other_IR", lit(0))
      .withColumn("Street_PriceWhoChange_log", when(col("Changed_Street_Price")===0, 0).otherwise(log(col("Street Price")*col("Changed_Street_Price"))))
      .withColumn("SKUWhoChange", when(col("Changed_Street_Price")===0, 0).otherwise(col("SKU")))
      .withColumn("PriceChange_HPS_OPS", when(col("Changed_Street_Price")===0, 0).otherwise(col("HPS_OPS")))

    val maxWED = commercial.agg(max("Week_End_Date")).head().getDate(0)
    val maxWEDSeason = commercial.where(col("Week_End_Date")===maxWED).sort(col("Week_End_Date").desc).select("Season").head().getString(0)
    val latestSeasonCommercial = commercial.where(col("Season")===maxWEDSeason)

    val windForSeason = Window.orderBy(col("Week_End_Date").desc)
    val uniqueSeason = commercial.withColumn("rank", row_number().over(windForSeason))
      .where(col("rank")===2).select("Season").head().getString(0)

    val latestSeason = latestSeasonCommercial.select("Week_End_Date").distinct().count()
    if (latestSeason<13) {
      commercial = commercial.withColumn("Season_most_recent", when(col("Season")===maxWEDSeason,uniqueSeason).otherwise(col("Season")))
    } else {
      commercial = commercial.withColumn("Season_most_recent", col("Season"))
    }

    commercial = commercial
      .withColumnRenamed("Street Price","Street_Price")
      .withColumnRenamed("Category Subgroup","Category_Subgroup")
      .withColumnRenamed("Category Custom","Category_Custom")
      .withColumnRenamed("PLC Status","PLC_Status")
      .withColumnRenamed("Consol SKU","Consol_SKU")
      .withColumnRenamed("Full Name","Full_Name")
      .withColumn("PLC_Status", when(col("PLC_Status") === "#N/A", null).otherwise(col("PLC_Status")))

    commercial.write.option("header","true").mode(SaveMode.Overwrite).csv(TEMP_OUTPUT_DIR)

    var commercialFinalOut = spark.read.option("header","true").option("inferSchema","true").csv(TEMP_OUTPUT_DIR)
      .withColumn("ES date", to_date(col("ES date")))
      .withColumn("Week_End_Date", to_date(col("Week_End_Date")))
      .withColumn("Valid_Start_Date", to_date(col("Valid_Start_Date")))
      .withColumn("Valid_End_Date", to_date(col("Valid_End_Date")))
      .withColumn("GA date", to_date(unix_timestamp(col("GA date"),"yyyy-MM-dd").cast("timestamp")))

    //Change: Changing Week End Date format
    commercialFinalOut = commercialFinalOut.withColumn("Week_End_Date", date_format(to_date(col("Week_End_Date"), "yyyy-MM-dd"),"MM/dd/yyyy"))
      .withColumn("Week_End_Date", col("Week_End_Date").cast("string"))

    commercialFinalOut
      .select("SKU_Name","Reseller_Cluster","SKU","Week_End_Date","L1_Category","L2_Category","Season","Street_Price","IPSLES","HPS_OPS","Series","Category","Category_Subgroup","Category_1","Category_2","Category_3",
        "Category_Custom","Line","PL","PLC_Status","GA date","ES date","Inv_Qty","Special_Programs","Qty","IR","Big_Deal_Qty","Non_Big_Deal_Qty","Brand","Consol_SKU","Full_Name","VPA","Promo_Flag","Promo_Pct",
        "Discount_Depth_Category","log_Qty","price","Inv_Qty_log","Fixed_Cost","USChristmasDay","USColumbusDay","USIndependenceDay","USLaborDay","USLincolnsBirthday","USMemorialDay","USMLKingsBirthday",
        "USNewYearsDay","USPresidentsDay","USVeteransDay","USWashingtonsBirthday","USThanksgivingDay","USCyberMonday","L1_competition_Brother","L1_competition_Canon","L1_competition_Epson","L1_competition_Lexmark",
        "L1_competition_Samsung","L2_competition_Brother","L2_competition_Canon","L2_competition_Epson","L2_competition_Lexmark","L2_competition_Samsung","L1_competition","L2_competition","L1_competition_HP_ssmodel",
        "L2_competition_HP_ssmodel","L1_cannibalization","L2_cannibalization","Sale_Price","seasonality_npd","seasonality_npd2","Hardware_GM","Supplies_GM","Hardware_Rev","Supplies_Rev","Changed_Street_Price",
        "Valid_Start_Date","Valid_End_Date","Hardware_GM_type","Hardware_Rev_type","Supplies_GM_type","Supplies_Rev_type","avg_discount_SKU_Account","supplies_GM_scaling_factor","Supplies_GM_unscaled",
        "Supplies_GM_no_promo","Supplies_Rev_unscaled","Supplies_Rev_no_promo","L1_cannibalization_log","L2_cannibalization_log","L1_competition_log","L2_competition_log","Big_Deal","Big_Deal_Qty_log","outlier",
        "spike","spike2","no_promo_avg","no_promo_med","low_baseline","low_volume","raw_bl_avg","raw_bl_med","EOL","BOL","opposite","no_promo_sales","NP_Flag","NP_IR","high_disc_Flag","always_promo_Flag","cann_group",
        "cann_receiver","Direct_Cann_201","Direct_Cann_225","Direct_Cann_252","Direct_Cann_277","Direct_Cann_Weber","Direct_Cann_Muscatel_Weber","Direct_Cann_Muscatel_Palermo","Direct_Cann_Palermo","Amount","exclude",
        "AE_NP_IR","AE_ASP_IR","AE_Other_IR","Street_PriceWhoChange_log","SKUWhoChange","PriceChange_HPS_OPS","Season_most_recent")
      .coalesce(1).write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/Pricing/Outputs/Preregression_Commercial/preregresion_commercial_output_"+currentTS+".csv")
  }

}