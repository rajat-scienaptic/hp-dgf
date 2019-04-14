package com.scienaptic.jobs.core

import java.text.SimpleDateFormat

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.bean.UnionOperation.doUnion
import com.scienaptic.jobs.bean.SelectOperation.doSelect
import com.scienaptic.jobs.bean.GroupOperation.doGroup
import com.scienaptic.jobs.bean.JoinAndSelectOperation.doJoinAndSelect
import com.scienaptic.jobs.bean.FilterOperation.doFilter
import com.scienaptic.jobs.bean.SortOperation.doSort
import com.scienaptic.jobs.utility.Utils
import com.scienaptic.jobs.utility.CommercialUtility._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{SaveMode, SparkSession}

object CommercialSimplifiedTransform {

  val IEC_SOURCE="IEC_CLAIMS"
  val XS_CLAIMS_SOURCE="XS_CLAIMS"
  val RAW_CALENDAR_SOURCE="RAW_MASTER_CALENDAR"
  val WED_SOURCE="WEEK_END_DATE"
  val SKU_HIERARCHY_SOURCE="SKU_HIERARCHY"
  val COMM_ACCOUNTS_SOURCE="COMM_ACCOUNTS"
  val ST_ONYX_SOURCE="ONYX_ST"
  val STT_ONYX_SOURCE="ONYX_STT"
  val TIDY_HISTORICAL_POS_SOURCE="TIDY_HPS_2016_17_HISTORICAL_POS"

  val LEFT="leftanti"
  val INNER="inner"
  val RIGHT="rightanti"
  val NUMERAL0=0
  val NUMERAL1=1

  val INITIAL_SELECT="INITIAL_SELECT"
  val RENAME_INITIAL_SELECT="RENAME_INITIAL_SELECT"
  val DISTINCT_SELECT="DISTINCT_SELECT"
  val CLAIMS_AND_ACCOUNTS="CLAIMS_AND_ACCOUNTS"
  val GROUP_SKU_ACCOUNT_SUM_CLAIM_QUANT="GROUP_SKU_ACCOUNT_SUM_CLAIM_QUANT"
  val CLAIMS_AND_MASTER_CALENDAR="CLAIMS_AND_MASTER_CALENDAR"
  val CLAIMS_AND_WED="CLAIMS_AND_WED"
  val CLAIMS_AND_SKU_HIER="CLAIMS_AND_SKU_HIER"
  val RESELLER_WED_SKU_PROGRAM_AGG_CLAIM_REBATE_QUANTITY="RESELLER_WED_SKU_PROGRAM_AGG_CLAIM_REBATE_QUANTITY"
  val FILTER_PROGRAM_EQ_BID_DEAL="FILTER_PROGRAM_EQ_BID_DEAL"
  val FILTER_PROGRAM_EQ_PROMO_OPTION_C="FILTER_PROGRAM_EQ_PROMO_OPTION_C"
  val RESELLER_WED_SKU_PROGRAM_AGG_CLAIM_QUANTITY="RESELLER_WED_SKU_PROGRAM_AGG_CLAIM_QUANTITY"
  val FILTER_OUTSIDE_PROMO_DATE_EQ_N="FILTER_OUTSIDE_PROMO_DATE_EQ_N"
  val FILTER_OUTSIDE_PROMO_DATE_EQ_Y="FILTER_OUTSIDE_PROMO_DATE_EQ_Y"
  val WED_SKU_C2B_PROMO_CODE_SUM_CLAIM_QUANTITY="WED_SKU_C2B_PROMO_CODE_SUM_CLAIM_QUANTITY"
  val SKU_PROMO_CODE_AVG_CLAIM_QUANTITY="SKU_PROMO_CODE_AVG_CLAIM_QUANTITY"
  val WED_SKU_CLAIM_REBATE_PROMO_CODE_SUM_CLAIM_QUANTITY="WED_SKU_CLAIM_REBATE_PROMO_CODE_SUM_CLAIM_QUANTITY"
  val CLAIMS_OUTSIDE_PROMO_Y_AND_N="CLAIMS_OUTSIDE_PROMO_Y_AND_N"
  val FILTER_INCLUDE_EQ_Y="FILTER_INCLUDE_EQ_Y"
  val PROMO_CODE_SKU_MAX_WED="PROMO_CODE_SKU_MAX_WED"
  val CLAIMS_AND_MASTER_CALENDAR_AFTER_AGGREGATION="CLAIMS_AND_MASTER_CALENDAR_AFTER_AGGREGATION"
  val ASC_SKU_WED_DESC_REBATE="ASC_SKU_WED_DESC_REBATE"
  val OPTION_TYPE_NOT_EQUALS_OPTION_C="OPTION_TYPE_NOT_EQUALS_OPTION_C"
  val SKU_WED_PROMO_CODE_MAX_REBATE_AMOUNT="SKU_WED_PROMO_CODE_MAX_REBATE_AMOUNT"
  val SELECT_FOR_DISTINCT="SELECT_FOR_DISTINCT"
  val FILTER_CHANNEL_EQ_COMMERCIAL="FILTER_CHANNEL_EQ_COMMERCIAL"
  val SKU_SHORT_RESELLER_WED_SEASON_CAL_FISCAL_SUM_QUANT="SKU_SHORT_RESELLER_WED_SEASON_CAL_FISCAL_SUM_QUANT"
  val RENAME_SEASON_CAL_FISCAL_AFTER_GROUP="RENAME_SEASON_CAL_FISCAL_AFTER_GROUP"
  val TIDY_HIST_AND_SKU_HIER="TIDY_HIST_AND_SKU_HIER"
  val ST_ONYX_AND_AUX_ACCOUNTS="ST_ONYX_AND_AUX_ACCOUNTS"
  val ST_ONYX_AND_SKU_HIERARCHY="ST_ONYX_AND_SKU_HIERARCHY"
  val PRODUCT_BASE_SUM_AGG_SELL_THRU_SELL_TO_QTY="PRODUCT_BASE_SUM_AGG_SELL_THRU_SELL_TO_QTY"
  val DESC_SELL_THRU_SELL_TO="DESC_SELL_THRU_SELL_TO"
  val ST_ONYX_AND_WED="ST_ONYX_AND_WED"
  val STT_ONYX_AND_AUX_ACCOUNTS="STT_ONYX_AND_AUX_ACCOUNTS"
  val STT_ONYX_AND_SKU_HIERARCHY="STT_ONYX_AND_SKU_HIERARCHY"
  val STT_ONYX_AND_WED_MERGE_WED="STT_ONYX_AND_WED_MERGE_WED"
  val RESELLER_SKU_WED_SUM_AGG_INVENTORY_TOTAL="RESELLER_SKU_WED_SUM_AGG_INVENTORY_TOTAL"
  val DEAL_ETAILERS_ACCOUNT_SEASON_CAL_SKU_FISCAL_SUM_AGG_SELL_THRU_TO_QTY="DEAL_ETAILERS_ACCOUNT_SEASON_CAL_SKU_FISCAL_SUM_AGG_SELL_THRU_TO_QTY"
  val ST_ONYX_AND_PROMO_ID="ST_ONYX_AND_PROMO_ID"
  val GROUP_ALL_SUM_AGG_BIG_NON_BIG_DEAL_QTY="GROUP_ALL_SUM_AGG_BIG_NON_BIG_DEAL_QTY"
  val GROUP_ALL_EXCEPT_ACCOUNT_GROUPING_PROMO_SUM_AGG_QTY_AND_DEAL_QTS="GROUP_ALL_EXCEPT_ACCOUNT_GROUPING_PROMO_SUM_AGG_QTY_AND_DEAL_QTS"
  val FILTER_ETAILERS_EQ_Y="FILTER_ETAILERS_EQ_Y"
  val FILTER_ETAILERS_NOT_EQ_Y="FILTER_ETAILERS_NOT_EQ_Y"
  val ST_ONYX_AND_CLAIMS_AGGREGATED="ST_ONYX_AND_CLAIMS_AGGREGATED"
  val ST_ONYX_OPTION_C_JOIN="ST_ONYX_OPTION_C_JOIN"
  val FILTER_OPTION_C_QTY_ADJ_GR_0="FILTER_OPTION_C_QTY_ADJ_GR_0"
  val ST_ONYX_PROMO_JOIN="ST_ONYX_PROMO_JOIN"
  val ST_ONYX_MERGE_INVENTORY="ST_ONYX_MERGE_INVENTORY"
  val SELECT_BEFORE_OUTPUT="SELECT_BEFORE_OUTPUT"
  val COL_PARTNER_DATE="Partner Ship Calendar Date"

  def execute(executionContext: ExecutionContext): Unit = {
    /* Source Map with all sources' information */

    val sparkConf = new SparkConf().setAppName("Test")
    val spark = SparkSession.builder
      //.master("local[*]")
      .master("yarn-client")
      .appName("Commercial")
      .config(sparkConf)
      .getOrCreate

    val sourceMap = executionContext.configuration.sources

    val currentTS = spark.read.json("/etherData/state/currentTS.json").select("ts").head().getString(0)

    val iecSource = sourceMap(IEC_SOURCE)
    val xsClaimsSource = sourceMap(XS_CLAIMS_SOURCE)
    val rawCalendarSource = sourceMap(RAW_CALENDAR_SOURCE)
    val wedSource = sourceMap(WED_SOURCE)
    val skuHierarchySource = sourceMap(SKU_HIERARCHY_SOURCE)
    val commAccountsSource= sourceMap(COMM_ACCOUNTS_SOURCE)
    val stOnyxSource = sourceMap(ST_ONYX_SOURCE)
    val sttOnyxSource = sourceMap(STT_ONYX_SOURCE)
    val tidyHistSource = sourceMap(TIDY_HISTORICAL_POS_SOURCE)

    val iecDF = Utils.loadCSV(executionContext, iecSource.filePath).get.cache()
    val xsClaimsDF = Utils.loadCSV(executionContext, xsClaimsSource.filePath).get.cache()
    val rawCalendarDF = Utils.loadCSV(executionContext, rawCalendarSource.filePath).get.cache()
    val WEDDF = Utils.loadCSV(executionContext, wedSource.filePath).get.cache()
    val SKUHierDF = Utils.loadCSV(executionContext, skuHierarchySource.filePath).get.cache()
    val commAccountDF = Utils.loadCSV(executionContext, commAccountsSource.filePath).get.cache()
    val tidyHistDF = Utils.loadCSV(executionContext, tidyHistSource.filePath).get.cache()
    val sttONYXDF = Utils.loadCSV(executionContext, sttOnyxSource.filePath).get.cache()
    val stONYXDF = Utils.loadCSV(executionContext, stOnyxSource.filePath).get.cache()

    /*
    * Initial Select operations
    * */
    val iecInitialSelect = iecSource.selectOperation(INITIAL_SELECT)
    val xsInitialSelect = xsClaimsSource.selectOperation(INITIAL_SELECT)
    //val rawCalendarInitalSelect = rawCalendarSource.selectOperation(INITIAL_SELECT)
    val wedInitialSelect = wedSource.selectOperation(INITIAL_SELECT)
    val skuHierarchyInitialSelect = skuHierarchySource.selectOperation(INITIAL_SELECT)
    val commAccountsInitialSelect = commAccountsSource.selectOperation(INITIAL_SELECT)
    val stONYXInitialSelect = stOnyxSource.selectOperation(INITIAL_SELECT)
    val sttONYXInitialSelect = sttOnyxSource.selectOperation(INITIAL_SELECT)
    val tidyHistInitialSelect = tidyHistSource.selectOperation(INITIAL_SELECT)

    val iecSelectDF = doSelect(iecDF, iecInitialSelect.cols,iecInitialSelect.isUnknown).get
      //.withColumn(COL_PARTNER_DATE,to_date(unix_timestamp(col(COL_PARTNER_DATE),"yyyy-MM-dd").cast("timestamp"))).cache()
      //TODO: Check in production which format it is read as
      .withColumn(COL_PARTNER_DATE,to_date(col(COL_PARTNER_DATE))).cache()
    //writeDF(iecSelectDF,"IECSELECTDF")
    val xsClaimsSelectDF = doSelect(xsClaimsDF, xsInitialSelect.cols,xsInitialSelect.isUnknown).get
      //.withColumn(COL_PARTNER_DATE, to_date(unix_timestamp(col(COL_PARTNER_DATE),"dd-MM-yyyy").cast("timestamp")))
      //TODO: Check in production which format it is read as
      .withColumn(COL_PARTNER_DATE,to_date(col(COL_PARTNER_DATE))).cache()
    //writeDF(xsClaimsSelectDF,"xsCLAIMS_SELECT_DF")
    val rawCalendarSelectDF = rawCalendarDF/*doSelect(rawCalendarDF, iecInitialSelect.cols,iecInitialSelect.isUnknown).get*/
      //.withColumn("Start Date",to_date(unix_timestamp(col("Start Date"),"MM-dd-yy").cast("timestamp")))
      //.withColumn("End Date",to_date(unix_timestamp(col("End Date"),"MM-dd-yy").cast("timestamp"))).cache()
      //TODO: For production:- below code will go, as the fields are being read as timestamp directly
      /*   Avik Change: April 14: Dates being read as timestamp instead of string  */
      .withColumn("Start Date",to_date(col("Start Date")))
      .withColumn("End Date",to_date(col("End Date"))).cache()

    //writeDF(rawCalendarSelectDF,"rawCalendarSelectDF")
    val wedSelectDF = doSelect(WEDDF, wedInitialSelect.cols,wedInitialSelect.isUnknown).get
      //.withColumn("wed",to_date(unix_timestamp(col("wed"),"dd-MM-yyyy").cast("timestamp")))
      //TODO: For  production keep just 'to_date'
      /*  Avik Change April 14: wed is being read as timestamp not string. Keep this code in production  */
      .withColumn("wed",to_date(col("wed")))
    //writeDF(wedSelectDF,"wedSelectDF")
    val skuHierarchy = doSelect(SKUHierDF, skuHierarchyInitialSelect.cols,skuHierarchyInitialSelect.isUnknown).get
    //writeDF(skuHierarchy,"skuHierarchy")
    val skuHierarchySelectDF = skuHierarchy
      //.withColumn("GA date",to_date(unix_timestamp(col("GA date"),"dd-MM-yyyy").cast("timestamp")))
      //.withColumn("ES date",to_date(unix_timestamp(col("ES date"),"dd-MM-yyyy").cast("timestamp"))).cache()
      //TODO: For production keep just to_date
      .withColumn("GA date", to_date(col("GA date")))
      .withColumn("ES date", to_date(col("ES date")))
    //writeDF(skuHierarchySelectDF,"skuHierarchySelectDF")
    val commAccountsSelectDF = doSelect(commAccountDF, commAccountsInitialSelect.cols,commAccountsInitialSelect.isUnknown).get.cache()
    val stONYXSelectDF = doSelect(stONYXDF, stONYXInitialSelect.cols,stONYXInitialSelect.isUnknown).get.cache()
    val sttONYXSelectDF = doSelect(sttONYXDF, sttONYXInitialSelect.cols,sttONYXInitialSelect.isUnknown).get.cache()
    val tidyHistSelectDF = doSelect(tidyHistDF, tidyHistInitialSelect.cols,tidyHistInitialSelect.isUnknown).get
      .withColumn("Week_End_Date",to_date(unix_timestamp(col("wed"),"MM/dd/yyyy").cast("timestamp")))
      .drop("wed")
    //writeDF(tidyHistSelectDF,"S-print-WED-CONVERTED")

    /*
    * Rename Columns after initial Select Operation
    * */
    val xsClaimsInitialRename = xsClaimsSource.renameOperation(RENAME_INITIAL_SELECT)
    val wedInitialRename = wedSource.renameOperation(RENAME_INITIAL_SELECT)
    val tidyHistInitialRename = tidyHistSource.renameOperation(RENAME_INITIAL_SELECT)

    val xsClaimsRenamedDF = Utils.convertListToDFColumnWithRename(xsClaimsInitialRename, xsClaimsSelectDF).cache()
    val wedRenamedDF = Utils.convertListToDFColumnWithRename(wedInitialRename, wedSelectDF).cache()
    val tidyHistRenamedDF = Utils.convertListToDFColumnWithRename(tidyHistInitialRename, tidyHistSelectDF).cache()

    /*
    * Filter IEC Claims for Partner Ship Calendar Date  */
    val iecFilterShipDateDF = iecSelectDF.filter(to_date(col("Partner Ship Calendar Date")) <= lit("2016-12-31"))


    /*
    * Union IEC and XS Dataset */
    val iecUnionXSDF = doUnion(iecFilterShipDateDF, xsClaimsRenamedDF).get
    //writeDF(iecUnionXSDF,"iec_xs_union_output")
    
    /*
    * Create Features - Base SKU, Temp Date Calc String, Temp Date Calc, Week End Date */
    val iecXSNewFeaturesDF = iecUnionXSDF.withColumn("Base SKU",when(col("Product ID").isNull,null).otherwise(createBaseSKUFromProductIDUDF(col("Product ID"))))
      .withColumn("Temp Date Calc String", dayofweek(col("Partner Ship Calendar Date"))-lit(1))
      //.withColumn("Temp Date Calc String",when(col("Partner Ship Calendar Date").isNull,null).otherwise(extractWeekFromDateUDF(col("Partner Ship Calendar Date").cast("string"), lit("yyyy-MM-dd"))))
      .withColumn("Temp Date Calc", col("Temp Date Calc String").cast(DoubleType))
      .withColumn("Temp Date Calc", when(col("Temp Date Calc").isNull,0).otherwise(col("Temp Date Calc")))
      .withColumn("Temp Date Calc Sub Six", lit(6)-col("Temp Date Calc"))
      .withColumnRenamed("Partner Ship Calendar Date","Partner_Ship_Calendar_Date")
      .withColumnRenamed("Temp Date Calc Sub Six","Temp_Date_Calc_Sub_Six")
      .withColumn("Week End Date", expr("date_add(Partner_Ship_Calendar_Date,Temp_Date_Calc_Sub_Six)"))
      .withColumnRenamed("Partner_Ship_Calendar_Date","Partner Ship Calendar Date")
      .drop("Temp_Date_Calc_Sub_Six")
      .withColumn("Base SKU",baseSKUFormulaUDF(col("Base SKU")))
      .withColumn("Week End Date", to_date(unix_timestamp(col("Week End Date"),"YYYY-MM-dd").cast("timestamp")))
    //writeDF(iecXSNewFeaturesDF,"iecXSNewFeaturesDFCONVERTE_WED")
    /*
    * Join Claims with Accounts based on Partner Desc and Account Company */
    val claimsAndAccountJoin = xsClaimsSource.joinOperation(CLAIMS_AND_ACCOUNTS)
    val claimsAndAccountJoinMap = doJoinAndSelect(iecXSNewFeaturesDF, commAccountsSelectDF, claimsAndAccountJoin)
    val claimsAndAccountLeftJoinDF = claimsAndAccountJoinMap(LEFT)//211642
    val claimsAndAccountInnerJoinDF = claimsAndAccountJoinMap(INNER)//685877 More than required
    //writeDF(claimsAndAccountInnerJoinDF,"claimsAndAccountInnerJoinDF")
    /*
    * Add new variables - Account Consol, Grouping, VPA, Reseller Cluster*/
    val claimsAndAccountLeftJoinNewFeatDF = claimsAndAccountLeftJoinDF
      .withColumn("Account Consol",lit("Others"))
      .withColumn("Grouping",lit("Distributor"))
      .withColumn("VPA", lit("Non-VPA"))
      .withColumn("Reseller Cluster",lit("Other"))

    val claimsAndAccountJoinsUnionDF = doUnion(claimsAndAccountLeftJoinNewFeatDF, claimsAndAccountInnerJoinDF).get//897519
    //writeDF(claimsAndAccountJoinsUnionDF,"Claims_Join_UNION")
    
    /** Group on Account Consol, Grouping, Reseller Cluster, VPA, WED, Deal ID, SKU and sum Claim Quantity*/
    val claimsGroupAndAggClaimQuant = xsClaimsSource.groupOperation(GROUP_SKU_ACCOUNT_SUM_CLAIM_QUANT)
    val claimsGroupAndAggClaimQuantDF = doGroup(claimsAndAccountJoinsUnionDF, claimsGroupAndAggClaimQuant).get//716801 TODO Should be 504158
    //writeDF(claimsGroupAndAggClaimQuantDF,"CLAIMS_Union_Group")

    /*
    * Distinct Master Calendar on Promo Code, Name, SKU, Start and End Date */
    val rawMasterCalendarDistinctSelect = rawCalendarSource.selectOperation(DISTINCT_SELECT)
    val rawMasterCalendarDistinctSelectDF = doSelect(rawCalendarSelectDF, rawMasterCalendarDistinctSelect.cols, rawMasterCalendarDistinctSelect.isUnknown).get
    .select("C2B Promo Code","Promo Name","SKU","Start Date","End Date").distinct()
    //writeDF(rawMasterCalendarDistinctSelectDF,"Aftersummarize")
    /*
    * Join Claims and Master Calendar on Deal ID, SKU*/
    val claimsAndMasterCalendarJoin = xsClaimsSource.joinOperation(CLAIMS_AND_MASTER_CALENDAR)
    val claimsAndMasterCalendarJoinMap = doJoinAndSelect(claimsGroupAndAggClaimQuantDF, rawMasterCalendarDistinctSelectDF, claimsAndMasterCalendarJoin)
    val claimsAndMasterCalendarLeftJoinDF = claimsAndMasterCalendarJoinMap(LEFT)//546224 Should be 327118
    val claimsAndMasterCalendarInnerJoinDF = claimsAndMasterCalendarJoinMap(INNER)//170576 TODO should be 177040
    //writeDF(claimsAndMasterCalendarInnerJoinDF,"CLAIMS_Join_Calendar")
    //TODO: mistmatch between Start and End Date, but joining criteria matches.
    /*
    * Add new features - Program, Total Amount*/
    val claimsAndCalendarLeftJoinNewFeat = claimsAndMasterCalendarLeftJoinDF
    .withColumn("Program", lit("Big.Deal"))
    .withColumn("Total Amount", col("Claim Partner Unit Rebate")*col("Sum_Claim Quantity"))

    val claimsAndCalendarInnerJoinNewFeat = claimsAndMasterCalendarInnerJoinDF
    .withColumn("Program", when(col("Promo Name")==="Option C","Promo.Option.C").otherwise("Promo"))
    .withColumn("Total Amount",col("Claim Partner Unit Rebate")*col("Sum_Claim Quantity"))

    val claimsAndCalendarJoinsUnionDF = doUnion(claimsAndCalendarLeftJoinNewFeat, claimsAndCalendarInnerJoinNewFeat).get//499133
    //writeDF(claimsAndCalendarJoinsUnionDF,"Claims_and_Calendar_Join_UNION")//716800 //Perfect match for total amount and sum_claim quantity
    /*
    * Join Claim and Week end Date on Week End Date*/
    val claimsAndCalendarJoin = xsClaimsSource.joinOperation(CLAIMS_AND_WED)
    val claimsAndCalendarJoinsUnionWithWEDTimeStampdDF = claimsAndCalendarJoinsUnionDF.withColumn("Week End Date", to_date(unix_timestamp(col("Week End Date"),"yyyy-MM-dd").cast("timestamp")))
    //claimsAndCalendarJoinsUnionWithWEDTimeStampdDF has non null Base SKU but on joining with wedRenameDF only rows with null Base SKU comes up
    val claimsAndCalendarJoinMap = doJoinAndSelect(claimsAndCalendarJoinsUnionWithWEDTimeStampdDF, wedRenamedDF, claimsAndCalendarJoin)
    val claimsAndCalendarInnerJoinDF = claimsAndCalendarJoinMap(INNER)//TODO Large difference
    //writeDF(claimsAndCalendarInnerJoinDF,"joinWithClaims")
    /*
    * Filter, Summarize, Formula, Select not implemented as going to BROWSE*/

    /*
    * Join Claims and SKU Hierarchy on SclaimsAndSKUHierInnerJoinDFKU - 254 */
    val claimsAndSKUHierJoin = xsClaimsSource.joinOperation(CLAIMS_AND_SKU_HIER)
    val claimsAndSKUHierJoinMap = doJoinAndSelect(claimsAndCalendarInnerJoinDF, skuHierarchySelectDF, claimsAndSKUHierJoin)
    val claimsAndSKUHierLeftJoinDF = claimsAndSKUHierJoinMap(LEFT)
    val claimsAndSKUHierInnerJoinDF = claimsAndSKUHierJoinMap(INNER)

    val claimsAndSKUJoinsUnionDF = doUnion(claimsAndSKUHierLeftJoinDF, claimsAndSKUHierInnerJoinDF).get
    //writeDF(claimsAndSKUJoinsUnionDF,"claims_consolidated") //7.16 lacs
    /*
    * OUTPUT - claims_consolidated.csv
    * */
    claimsAndSKUJoinsUnionDF.write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/Pricing/Outputs/POS_Commercial/claims_consolidated_"+currentTS+".csv")
    //claimsAndSKUJoinsUnionDF.write.option("header","true").mode(SaveMode.Overwrite).csv("E:\\Scienaptic\\HP\\Pricing\\Testing\\Commercial_Alteryx\\spark-intermediate\\claims_consolidated.csv")
    /*Group - 365*/
    val claimsResellerWEDSKUProgramGroup = xsClaimsSource.groupOperation(RESELLER_WED_SKU_PROGRAM_AGG_CLAIM_REBATE_QUANTITY)
    val claimsResellerWEDSKUProgramGroupDF = doGroup(claimsAndSKUJoinsUnionDF, claimsResellerWEDSKUProgramGroup).get
    //writeDF(claimsResellerWEDSKUProgramGroupDF,"Reseller_Cluster_Grouping_output")   //TODO: Massive difference: gives 1362, spark - 74841 (WED, others null in Alteryx)

    /*
    * Filter Program = Big_Deal*/
    val programFilterBigDeal = xsClaimsSource.filterOperation(FILTER_PROGRAM_EQ_BID_DEAL)
    val claimsProgramFilterBigDealDF = doFilter(claimsResellerWEDSKUProgramGroupDF, programFilterBigDeal, programFilterBigDeal.conditionTypes(NUMERAL0)).get
    //writeDF(claimsProgramFilterBigDealDF,"claimsProgramFilterBigDealDF")
    //385
    val programFilterPromoOptionC = xsClaimsSource.filterOperation(FILTER_PROGRAM_EQ_PROMO_OPTION_C)
    val claimsProgramFilterPromoOptionCDF = doFilter(claimsResellerWEDSKUProgramGroupDF, programFilterPromoOptionC, programFilterPromoOptionC.conditionTypes(NUMERAL0)).get
    //writeDF(claimsProgramFilterPromoOptionCDF,"Program_Promo_Option_C")

    val claimsProgramOptionCUniqueDF = claimsProgramFilterPromoOptionCDF.dropDuplicates(List("Reseller Cluster","Week End Date","Consol SKU")) //Dont Remove
    //writeDF(claimsProgramOptionCUniqueDF,"claimsProgramOptionCUniqueDF")

    /*
    * Group claims on Reseller, SKU, Program, WED and sum Aggregate Claim Quantity - 400
    * */
    val claimsGroupResellerWEDSKUSumClaimQuant = xsClaimsSource.groupOperation(RESELLER_WED_SKU_PROGRAM_AGG_CLAIM_QUANTITY)
    val claimsGroupResellerWEDSKUSumClaimQuantDF = doGroup(claimsProgramFilterBigDealDF, claimsGroupResellerWEDSKUSumClaimQuant).get
    //writeDF(claimsGroupResellerWEDSKUSumClaimQuantDF,"Program_Big_Deal")
    /*
    * Create 'Outside Promo Date' = IF Partner Ship Calendar Date > End Date + 3 days THEN "Y" ELSE "N" ENDIF
    * */
    val claimsWithOutsidePromoDateFeatDF = claimsAndMasterCalendarInnerJoinDF.withColumn("EndDatePlus3", date_add(col("End Date").cast("timestamp"), 3))
    .withColumn("Outside Promo Date", when(col("Partner Ship Calendar Date").cast("timestamp")>col("EndDatePlus3").cast("timestamp"),"Y")
    .otherwise("N"))
    .drop("EndDatePlus3") //17057
    //writeDF(claimsWithOutsidePromoDateFeatDF,"claimsWithOutsidePromoDateFeatDF")

    /*
    * Filter where 'Outside Promo Date' = N and Y
    * */
    val claimsOutsidePromoDateFilter = xsClaimsSource.filterOperation(FILTER_OUTSIDE_PROMO_DATE_EQ_N)
    val claimsOutsidePromoDateEqNDF = doFilter(claimsWithOutsidePromoDateFeatDF, claimsOutsidePromoDateFilter, claimsOutsidePromoDateFilter.conditionTypes(NUMERAL0)).get
    //writeDF(claimsOutsidePromoDateEqNDF,"claims_Filter_Outside_Promo_Date_N")
    val claimsOutsidePromoDateEqYFilter = xsClaimsSource.filterOperation(FILTER_OUTSIDE_PROMO_DATE_EQ_Y)
    val claimsOutsidePromoDateEqYDF = doFilter(claimsWithOutsidePromoDateFeatDF, claimsOutsidePromoDateEqYFilter, claimsOutsidePromoDateEqYFilter.conditionTypes(NUMERAL0)).get
    //writeDF(claimsOutsidePromoDateEqYDF,"Outside_Promo_N_Not_Eq")
    /*
    * Group WED, SKU, Promo Code and sum Sum_Claim_Quantity
    * And
    * Group SKU, Promo Code and Avg Sum_Sum_Claim_Quantity
    * */
    val claimsWEDSKUPromoCodeGroup = xsClaimsSource.groupOperation(WED_SKU_C2B_PROMO_CODE_SUM_CLAIM_QUANTITY)
    val claimsWEDSKUPromoCodeGroupDF = doGroup(claimsOutsidePromoDateEqNDF, claimsWEDSKUPromoCodeGroup).get
    //133
    val claimsSKUPromoCodeGroup = xsClaimsSource.groupOperation(SKU_PROMO_CODE_AVG_CLAIM_QUANTITY)
    val claimsSKUPromoCodeGroupDF = doGroup(claimsWEDSKUPromoCodeGroupDF, claimsSKUPromoCodeGroup).get
    //writeDF(claimsSKUPromoCodeGroupDF,"claimsSKUPromoCodeGroupDF")
    //134
    val claimsWEDSKUClaimRebatePromoCodeGroup = xsClaimsSource.groupOperation(WED_SKU_CLAIM_REBATE_PROMO_CODE_SUM_CLAIM_QUANTITY)
    val claimsWEDSKUClaimRebatePromoCodeGroupDF = doGroup(claimsOutsidePromoDateEqYDF, claimsWEDSKUClaimRebatePromoCodeGroup).get
    /*
    * Join Claims Outside Promo Code = Y and N
    * AND create 'Include' as IF Sum_Sum_Claim Quantity > 0.2*Avg_Sum_Sum_Claim Quantity THEN "Y" ELSE "N"
    * */
    val claimsJoinOutsidePromoYAndN = xsClaimsSource.joinOperation(CLAIMS_OUTSIDE_PROMO_Y_AND_N)
    val claimsSKUPromoCodeGroupRenamedDF = claimsSKUPromoCodeGroupDF
    .withColumnRenamed("Base SKU","Right_Base SKU")
    .withColumnRenamed("C2B Promo Code","Right_C2B Promo Code")
    val claimsJoinOutsidePromoYAndNDF = doJoinAndSelect(claimsWEDSKUClaimRebatePromoCodeGroupDF, claimsSKUPromoCodeGroupRenamedDF, claimsJoinOutsidePromoYAndN)(INNER)
    //writeDF(claimsJoinOutsidePromoYAndNDF,"Outside_Promo_Grouped_Join")
    val claimsJoinOutsidePromoYAndNWithIncludeDF = claimsJoinOutsidePromoYAndNDF
    .withColumn("Include", when(col("Sum_Sum_Claim Quantity") > lit(0.2)*col("Avg_Sum_Sum_Claim Quantity"),"Y").otherwise("N"))
    //writeDF(claimsJoinOutsidePromoYAndNWithIncludeDF,"claimsJoinOutsidePromoYAndNWithIncludeDF")

    /*
    * Filter where Include = Y ,i.e, Sum_Sum_Claim Quantity > 0.2 * Avg_Sum_Sum_Claim Quantity
    * */
    val claimsIncludeEqYFilter = xsClaimsSource.filterOperation(FILTER_INCLUDE_EQ_Y)
    val claimsIncludeEqYDF = doFilter(claimsJoinOutsidePromoYAndNWithIncludeDF, claimsIncludeEqYFilter, claimsIncludeEqYFilter.conditionTypes(NUMERAL0)).get
    //writeDF(claimsIncludeEqYDF,"claims_include_Eq_Y")
    /*
    * Group C2B Promo Code and Base SKU and Max Week End Date - 144
    * */

    val claimsPromoCodeSKUGroup = xsClaimsSource.groupOperation(PROMO_CODE_SKU_MAX_WED)//TODO: Max on Week End Date
    val claimsPromoCodeSKUDF = doGroup(claimsIncludeEqYDF, claimsPromoCodeSKUGroup).get
    //writeDF(claimsPromoCodeSKUDF,"claimsPromoCodeSKUDF")
    /*
    * Join Claims back with Calendar after aggregations - 146
    * */
    val claimsAndCalendarAfterAggJoin = xsClaimsSource.joinOperation(CLAIMS_AND_MASTER_CALENDAR_AFTER_AGGREGATION)
    val claimsPromoCodeSKURenamedDF = claimsPromoCodeSKUDF.withColumnRenamed("C2B Promo Code","Right_C2B Promo Code")
    val claimsAndCalendarAfterAggJoinMap = doJoinAndSelect(rawCalendarSelectDF, claimsPromoCodeSKURenamedDF, claimsAndCalendarAfterAggJoin)
    val claimsAndCalendarAfterAggLeftJoinDF = claimsAndCalendarAfterAggJoinMap(LEFT)
    val claimsAndCalendarAfterAggInnerJoinDF = claimsAndCalendarAfterAggJoinMap(INNER)
    //writeDF(claimsAndCalendarAfterAggInnerJoinDF,"JoiclaimsPromoCodeSKUDFn_Back_Calendar")
    /*
    * Add 'New End Date' using 'End Date'
    * */
    val claimsAndCalendarAfterAggLeftJoinWithNewEndDateDF = claimsAndCalendarAfterAggLeftJoinDF.withColumn("New End Date",col("End Date"))
    val claimsAndCalendarAfterAggInnerJoinWithNewEndDateDF = claimsAndCalendarAfterAggInnerJoinDF
    .withColumn("MaxWeekDiff", datediff(to_date(unix_timestamp(col("Max_Week_End_Date"),"dd-MM-yyyy").cast("timestamp")), col("End Date")))
    .withColumn("New End Date",when(col("MaxWeekDiff")>14, date_add(col("End Date").cast("timestamp"), 14))
      .when(col("Max_Week_End_Date").cast("timestamp")>col("End Date").cast("timestamp"),col("Max_Week_End_Date"))
      .otherwise(col("End Date")))
    .drop("MaxWeekDiff")
    //writeDF(claimsAndCalendarAfterAggInnerJoinWithNewEndDateDF,"claimsAndCalendarAfterAggInnerJoinWithNewEndDateDF")
    /*
    * Union Claims and Master Calendar join with 'New End Date' Variable added - 150
    * */
    val claimsAndCalendarAfterAggJoinsUnionDF = doUnion(claimsAndCalendarAfterAggLeftJoinWithNewEndDateDF, claimsAndCalendarAfterAggInnerJoinWithNewEndDateDF).get
    //writeDF(claimsAndCalendarAfterAggJoinsUnionDF,"New_End_Date-end_Date_Union")
    /*
    * Add 'End Date Change' by comparing 'End Date' and 'New End Date' - 155
    * And
    * Drop duplicates on SKU, C2B Promo Code, Start Date, New End Date - 157
    * */
    val claimsWithEndDateChangeDF = claimsAndCalendarAfterAggJoinsUnionDF
    .withColumn("End Date Change",
    when(col("End Date")===col("New End Date"),"N").otherwise("Y"))
    .dropDuplicates(List("SKU","C2B Promo Code","Start Date","New End Date"))
    //writeDF(claimsWithEndDateChangeDF,"New_End_Date_Unique")
    //IMPORTANT: Rebate Amount Column has $ in it.
    /*
    * Cartesian join WED and Claims with 'End Date Change' variable - 158
    * */
    val wedSelectRenamedDF = wedRenamedDF.withColumnRenamed("season","Source_season").withColumnRenamed("Season_Ordered","Source_Season_Ordered")
    .select("Week_End_Date","Source_season","Source_Season_Ordered","Cal_Month","Cal_Year","Fiscal_Qtr","Fiscal_Year")
    val claimsCartJoinWEDDF = wedSelectRenamedDF.join(claimsWithEndDateChangeDF)
    //writeDF(claimsCartJoinWEDDF,"claimsCartJoinWEDDF_DF")

    /*
    * Create variables 'Start - Week End', 'Week End - End', 'Option Type' - 159
    * */

    val claimsJoinWEDWithStartEndnOptionTypeVarsDF = claimsCartJoinWEDDF
    .withColumn("Start - Week End", datediff(col("Start Date").cast("timestamp"),col("Week_End_Date").cast("timestamp")))
    .withColumn("Week End - End", datediff(col("Week_End_Date").cast("timestamp"),col("New End Date").cast("timestamp")))
    .withColumn("Option Type",
    when(col("Promo Name").contains("Option B"),"Option B")
      .when(col("Promo Name").contains("Option C"),"Option C")
      .otherwise("All"))
    claimsJoinWEDWithStartEndnOptionTypeVarsDF.printSchema()
    //writeDF(claimsJoinWEDWithStartEndnOptionTypeVarsDF,"AppendNewFieldInUnique_DF")

    /*
    * Filter 'Start - Week End' <= -3 AND 'Week End - End' <= 3
    * */
    val claimsJoinWEDWithStartnWEDFilteredDF = claimsJoinWEDWithStartEndnOptionTypeVarsDF
    .withColumn("StartMinusEnd", col("Start - Week End"))
    .withColumn("WeekEndMinusEnd", col("Week End - End"))
    .filter("StartMinusEnd <= -3").filter("WeekEndMinusEnd <= 3")
    .drop("StartMinusEnd").drop("WeekEndMinusEnd")
    .select("SKU","C2B Promo Code","Promo Name","Season","BU","Product","Rebate Amount","Week_End_Date","Option Type","Source_season","Source_Season_Ordered","Cal_Month","Cal_Year","Fiscal_Year","Fiscal_Qtr")
    //writeDF(claimsJoinWEDWithStartnWEDFilteredDF,"claimsJoinWEDWithStartnWEDFilteredDF")
    /*
    * Sort SKU, WED and Rebate Amount - 162
    * */
    val claimsSKUWEDRebateAmountSort = xsClaimsSource.sortOperation(ASC_SKU_WED_DESC_REBATE)
    val claimsSKUWEDRebateAmountSortDF = doSort(claimsJoinWEDWithStartnWEDFilteredDF, claimsSKUWEDRebateAmountSort.ascending, claimsSKUWEDRebateAmountSort.descending).get
    .dropDuplicates(List("SKU","C2B Promo Code","Week_End_Date"))
    //writeDF(claimsSKUWEDRebateAmountSortDF,"UniquesFromNewEndDate")

    /*
    * Filter Option Type != Option C - 368
    * */
    val claimsOptionTypeNotEqOptionCFilter = xsClaimsSource.filterOperation(OPTION_TYPE_NOT_EQUALS_OPTION_C)
    val claimsOptionTypeNotEqOptionCFilterDF = doFilter(claimsSKUWEDRebateAmountSortDF, claimsOptionTypeNotEqOptionCFilter, claimsOptionTypeNotEqOptionCFilter.conditionTypes(NUMERAL0)).get
    //writeDF(claimsOptionTypeNotEqOptionCFilterDF,"claimsOptionTypeNotEqOptionCFilterDF")
    /*
    * Group SKU, WED, Promo Code and max aggregate Rebate Amount - 364
    * */
    val claimsSKUWEDPromoCodeGroup = xsClaimsSource.groupOperation(SKU_WED_PROMO_CODE_MAX_REBATE_AMOUNT)
    val claimsSKUWEDPromoCodeGroupDF = doGroup(claimsOptionTypeNotEqOptionCFilterDF, claimsSKUWEDPromoCodeGroup).get
    //writeDF(claimsSKUWEDPromoCodeGroupDF,"claimsSKUWEDPromoCodeGroupDF")
    /*
    * Select C2B Promo Code and Promo Name to compute distinct - 19
    * */
    val masterCalSelectForDistinct = rawCalendarSource.selectOperation(SELECT_FOR_DISTINCT)
    val masterCalSelectForDistinctDF = doSelect(rawCalendarSelectDF, masterCalSelectForDistinct.cols, masterCalSelectForDistinct.isUnknown).get
    .dropDuplicates(List("C2B Promo Code","Promo Name"))
    //writeDF(masterCalSelectForDistinctDF,"PromoIDList")
    /* ---------------------------- TIDY HISTORICAL POS ------------------------- */
    /*
    * Filter Channel equals commercial - 418
    * */
    val tidyHistChannelEqCommercialFilter = tidyHistSource.filterOperation(FILTER_CHANNEL_EQ_COMMERCIAL)
    val tidyHistChannelEqCommercialFilterDF = doFilter(tidyHistRenamedDF, tidyHistChannelEqCommercialFilter, tidyHistChannelEqCommercialFilter.conditionTypes(NUMERAL0)).get
    //writeDF(tidyHistChannelEqCommercialFilterDF,"Channel_Eq_Commercial_FILTER")
    /*
    * Group SKU, Reseller, WED, Season, Cal, fiscal year, quarter and sum Qty
    * AND
    * Rename grouping columns - 421
    * */
    val tidyHistSKUResellerWEDSeasonCalFiscalYrGroup = tidyHistSource.groupOperation(SKU_SHORT_RESELLER_WED_SEASON_CAL_FISCAL_SUM_QUANT)
    val tidyHistRenameAfterGroup = tidyHistSource.renameOperation(RENAME_SEASON_CAL_FISCAL_AFTER_GROUP)
    val tidyHistSKUResellerWEDSeasonCalFiscalYrGroupDF = doGroup(tidyHistChannelEqCommercialFilterDF, tidyHistSKUResellerWEDSeasonCalFiscalYrGroup).get
    val tidyHistRenameAfterGroupDF = Utils.convertListToDFColumnWithRename(tidyHistRenameAfterGroup, tidyHistSKUResellerWEDSeasonCalFiscalYrGroupDF)
    //writeDF(tidyHistRenameAfterGroupDF,"tidyHistRenameAfterGroupDF")
    /*
    * Create Features - Big_Deal_Qty, eTailer, IR, Non_Big_Deal_Qty, Qty, Promo Flag - 419
    * */
    val tidyHistWithDealQtyETailerIRPromoFlagDF = tidyHistRenameAfterGroupDF
    .withColumn("Big_Deal_Qty",lit(0))
    .withColumn("eTailer",lit(0))
    .withColumn("IR",lit(0))
    .withColumn("Non_Big_Deal_Qty", col("Sum_Qty"))
    .withColumn("Qty", col("Big_Deal_Qty")+col("Non_Big_Deal_Qty"))
    .withColumn("Promo Flag", lit(0))
    //writeDF(tidyHistWithDealQtyETailerIRPromoFlagDF,"tidyHistWithDealQtyETailerIRPromoFlagDF")
    /*
    * Join Tidy Hist pos with SKU Hierarchy on SKU - 420
    * */
    val tidyHistAndSKUHierJoin = tidyHistSource.joinOperation(TIDY_HIST_AND_SKU_HIER)
    val tidyHistAndSKUHierInnerJoinDF = doJoinAndSelect(tidyHistWithDealQtyETailerIRPromoFlagDF, skuHierarchySelectDF, tidyHistAndSKUHierJoin)(INNER)
    .withColumnRenamed("season","Season")
    .withColumnRenamed("Consol SKU","SKU")
    .withColumnRenamed("Abbreviated Name","SKU_Name")
    //writeDF(tidyHistAndSKUHierInnerJoinDF,"BIG_DEAL_QTY_ETAILER_NON_BIG_DEAL")

    /* ---------------------------- STT ---------------------------- */
    /*
    * Join STT Onyx and Accounts on Account Company
    * */
    val sttOnyxAndAccountsJoin = sttOnyxSource.joinOperation(STT_ONYX_AND_AUX_ACCOUNTS)
    val commAccountsSelectRenamedForSTTDF = commAccountsSelectDF.withColumnRenamed("Account Company","Right_Account Company")
    val sttOnyxAndAccountsJoinMap = doJoinAndSelect(sttONYXSelectDF, commAccountsSelectRenamedForSTTDF, sttOnyxAndAccountsJoin)
    val sttOnyxAndAccountsInnerJoinDF = sttOnyxAndAccountsJoinMap(INNER)
    val sttOnyxAndAccountsLeftJoinDF = sttOnyxAndAccountsJoinMap(LEFT)
    .withColumn("Reseller Cluster",lit("Other"))
      /* Avik Change: April 13: New columns Account conol, Grouping, VPA weren't added */
      .withColumn("Account Consol", lit("Others"))
      .withColumn("Grouping", lit("Distributor"))
      .withColumn("VPA", lit("Non-VPA"))
    //writeDF(sttOnyxAndAccountsLeftJoinDF,"CI_Join")
    //writeDF(sttOnyxAndAccountsInnerJoinDF,"CI_JOIN_INNER")
    val sttOnyxAndAccountJoinsUnionDF = doUnion(sttOnyxAndAccountsLeftJoinDF, sttOnyxAndAccountsInnerJoinDF).get
    //writeDF(sttOnyxAndAccountJoinsUnionDF,"Account_merge_UNION")

    /*
    * Merge Consol SKU using SKU Hierarchy - 409
    * */
    val sttOnyxAndSkuHierJoin = sttOnyxSource.joinOperation(STT_ONYX_AND_SKU_HIERARCHY)
    val skuHierarchySelectRenamedForSTTDF = skuHierarchySelectDF.withColumnRenamed("SKU","Join_SKU")
    .withColumnRenamed("Consol SKU","SKU")
    val sttOnyxAndSkuHierInnerJoinDF = doJoinAndSelect(sttOnyxAndAccountJoinsUnionDF, skuHierarchySelectRenamedForSTTDF, sttOnyxAndSkuHierJoin)(INNER)
    //writeDF(sttOnyxAndSkuHierInnerJoinDF,"MERGE_CONSOLE_SKU")
    /*
    * Merge WED using WED - 410
    * */
    val sttOnyxAndWEDJoin = sttOnyxSource.joinOperation(STT_ONYX_AND_WED_MERGE_WED)
    val sttOnyxAndWEDInnerJoinDF = doJoinAndSelect(sttOnyxAndSkuHierInnerJoinDF, wedRenamedDF, sttOnyxAndWEDJoin)(INNER)
    //writeDF(sttOnyxAndWEDInnerJoinDF,"sttOnyxAndWEDInnerJoinDF")
    /*
    * Group Reseller Cluster, SKU, Week_End_Date - 411 AND add 7 days to Week_End_Date - 414 - Week shift
    * */
    val wedSelectDFsttOnyxResellerSKUWEDGroup = sttOnyxSource.groupOperation(RESELLER_SKU_WED_SUM_AGG_INVENTORY_TOTAL)
    val sttOnyxResellerSKUWEDGroupDF = doGroup(sttOnyxAndWEDInnerJoinDF, wedSelectDFsttOnyxResellerSKUWEDGroup).get
    .withColumn("Week_End_Date",date_add(col("Week_End_Date").cast("timestamp"), 7))
    //writeDF(sttOnyxResellerSKUWEDGroupDF,"Account_WEEK_SHIFT")
    /* ---------------------------- ST ----------------------------- */
    /*
    * Join ST ONYX and Aux Accounts - 345
    * */
    val stOnyxAndAccountsJoin = stOnyxSource.joinOperation(ST_ONYX_AND_AUX_ACCOUNTS)
    val commAccountsSelectRenamedDF = commAccountsSelectDF
    .withColumnRenamed("Account Company","Right_Account Company")
    val stOnyxAndAccountsJoinMap = doJoinAndSelect(stONYXSelectDF, commAccountsSelectRenamedDF, stOnyxAndAccountsJoin)
    val stOnyxAndAccountsLeftJoinDF = stOnyxAndAccountsJoinMap(LEFT)
    val stOnyxAndAccountsInnerJoinDF = stOnyxAndAccountsJoinMap(INNER)
    //writeDF(stOnyxAndAccountsLeftJoinDF,"ST_Account_JOIN_Inner")
    /*
    * Add Features - 'Account Consol', 'Grouping', 'VPA', 'Reseller Cluster'
    * */
    val stOnyxAndAccountWithFeaturesDF = stOnyxAndAccountsLeftJoinDF
    .withColumn("Account Consol",lit("Others"))
    .withColumn("Grouping", lit("Distributor"))
    .withColumn("VPA", lit("Non-VPA"))
    .withColumn("Reseller Cluster", lit("Other"))
    //writeDF(stOnyxAndAccountWithFeaturesDF,"stOnyxAndAccountWithFeaturesDF")
    /*
    * Union ST joins
    * */
    val stOnyxAndAccountJoinsUnion = doUnion(stOnyxAndAccountWithFeaturesDF, stOnyxAndAccountsInnerJoinDF).get
    //writeDF(stOnyxAndAccountJoinsUnion,"AccountSTUnion")
    /*
    * Join ST ONYX And SKU Hierarchy on Product Base Id and SKU - 348
    * */
    val stOnyxAndSKUHierJoin = stOnyxSource.joinOperation(ST_ONYX_AND_SKU_HIERARCHY)
    val skuHierarchySelectRenamedDF = skuHierarchySelectDF.withColumnRenamed("SKU","Right_SKU")
    .withColumnRenamed("Consol SKU","SKU")
    val stOnyxAndSKUHierJoinMap = doJoinAndSelect(stOnyxAndAccountJoinsUnion, skuHierarchySelectRenamedDF, stOnyxAndSKUHierJoin)
    val stOnyxAndSKUHierLeftJoinDF = stOnyxAndSKUHierJoinMap(LEFT)
    val stOnyxAndSKUHierInnerJoinDF = stOnyxAndSKUHierJoinMap(INNER)
    //writeDF(stOnyxAndSKUHierInnerJoinDF,"stOnyxAndSKUHierInnerJoinDF"); //writeDF(stOnyxAndSKUHierLeftJoinDF,"stOnyxAndSKUHierLeftJoinDF")
    /*
    * Group Left join on Product Base Id, Product Base Desc and sum Aggregate Sell Thru Qty, Sell-to Qty
    * And
    * Sort on Sell Thru and Sell to Quantities
    * */
    val stOnyxProductBaseGroup = stOnyxSource.groupOperation(PRODUCT_BASE_SUM_AGG_SELL_THRU_SELL_TO_QTY)
    val stOnyxProductBaseGroupDF = doGroup(stOnyxAndSKUHierLeftJoinDF, stOnyxProductBaseGroup).get
    val stOnyxSellThruAndToSort = stOnyxSource.sortOperation(DESC_SELL_THRU_SELL_TO)
    val stOnyxSellThruAndToSortDF = doSort(stOnyxProductBaseGroupDF, stOnyxSellThruAndToSort.ascending, stOnyxSellThruAndToSort.descending).get
    //writeDF(stOnyxSellThruAndToSortDF,"CHECK_SKU_Fallout")

    /*
    * Group Week on St ONYX and SKU Join - 350
    * */
    val stOnyxWeekJoin = stOnyxSource.joinOperation(ST_ONYX_AND_WED)
    val stOnyxWeekInnerJoinDF = doJoinAndSelect(stOnyxAndSKUHierInnerJoinDF, wedRenamedDF, stOnyxWeekJoin)(INNER)
    .withColumnRenamed("Abbreviated Name","SKU_Name")
    //writeDF(stOnyxWeekInnerJoinDF,"stOnyxWeekInnerJoinDF")
    /*
    * 351 - Group Deal ID 1, eTailers,
    * */
    val stOnyxGroup = stOnyxSource.groupOperation(DEAL_ETAILERS_ACCOUNT_SEASON_CAL_SKU_FISCAL_SUM_AGG_SELL_THRU_TO_QTY)
    val stOnyxGroupDF = doGroup(stOnyxWeekInnerJoinDF, stOnyxGroup).get
    //writeDF(stOnyxGroupDF,"ST_Join_Group")
    //TODO: GA and ES date are blank here!
    /*
    * Join ST and Promo ID list - 352
    * */
    val stOnyxAndPromoIDJoin = stOnyxSource.joinOperation(ST_ONYX_AND_PROMO_ID)
    val stOnyxAndPromoIDJoinMap = doJoinAndSelect(stOnyxGroupDF, masterCalSelectForDistinctDF, stOnyxAndPromoIDJoin)
    val stOnyxAndPromoIDLeftJoinDF = stOnyxAndPromoIDJoinMap(LEFT)
    val stOnyxAndPromoIDInnerJoinDF = stOnyxAndPromoIDJoinMap(INNER)
    //writeDF(stOnyxAndPromoIDInnerJoinDF,"ST_INNER_Join_Sample")
    /*
    * Add 'Big Deal Qty', 'Non Big Deal Qty', 'Qty', 'Promo' variables - 355
    * AND 356
    * */
    val stOnyxAndPromoLeftJoinWithDealQtyPromoDF = stOnyxAndPromoIDLeftJoinDF
    .withColumn("Big Deal Qty", when(col("Deal ID 1").isNull, 0).otherwise(col("ST Qty")+col("STT Qty")))
    .withColumn("Non Big Deal Qty", when(col("Deal ID 1").isNull, col("ST Qty")+col("STT Qty")).otherwise(0))
    .withColumn("Qty", col("STT Qty")+col("ST Qty"))
    .withColumn("Promo", lit("No Promo"))

    val stOnyxAndPromoIDInnerJoinWithDealQtyPromoDF = stOnyxAndPromoIDInnerJoinDF
    .withColumn("Big Deal Qty",lit(0))
    .withColumn("Non Big Deal Qty", col("ST Qty")+col("STT Qty"))
    .withColumn("Qty", col("ST Qty")+col("STT Qty"))
    .withColumn("Promo", when(col("Promo Name")===lit("Option C"), "Option C").otherwise("Commercial"))

    val stOnyxAndPromoJoinsUnionDF = doUnion(stOnyxAndPromoLeftJoinWithDealQtyPromoDF, stOnyxAndPromoIDInnerJoinWithDealQtyPromoDF).get
    //writeDF(stOnyxAndPromoJoinsUnionDF,"ST_Union_11")
    /*
    * Group on all columns and compote sum for Big Deal, Non Big deal Qty and Qty - 359
    * */
    val stOnyxAllColumSumAggQtyGroup = stOnyxSource.groupOperation(GROUP_ALL_SUM_AGG_BIG_NON_BIG_DEAL_QTY)
    val stOnyxAllColumSumAggQtyGroupDF = doGroup(stOnyxAndPromoJoinsUnionDF, stOnyxAllColumSumAggQtyGroup).get
    //writeDF(stOnyxAllColumSumAggQtyGroupDF,"stOnyxAllColumSumAggQtyGroupDF")
    /*
    * Group All columns except Account, Promo, Grouping and Sum aggregation Qty, Big deal Qty, Non big deal qty - 369
    * */
    val stOnyxAllExceptAccountGroupPromoGroup = stOnyxSource.groupOperation(GROUP_ALL_EXCEPT_ACCOUNT_GROUPING_PROMO_SUM_AGG_QTY_AND_DEAL_QTS)
    val stOnyxAllExceptAccountGroupPromoGroupDF = doGroup(stOnyxAllColumSumAggQtyGroupDF, stOnyxAllExceptAccountGroupPromoGroup).get
    //writeDF(stOnyxAllExceptAccountGroupPromoGroupDF,"stOnyxAllExceptAccountGroupPromoGroupDF")
    /*
    * Filter eTailers Equals and not equals 'Y'
    * */
    val stOnyxETailersNotEqYFilter = stOnyxSource.filterOperation(FILTER_ETAILERS_NOT_EQ_Y)
    val stOnyxETailersEqYFilter = stOnyxSource.filterOperation(FILTER_ETAILERS_EQ_Y)
    val stOnyxETailersNotEqYFilterDF = doFilter(stOnyxAllExceptAccountGroupPromoGroupDF, stOnyxETailersNotEqYFilter, stOnyxETailersNotEqYFilter.conditionTypes(NUMERAL0)).get
    val stOnyxETailersEqYFilterDF = doFilter(stOnyxAllExceptAccountGroupPromoGroupDF, stOnyxETailersEqYFilter, stOnyxETailersEqYFilter.conditionTypes(NUMERAL0)).get
    //writeDF(stOnyxETailersEqYFilterDF,"ETAILERS_EQ_Y")
    /*
    * Join ST ONYX With Aggregated Claims on Reseller Cluster, SKU, Week End Date
    * */
    val stOnyxAndAggClaimsJoin = stOnyxSource.joinOperation(ST_ONYX_AND_CLAIMS_AGGREGATED)
    val claimsGroupResellerWEDSKUSumClaimQuantRenamedDF = claimsGroupResellerWEDSKUSumClaimQuantDF
    .withColumnRenamed("Reseller Cluster","Right_Reseller Cluster")
    //Check whether Week_End_Date and Week End Date in same format. other columns - SKU, Console SKU and Reseller_Cluster and Right_Reseller Cluster
    val stOnyxAndAggClaimsJoinMap = doJoinAndSelect(stOnyxETailersNotEqYFilterDF, claimsGroupResellerWEDSKUSumClaimQuantRenamedDF, stOnyxAndAggClaimsJoin)
    val stOnyxAndAggClaimsLeftJoinDF = stOnyxAndAggClaimsJoinMap(LEFT)
    .withColumnRenamed("Sum_Sum_Claim Quantity","Claimed Big Deal Qty")
    val stOnyxAndAggClaimsInnerJoinDF = stOnyxAndAggClaimsJoinMap(INNER)
    .withColumnRenamed("Sum_Sum_Claim Quantity","Claimed Big Deal Qty")
    //writeDF(stOnyxAndAggClaimsInnerJoinDF,"BIG_DEAL_Completion")
    //writeDF(stOnyxAndAggClaimsLeftJoinDF,"stOnyxAndAggClaimsLeftJoinDF")
    /*
    * Add Variables 'Big Deal Claim Difference', 'Big_Deal_Qty','Non_Big_Deal_Qty' - 381
    * AND
    * Union Left and inner join (With new variables) - 382
    * */
    val stOnyxAndAggClaimsInnerJoinWithClaimDifferenceDealQtyDF = stOnyxAndAggClaimsInnerJoinDF
    .withColumn("Big Deal Claim Difference", when(col("Claimed Big Deal Qty")<=col("Qty"), col("Claimed Big Deal Qty")-col("Big_Deal_Qty")).otherwise(0))
    .withColumn("Big_Deal_Qty", col("Big_Deal_Qty")+col("Big Deal Claim Difference"))
    .withColumn("Non_Big_Deal_Qty", col("Non_Big_Deal_Qty")-col("Big Deal Claim Difference"))
    //writeDF(stOnyxAndAggClaimsInnerJoinWithClaimDifferenceDealQtyDF,"stOnyxAndAggClaimsInnerJoinWithClaimDifferenceDealQtyDF")
    val stOnyAndAggClaimsJoinsUnion = doUnion(stOnyxAndAggClaimsLeftJoinDF, stOnyxAndAggClaimsInnerJoinWithClaimDifferenceDealQtyDF).get
    //writeDF(stOnyAndAggClaimsJoinsUnion,"ST_Nig_Deal_Claim_Union")
    /*
    * Option C Join - 386
    * */
    val stOnyxOptionCJoin = stOnyxSource.joinOperation(ST_ONYX_OPTION_C_JOIN)
    //val claimsProgramFilterPromoOptionCRenamedDF = claimsProgramOptionCUniqueDF
    val claimsProgramFilterPromoOptionCRenamedDF = claimsProgramFilterPromoOptionCDF
    .withColumnRenamed("Reseller Cluster","Right_Reseller Cluster")
    .withColumnRenamed("Claim Partner Unit Rebate", "Option C IR")
    .withColumnRenamed("Sum_Sum_Claim Quantity", "Claimed Option C Qty")
    val stOnyxOptionCJoinMap = doJoinAndSelect(stOnyAndAggClaimsJoinsUnion, claimsProgramFilterPromoOptionCRenamedDF, stOnyxOptionCJoin)
    val stOnyxOptionCLeftJoinDF = stOnyxOptionCJoinMap(LEFT)
    val stOnyxOptionCInnerJoinDF = stOnyxOptionCJoinMap(INNER)
    //writeDF(stOnyxOptionCInnerJoinDF,"stOnyxOptionCInnerJoinDF"); //writeDF(stOnyxOptionCLeftJoinDF,"stOnyxOptionCLeftJoinDF")
    /*
    * Add Variables Option C Qty Adj, Non_Big_Deal_Qty, Qty
    * */
    val stOnyxOptionCInnerJoinWithOptionCQtyDealQty = stOnyxOptionCInnerJoinDF
    .withColumn("Option C Qty Adj", when(col("Claimed Option C Qty")>col("Qty"), col("Qty")).otherwise(col("Claimed Option C Qty")))
    .withColumn("Non_Big_Deal_Qty", col("Non_Big_Deal_Qty")-col("Option C Qty Adj"))
    .withColumn("Qty", col("Qty")-col("Option C Qty Adj"))
    //writeDF(stOnyxOptionCInnerJoinWithOptionCQtyDealQty,"stOnyxOptionCInnerJoinWithOptionCQtyDealQty")
    /*
    * Union 395 - Bring back eTailers
    */
    //val stOnyxETailersEqYFilterDF2 = spark.read.option("header","true").option("inferSchema","true").csv("E:\\Scienaptic\\HP\\Pricing\\Testing\\Commercial_Alteryx\\spark-intermediate\\ETAILERS_EQ_Y.csv")
    val stOnyxUnionETailersOptionCDF = doUnion(stOnyxOptionCLeftJoinDF, doUnion(stOnyxOptionCInnerJoinWithOptionCQtyDealQty, stOnyxETailersEqYFilterDF).get).get
    //writeDF(stOnyxUnionETailersOptionCDF,"Bringing_Back_ETailers")
    /*
    * Filter Option C Qty Adj > 0 - 388
    * */
    val stOnyxOptionCQtyAdjFilter = stOnyxSource.filterOperation(FILTER_OPTION_C_QTY_ADJ_GR_0)
    val stOnyxOptionCQtyAdjFilterDF = doFilter(stOnyxOptionCInnerJoinWithOptionCQtyDealQty/*stOnyxUnionETailersOptionCDF*/, stOnyxOptionCQtyAdjFilter, stOnyxOptionCQtyAdjFilter.conditionTypes(NUMERAL0)).get
    //writeDF(stOnyxOptionCQtyAdjFilterDF,"Option_C_Qty_Adj_GT_0")
    /*
    * Add Reseller Cluster, Qty, Non_Big_Deal_Qty, Big_Deal_Qty, IR, Promo Flag  - 394
    * */
    val stOnyxOptionCWithResellerQtyIRPromoFlagDF = stOnyxOptionCQtyAdjFilterDF
    .withColumn("Reseller Cluster", lit("Other - Option C"))
    .withColumn("Qty", col("Option C Qty Adj"))
    .withColumn("Non_Big_Deal_Qty", col("Option C Qty Adj"))
    .withColumn("Big_Deal_Qty", lit(0))
    .withColumn("IR", col("Option C IR"))
    .withColumn("Promo Flag", lit(1))
    //writeDF(stOnyxOptionCWithResellerQtyIRPromoFlagDF,"Reseller Cluster")
    /*
    * Promo Join - 370
    * */
    val stOnyxPromoJoin = stOnyxSource.joinOperation(ST_ONYX_PROMO_JOIN)
    val claimsSKUWEDPromoCodeGroupRenamedDF = claimsSKUWEDPromoCodeGroupDF
    .withColumnRenamed("SKU","Right_SKU")
    .withColumnRenamed("Week_End_Date","Right_Week_End_Date")
    val stOnyxPromoJoinMap = doJoinAndSelect(stOnyxUnionETailersOptionCDF, claimsSKUWEDPromoCodeGroupRenamedDF, stOnyxPromoJoin)
    val stOnyxPromoLeftJoinDF = stOnyxPromoJoinMap(LEFT)
    val stOnyxPromoInnerJoinDF = stOnyxPromoJoinMap(INNER)
    //writeDF(stOnyxPromoInnerJoinDF,"PROMO_INNER_JOIN")
    /*
    * Add Promo Flag = 0 to Left join - 372
    * AND Add Promo Flag = 1 to  Inner Join 371
    * */
    val stOnyxPromoLeftJoinWithPromoFlagDF = stOnyxPromoLeftJoinDF.withColumn("Promo Flag", lit(0))
    val stOnyxPromoInnerJoinWithPromoFlagDF = stOnyxPromoInnerJoinDF.withColumn("Promo Flag", lit(1))

    /*
    * Union St Onyx joins - Bring back Option C - 373
    * */
    val stOnyxPromoJoinsWithPromoFlagUnionDF = doUnion(doUnion(stOnyxPromoLeftJoinWithPromoFlagDF, stOnyxPromoInnerJoinWithPromoFlagDF).get,stOnyxOptionCWithResellerQtyIRPromoFlagDF).get
    //writeDF(stOnyxPromoJoinsWithPromoFlagUnionDF,"Bringing_Back_Option_C")
    /*
    * Merge inventory Join - 412
    * */
    val stOnyxMergeInventoryJoin = stOnyxSource.joinOperation(ST_ONYX_MERGE_INVENTORY)
    val sttOnyxResellerSKUWEDGroupRenamedDF = sttOnyxResellerSKUWEDGroupDF
    .withColumnRenamed("SKU","Right_SKU")
    .withColumnRenamed("Reseller Cluster","Right_Reseller Cluster")
    .withColumnRenamed("Week_End_Date","Right_Week_End_Date")
    val stOnyxMergeInventoryJoinMap = doJoinAndSelect(stOnyxPromoJoinsWithPromoFlagUnionDF, sttOnyxResellerSKUWEDGroupRenamedDF, stOnyxMergeInventoryJoin)
    val stOnyxMergeInventoryInnerJoinDF = stOnyxMergeInventoryJoinMap(INNER)
    val stOnyxMergeInventoryLeftJoinDF = stOnyxMergeInventoryJoinMap(LEFT)
    //writeDF(stOnyxMergeInventoryInnerJoinDF,"Merge_Inevntory_INNER_Join")
    //writeDF(stOnyxMergeInventoryLeftJoinDF,"stOnyxMergeInventoryLeftJoinDF")
    /*
    * Union Inventory and S-Print Union - 413
    * */
    /* Avik Change : RCode: Version Apr6: Tidy Dataframe had Reseller_Cluster, but other had Reseller Cluster */
    val stOnyxInventorySPrintUnionDF = doUnion(tidyHistAndSKUHierInnerJoinDF.withColumnRenamed("Reseller_Cluster","Reseller Cluster"), doUnion(stOnyxMergeInventoryInnerJoinDF, stOnyxMergeInventoryLeftJoinDF).get).get
    //writeDF(stOnyxInventorySPrintUnionDF,"INVENTORY_AND_S-PRINT")
    /*
    * Calculate Promo Spend, eTailer, Reseller Cluster - 398
    * */
    val stOnyxWithPromoSpendETailerResellerClusterDF = stOnyxInventorySPrintUnionDF
    .withColumn("Promo Spend", col("IR")*col("Non_Big_Deal_Qty"))
    .withColumn("eTailer", when(col("eTailers")==="Y",1).otherwise(0))
    .withColumn("Reseller Cluster", when(col("Reseller Cluster")==="Other", "Other - Option B").otherwise(col("Reseller Cluster")))
    //writeDF(stOnyxWithPromoSpendETailerResellerClusterDF,"BEFORE_REPLACE_NULL_WITH_ZERO")
    /*
    * NULL Impute for numerical column with 0
    * */
    val stOnyxWithPromoSpendETailerResellerAndNullImputeDF = Utils.nullImputationForNumeralColumns(stOnyxWithPromoSpendETailerResellerClusterDF)
    //writeDF(stOnyxWithPromoSpendETailerResellerAndNullImputeDF,"AFTER_REPLACE_NULL_WITH_ZERO")

    /*
    * Select before output - 402
    * */
    val stOnyxFinalFeaturesSelect = stOnyxSource.selectOperation(SELECT_BEFORE_OUTPUT)
    val posqtyOutputCommercialDF = doSelect(stOnyxWithPromoSpendETailerResellerAndNullImputeDF, stOnyxFinalFeaturesSelect.cols, stOnyxFinalFeaturesSelect.isUnknown).get
    .withColumn("L1_Category",col("L1: Use Case")).withColumn("L2_Category",col("L2: Key functionality"))
    .drop("L1: Use Case").drop("L2: Key functionality")
        .withColumn("Spend",col("Promo Spend"))
        .select("Reseller Cluster","VPA","Street Price","SKU","Platform Name","Brand","IPSLES","HPS/OPS","Series","Category","Category Subgroup","Category_1","Category_2","Category_3","Line","PL","Mono/Color","Category Custom","L1_Category","L2_Category","PLC Status","GA date","ES date","Week_End_Date","Season","Season_Ordered","Cal_Month","Cal_Year","Fiscal_Qtr","Fiscal_Year","SKU_Name","Big_Deal_Qty","Non_Big_Deal_Qty","Qty","IR","Promo Flag","Inv_Qty","eTailer","Promo Spend","Spend")
      .withColumn("Street Price", regexp_replace(col("Street Price"),"\\$",""))
      .withColumn("Street Price", regexp_replace(col("Street Price"),",","").cast("double"))
      .withColumn("IR", regexp_replace(col("Street Price"),"\\$",""))
      .withColumn("IR", regexp_replace(col("Street Price"),",","").cast("double"))
    //writeDF(posqtyOutputCommercialDF,"POSQTY_OUTPUT_COMMERCIAL")
    //posqtyOutputCommercialDF.write.option("header","true").mode(SaveMode.Overwrite).csv("/home/avik/Scienaptic/HP/data/April13/spark_outputs/posqty_commercial_output.csv")
    posqtyOutputCommercialDF.write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/Pricing/Outputs/POS_Commercial/posqty_commercial_output_"+currentTS+".csv")

  }
}
