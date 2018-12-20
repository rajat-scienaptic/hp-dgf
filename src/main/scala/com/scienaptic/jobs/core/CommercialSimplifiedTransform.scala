package com.scienaptic.jobs.core

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.bean.UnionOperation.doUnion
import com.scienaptic.jobs.bean.SelectOperation.doSelect
import com.scienaptic.jobs.bean.GroupOperation.doGroup
import com.scienaptic.jobs.bean.JoinAndSelectOperation.doJoinAndSelect
import com.scienaptic.jobs.bean.FilterOperation.doFilter
import com.scienaptic.jobs.bean.SortOperation.doSort
import com.scienaptic.jobs.utility.Utils
import com.scienaptic.jobs.utility.CommercialUtility._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType


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

  val LEFT="left"
  val INNER="inner"
  val RIGHT="right"
  val NUMERAL0="0"
  val NUMERAL1="1"

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

  def execute(executionContext: ExecutionContext): Unit = {
    /* Source Map with all sources' information */
    val sourceMap = executionContext.configuration.sources

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
    val stONYXDF = Utils.loadCSV(executionContext, stOnyxSource.filePath).get.cache()
    val sttONYXDF = Utils.loadCSV(executionContext, sttOnyxSource.filePath).get.cache()
    val tidyHistDF = Utils.loadCSV(executionContext, tidyHistSource.filePath).get.cache()

    /*
    * Initial Select operations
    * */
    val iecInitialSelect = iecSource.selectOperation(INITIAL_SELECT)
    val xsInitialSelect = xsClaimsSource.selectOperation(INITIAL_SELECT)
    val rawCalendarInitalSelect = rawCalendarSource.selectOperation(INITIAL_SELECT)
    val wedInitialSelect = wedSource.selectOperation(INITIAL_SELECT)
    val skuHierarchyInitialSelect = skuHierarchySource.selectOperation(INITIAL_SELECT)
    val commAccountsInitialSelect = commAccountsSource.selectOperation(INITIAL_SELECT)
    val stONYXInitialSelect = stOnyxSource.selectOperation(INITIAL_SELECT)
    val sttONYXInitialSelect = sttOnyxSource.selectOperation(INITIAL_SELECT)
    val tidyHistInitialSelect = tidyHistSource.selectOperation(INITIAL_SELECT)

    val iecSelectDF = doSelect(iecDF, iecInitialSelect.cols,iecInitialSelect.isUnknown).get
    val xsClaimsSelectDF = doSelect(xsClaimsDF, xsInitialSelect.cols,xsInitialSelect.isUnknown).get
    val rawCalendarSelectDF = rawCalendarDF/*doSelect(rawCalendarDF, iecInitialSelect.cols,iecInitialSelect.isUnknown).get*/
      .withColumn("Start Date",unix_timestamp(col("Start Date"),"mm/dd/yy").cast("timestamp"))
      .withColumn("End Date",unix_timestamp(col("End Date"),"mm/dd/yy").cast("timestamp"))
    val wedSelectDF = doSelect(WEDDF, wedInitialSelect.cols,wedInitialSelect.isUnknown).get
    val skuHierarchySelectDF = doSelect(SKUHierDF, skuHierarchyInitialSelect.cols,skuHierarchyInitialSelect.isUnknown).get
    val commAccountsSelectDF = doSelect(commAccountDF, commAccountsInitialSelect.cols,commAccountsInitialSelect.isUnknown).get
    val stONYXSelectDF = doSelect(stONYXDF, stONYXInitialSelect.cols,stONYXInitialSelect.isUnknown).get
    val sttONYXSelectDF = doSelect(sttONYXDF, sttONYXInitialSelect.cols,sttONYXInitialSelect.isUnknown).get
    val tidyHistSelectDF = doSelect(tidyHistDF, tidyHistInitialSelect.cols,tidyHistInitialSelect.isUnknown).get
      .withColumn("Week_End_Date",to_date(unix_timestamp(col("wed"),"MM/dd/yyyy")))
      .drop("wed")

    /*
    * Rename Columns after initial Select Operation
    * */
    val xsClaimsInitialRename = xsClaimsSource.renameOperation(RENAME_INITIAL_SELECT)
    val rawCalendarInitialRename = rawCalendarSource.renameOperation(RENAME_INITIAL_SELECT)
    val tidyHistInitialRename = tidyHistSource.renameOperation(RENAME_INITIAL_SELECT)

    val xsClaimsRenamedDF = Utils.convertListToDFColumnWithRename(xsClaimsInitialRename, xsClaimsSelectDF)
    val wedRenamedDF = Utils.convertListToDFColumnWithRename(rawCalendarInitialRename, wedSelectDF)
    val tidyHistRenamedDF = Utils.convertListToDFColumnWithRename(tidyHistInitialRename, tidyHistSelectDF)

    /*
    * TODO: Convert all dates to date type:
    * .withColumn("Start Date",unix_timestamp(col("Start Date"),"mm/dd/yy").cast("timestamp"))
    */

    /*
    * Filter IEC Claims for Partner Ship Calendar Date  */
    val iecFilterShipDateDF = iecSelectDF.filter(col("Partner Ship Calendar Date") > lit("2016-12-31"))

    /*
    * Union IEC and XS Dataset */
    val iecUnionXSDF = doUnion(iecFilterShipDateDF, xsClaimsRenamedDF).get

    /*
    * Create Features - Base SKU, Temp Date Calc String, Temp Date Calc, Week End Date */
    val iecXSNewFeaturesDF = iecUnionXSDF.withColumn("Temp Date Calc String",extractWeekFromDateUDF(col("Partner Ship Calendar Date").cast("string"), lit("yyyy-MM-dd")))//.withColumn("Temp Date Calc String", weekofyear(to_date(col("Partner Ship Calendar Date"))))
      .withColumn("Temp Date Calc", col("Temp Date Calc String").cast(DoubleType))
      .withColumn("Week End Date",addDaystoDateStringUDF(col("Partner Ship Calendar Date"), col("Temp Date Calc")))
      .withColumn("Base SKU",baseSKUFormulaUDF(col("Base SKU")))

    /*
    * Join Claims with Accounts based on Partner Desc and Account Company */
    val claimsAndAccountJoin = xsClaimsSource.joinOperation(CLAIMS_AND_ACCOUNTS)
    val claimsAndAccountJoinMap = doJoinAndSelect(iecXSNewFeaturesDF, commAccountsSelectDF, claimsAndAccountJoin)
    val claimsAndAccountLeftJoinDF = claimsAndAccountJoinMap(LEFT)
    val claimsAndAccountInnerJoinDF = claimsAndAccountJoinMap(INNER)

    /*
    * Add new variables - Account Consol, Grouping, VPA, Reseller Cluster*/
    val claimsAndAccountLeftJoinNewFeatDF = claimsAndAccountLeftJoinDF
      .withColumn("Account Consol",lit("Others"))
      .withColumn("Grouping",lit("Distributor"))
      .withColumn("VPA", lit("Non-VPA"))
      .withColumn("Reseller Cluster",lit("Other"))

    val claimsAndAccountJoinsUnionDF = doUnion(claimsAndAccountLeftJoinNewFeatDF, claimsAndAccountInnerJoinDF).get

    /*
    * Group on Account Consol, Grouping, Reseller Cluster, VPA, WED, Deal ID, SKU and sum Claim Quantity*/
    val claimsGroupAndAggClaimQuant = xsClaimsSource.groupOperation(GROUP_SKU_ACCOUNT_SUM_CLAIM_QUANT)
    val claimsGroupAndAggClaimQuantDF = doGroup(claimsAndAccountJoinsUnionDF, claimsGroupAndAggClaimQuant).get

    /*
    * Distinct Master Calendar on Promo Code, Name, SKU, Start and End Date */
    val rawMasterCalendarDistinctSelect = rawCalendarSource.selectOperation(DISTINCT_SELECT)
    val rawMasterCalendarDistinctSelectDF = doSelect(rawCalendarSelectDF, rawMasterCalendarDistinctSelect.cols, rawMasterCalendarDistinctSelect.isUnknown).get

    /*
    * Join Claims and Master Calendar on Deal ID, SKU*/
    val claimsAndMasterCalendarJoin = xsClaimsSource.joinOperation(CLAIMS_AND_MASTER_CALENDAR)
    val claimsAndMasterCalendarJoinMap = doJoinAndSelect(claimsGroupAndAggClaimQuantDF, rawMasterCalendarDistinctSelectDF, claimsAndMasterCalendarJoin)
    val claimsAndMasterCalendarLeftJoinDF = claimsAndMasterCalendarJoinMap(LEFT)
    val claimsAndMasterCalendarInnerJoinDF = claimsAndMasterCalendarJoinMap(INNER)

    /*
    * Add new features - Program, Total Amount*/
    val claimsAndCalendarLeftJoinNewFeat = claimsAndMasterCalendarLeftJoinDF
      .withColumn("Program", lit("Big_Deal"))
      .withColumn("Total Amount", col("Claim Partner Unit Rebate")*col("Sum_Claim Quantity"))

    val claimsAndCalendarInnerJoinNewFeat = claimsAndMasterCalendarInnerJoinDF
      .withColumn("Program", when(col("Promo Name")==="Option C","Promo_Option_C").otherwise("Promo"))
      .withColumn("Total Amount",col("Claim Partner Unit Rebate")*col("Sum_Claim Quantity"))

    val claimsAndCalendarJoinsUnionDF = doUnion(claimsAndCalendarLeftJoinNewFeat, claimsAndCalendarInnerJoinNewFeat).get

    /*
    * Join Claim and Week end Date on Week End Date*/
    val claimsAndCalendarJoin = xsClaimsSource.joinOperation(CLAIMS_AND_WED)
    val claimsAndCalendarJoinMap = doJoinAndSelect(claimsAndCalendarJoinsUnionDF, wedRenamedDF, claimsAndCalendarJoin)
    val claimsAndCalendarInnerJoinDF = claimsAndCalendarJoinMap(INNER)

    /*
    * Filter, Summarize, Formula, Select not implemented as going to BROWSE*/

    /*
    * Join Claims and SKU Hierarchy on SKU*/
    val claimsAndSKUHierJoin = xsClaimsSource.joinOperation(CLAIMS_AND_SKU_HIER)
    val claimsAndSKUHierJoinMap = doJoinAndSelect(claimsAndCalendarInnerJoinDF, skuHierarchySelectDF, claimsAndSKUHierJoin)
    val claimsAndSKUHierLeftJoinDF = claimsAndSKUHierJoinMap(LEFT)
    val claimsAndSKUHierInnerJoinDF = claimsAndSKUHierJoinMap(INNER)

    val claimsAndSKUJoinsUnionDF = doUnion(claimsAndSKUHierLeftJoinDF, claimsAndSKUHierInnerJoinDF).get

    /*
    * OUTPUT - claims_consolidated.csv*/
    claimsAndSKUJoinsUnionDF.show()

    /*Group RESELLER_WED_SKU_PROGRAM_AGG_CLAIM_REBATE_QUANTITY*/
    val claimsResellerWEDSKUProgramGroup = xsClaimsSource.groupOperation(RESELLER_WED_SKU_PROGRAM_AGG_CLAIM_REBATE_QUANTITY)
    val claimsResellerWEDSKUProgramGroupDF = doGroup(claimsAndSKUJoinsUnionDF, claimsResellerWEDSKUProgramGroup).get

    /*
    * Filter Program = Big_Deal*/
    val programFilterBigDeal = xsClaimsSource.filterOperation(FILTER_PROGRAM_EQ_BID_DEAL)
    val claimsProgramFilterBigDealDF = doFilter(claimsResellerWEDSKUProgramGroupDF, programFilterBigDeal, programFilterBigDeal.conditionTypes(NUMERAL0)).get
    //385
    val programFilterPromoOptionC = xsClaimsSource.filterOperation(FILTER_PROGRAM_EQ_PROMO_OPTION_C)
    val claimsProgramFilterPromoOptionCDF = doFilter(claimsResellerWEDSKUProgramGroupDF, programFilterPromoOptionC, programFilterPromoOptionC.conditionTypes(NUMERAL0)).get

    val claimsProgramOptionCUniqueDF = claimsProgramFilterPromoOptionCDF.dropDuplicates(List("Reseller Cluster","Week End Date","Consol SKU"))

    /*
    * Group claims on Reseller, SKU, Program, WED and sum Aggregate Claim Quantity - 400
    * */
    val claimsGroupResellerWEDSKUSumClaimQuant = xsClaimsSource.groupOperation(RESELLER_WED_SKU_PROGRAM_AGG_CLAIM_QUANTITY)
    val claimsGroupResellerWEDSKUSumClaimQuantDF = doGroup(claimsProgramFilterBigDealDF, claimsGroupResellerWEDSKUSumClaimQuant).get

    /*
    * Create 'Outside Promo Date' = IF Partner Ship Calendar Date > End Date + 3 days THEN "Y" ELSE "N" ENDIF
    * */
    val claimsWithOutsidePromoDateFeatDF = claimsAndMasterCalendarInnerJoinDF.withColumn("EndDatePlus3", date_add(col("End Date").cast("timestamp"), 3))
      .withColumn("Outside Promo Date", when(col("Partner Ship Calendar Date").cast("timestamp")>col("EndDatePlus3").cast("timestamp"),"Y")
        .otherwise("N"))
      .drop("EndDatePlus3")

    /*
    * Filter where 'Outside Promo Date' = N and Y
    * */
    val claimsOutsidePromoDateFilter = xsClaimsSource.filterOperation(FILTER_OUTSIDE_PROMO_DATE_EQ_N)
    val claimsOutsidePromoDateEqNDF = doFilter(claimsWithOutsidePromoDateFeatDF, claimsOutsidePromoDateFilter, claimsOutsidePromoDateFilter.conditionTypes(NUMERAL0)).get

    val claimsOutsidePromoDateEqYFilter = xsClaimsSource.filterOperation(FILTER_OUTSIDE_PROMO_DATE_EQ_N)
    val claimsOutsidePromoDateEqYDF = doFilter(claimsWithOutsidePromoDateFeatDF, claimsOutsidePromoDateEqYFilter, claimsOutsidePromoDateEqYFilter.conditionTypes(NUMERAL0)).get

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
    val claimsJoinOutsidePromoYAndNWithIncludeDF = claimsJoinOutsidePromoYAndNDF
      .withColumn("Include", when(col("Sum_Sum_Claim Quantity") > lit(0.2)*col("Avg_Sum_Sum_Claim Quantity"),"Y").otherwise("N"))

    /*
    * Filter where Include = Y ,i.e, Sum_Sum_Claim Quantity > 0.2 * Avg_Sum_Sum_Claim Quantity
    * */
    val claimsIncludeEqYFilter = xsClaimsSource.filterOperation(FILTER_INCLUDE_EQ_Y)
    val claimsIncludeEqYDF = doFilter(claimsJoinOutsidePromoYAndNWithIncludeDF, claimsIncludeEqYFilter, claimsIncludeEqYFilter.conditionTypes(NUMERAL0)).get

    /*
    * Group C2B Promo Code and Base SKU and Max Week End Date - 144
    * */
    val claimsPromoCodeSKUGroup = xsClaimsSource.groupOperation(PROMO_CODE_SKU_MAX_WED)
    val claimsPromoCodeSKUDF = doGroup(claimsIncludeEqYDF, claimsPromoCodeSKUGroup).get

    /*
    * Join Claims back with Calendar after aggregations - 146
    * */
    val claimsAndCalendarAfterAggJoin = xsClaimsSource.joinOperation(CLAIMS_AND_MASTER_CALENDAR_AFTER_AGGREGATION)
    val rawCalendarSelectRenamedDF = rawCalendarSelectDF.withColumnRenamed("C2B Promo Code","Right_C2B Promo Code")
    val claimsAndCalendarAfterAggJoinMap = doJoinAndSelect(claimsPromoCodeSKUDF, rawCalendarSelectRenamedDF, claimsAndCalendarAfterAggJoin)
    val claimsAndCalendarAfterAggLeftJoinDF = claimsAndCalendarAfterAggJoinMap(LEFT)
    val claimsAndCalendarAfterAggInnerJoinDF = claimsAndCalendarAfterAggJoinMap(INNER)

    /*
    * Add 'New End Date' using 'End Date'
    * TODO: Test this out!!!!
    * */
    val claimsAndCalendarAfterAggLeftJoinWithNewEndDateDF = claimsAndCalendarAfterAggLeftJoinDF.withColumn("New End Date",col("End Date"))
    val claimsAndCalendarAfterAggInnerJoinWithNewEndDateDF = claimsAndCalendarAfterAggInnerJoinDF
      .withColumn("MaxWeekDiff", datediff(to_date(unix_timestamp(col("Max_Week_End_Date"),"yyyy-mm-dd").cast("timestamp"),"yyyy-mm-dd"), col("End Date")))
      .withColumn("New End Date",when(col("MaxWeekDiff")>14, date_add(col("End Date").cast("timestamp"), 14))
        .when(col("Max_Week_End_Date").cast("timestamp")>col("End Date").cast("timestamp"),col("Max_Week_End_Date"))
        .otherwise(col("End Date")))
      .drop("MaxWeekDiff")

    /*
    * Union Claims and Master Calendar join with 'New End Date' Variable added - 150
    * */
    val claimsAndCalendarAfterAggJoinsUnionDF = doUnion(claimsAndCalendarAfterAggLeftJoinWithNewEndDateDF, claimsAndCalendarAfterAggInnerJoinWithNewEndDateDF).get

    /*
    * Add 'End Date Change' by comparing 'End Date' and 'New End Date' - 155
    * And
    * Drop duplicates on SKU, C2B Promo Code, Start Date, New End Date - 157
    * */
    val claimsWithEndDateChangeDF = claimsAndCalendarAfterAggJoinsUnionDF.withColumn("End Date Change",
      when(col("End Date")===col("New End Date"),"N").otherwise("Y"))
      .dropDuplicates(List("SKU","C2B Promo Code","Start Date","New End Date"))

    /*
    * Cartesian join WED and Claims with 'End Date Change' variable - 158
    * */
    val wedSelectRenamedDF = wedSelectDF.withColumnRenamed("season","Source_season").withColumnRenamed("Season_Ordered","Source_Season_Ordered")
      .select("Week_End_Date","Source_season","Source_Season_Ordered","Cal_Month","Cal_Year","Fiscal_Qtr","Fiscal_Year")
    val claimsCartJoinWEDDF = wedSelectRenamedDF.join(claimsWithEndDateChangeDF)

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

    /*
    * Filter 'Start - Week End' <= -3 AND 'Week End - End' <= 3
    * TODO: Test this!!!!
    * */
    val claimsJoinWEDWithStartnWEDFilteredDF = claimsJoinWEDWithStartEndnOptionTypeVarsDF
      .withColumn("StartMinusEnd", col("Start - Week End"))
      .withColumn("WeekEndMinusEnd", col("Week End - End"))
      .filter("StartMinusEnd <= -3").filter("WeekEndMinusEnd <= 3")
      .drop("StartMinusEnd").drop("WeekEndMinusEnd")

    /*
    * Sort SKU, WED and Rebate Amount - 162
    * */
    val claimsSKUWEDRebateAmountSort = xsClaimsSource.sortOperation(ASC_SKU_WED_DESC_REBATE)
    val claimsSKUWEDRebateAmountSortDF = doSort(claimsJoinWEDWithStartnWEDFilteredDF, claimsSKUWEDRebateAmountSort.ascending, claimsSKUWEDRebateAmountSort.descending).get
      .dropDuplicates(List("SKU","C2B Promo Code","Week_End_Date"))

    /*
    * Filter Option Type != Option C - 368
    * */
    val claimsOptionTypeNotEqOptionCFilter = xsClaimsSource.filterOperation(OPTION_TYPE_NOT_EQUALS_OPTION_C)
    val claimsOptionTypeNotEqOptionCFilterDF = doFilter(claimsSKUWEDRebateAmountSortDF, claimsOptionTypeNotEqOptionCFilter, claimsOptionTypeNotEqOptionCFilter.conditionTypes(NUMERAL0)).get

    /*
    * Group SKU, WED, Promo Code and max aggregate Rebate Amount
    * */
    val claimsSKUWEDPromoCodeGroup = xsClaimsSource.groupOperation(SKU_WED_PROMO_CODE_MAX_REBATE_AMOUNT)
    val claimsSKUWEDPromoCodeGroupDF = doGroup(claimsOptionTypeNotEqOptionCFilterDF, claimsSKUWEDPromoCodeGroup).get

    /*
    * Select C2B Promo Code and Promo Name to compute distinct - 19
    * */
    val masterCalSelectForDistinct = rawCalendarSource.selectOperation(SELECT_FOR_DISTINCT)
    val masterCalSelectForDistinctDF = doSelect(rawCalendarSelectDF, masterCalSelectForDistinct.cols, masterCalSelectForDistinct.isUnknown).get
      .dropDuplicates(List("C2B Promo Code","Promo Name"))

    /* ---------------------------- TIDY HISTORICAL POS ------------------------- */
    /*
    * Filter Channel equals commercial - 418
    * */
    val tidyHistChannelEqCommercialFilter = tidyHistSource.filterOperation(FILTER_CHANNEL_EQ_COMMERCIAL)
    val tidyHistChannelEqCommercialFilterDF = doFilter(tidyHistRenamedDF, tidyHistChannelEqCommercialFilter, tidyHistChannelEqCommercialFilter.conditionTypes(NUMERAL0)).get

    /*
    * Group SKU, Resller, WED, Season, Cal, fiscal year, quarter and sum Qty
    * AND
    * Rename grouping columns - 421
    * */
    val tidyHistSKUResellerWEDSeasonCalFiscalYrGroup = tidyHistSource.groupOperation(SKU_SHORT_RESELLER_WED_SEASON_CAL_FISCAL_SUM_QUANT)
    val tidyHistRenameAfterGroup = tidyHistSource.renameOperation(RENAME_SEASON_CAL_FISCAL_AFTER_GROUP)
    val tidyHistSKUResellerWEDSeasonCalFiscalYrGroupDF = doGroup(tidyHistChannelEqCommercialFilterDF, tidyHistSKUResellerWEDSeasonCalFiscalYrGroup).get
    val tidyHistRenameAfterGroupDF = Utils.convertListToDFColumnWithRename(tidyHistRenameAfterGroup, tidyHistSKUResellerWEDSeasonCalFiscalYrGroupDF)

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

    /*
    * Join Tidy Hist pos with SKU Hierarchy on SKU - 420
    * */
    val tidyHistAndSKUHierJoin = tidyHistSource.joinOperation(TIDY_HIST_AND_SKU_HIER)
    val tidyHistAndSKUHierInnerJoinDF = doJoinAndSelect(tidyHistWithDealQtyETailerIRPromoFlagDF, skuHierarchySelectDF, tidyHistAndSKUHierJoin)(INNER)

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

    val sttOnyxAndAccountJoinsUnionDF = doUnion(sttOnyxAndAccountsLeftJoinDF, sttOnyxAndAccountsInnerJoinDF).get

    /*
    * Merge Consol SKU using SKU Hierarchy - 409
    * */
    val sttOnyxAndSkuHierJoin = sttOnyxSource.joinOperation(STT_ONYX_AND_SKU_HIERARCHY)
    val skuHierarchySelectRenamedForSTTDF = skuHierarchySelectDF.withColumnRenamed("SKU","Join_SKU")
      .withColumnRenamed("Consol SKU","SKU")
    val sttOnyxAndSkuHierInnerJoinDF = doJoinAndSelect(sttOnyxAndAccountJoinsUnionDF, skuHierarchySelectRenamedForSTTDF, sttOnyxAndSkuHierJoin)(INNER)

    /*
    * Merge WED using WED - 410
    * */
    val sttOnyxAndWEDJoin = sttOnyxSource.joinOperation(STT_ONYX_AND_WED_MERGE_WED)
    val sttOnyxAndWEDInnerJoinDF = doJoinAndSelect(sttOnyxAndSkuHierInnerJoinDF, wedSelectDF, sttOnyxAndWEDJoin)(INNER)

    /*
    * Group Reseller Cluster, SKU, Week_End_Date - 411 AND add 7 days to Week_End_Date - 414
    * */
    val sttOnyxResellerSKUWEDGroup = sttOnyxSource.groupOperation(RESELLER_SKU_WED_SUM_AGG_INVENTORY_TOTAL)
    val sttOnyxResellerSKUWEDGroupDF = doGroup(sttOnyxAndWEDInnerJoinDF, sttOnyxResellerSKUWEDGroup).get
      .withColumn("Week_End_Date",date_add(col("Week_End_Date").cast("timestamp"), 7))

    /* ---------------------------- ST ----------------------------- */
    /*
    * Join ST ONYX and Aux Accounts - 345
    * */
    val stOnyxAndAccountsJoin = stOnyxSource.joinOperation(ST_ONYX_AND_AUX_ACCOUNTS)
    val commAccountsSelectRenamedDF = commAccountsSelectDF
      .withColumnRenamed("Account Company","Right_Account Company")
    val stOnyxAndAccountsJoinMap = doJoinAndSelect(tidyHistAndSKUHierInnerJoinDF, commAccountsSelectRenamedDF, stOnyxAndAccountsJoin)
    val stOnyxAndAccountsLeftJoinDF = stOnyxAndAccountsJoinMap(LEFT)
    val stOnyxAndAccountsInnerJoinDF = stOnyxAndAccountsJoin(INNER)

    /*
    * Add Features - 'Account Consol', 'Grouping', 'VPA', 'Reseller Cluster'
    * */
    val stOnyxAndAccountWithFeaturesDF = stOnyxAndAccountsLeftJoinDF
      .withColumn("Account Consol",lit("Others"))
      .withColumn("Grouping", lit("Distributor"))
      .withColumn("VPA", lit("Non-VPA"))
      .withColumn("Reseller Cluster", lit("Other"))

    /*
    * Union ST joins
    * */
    val stOnyxAndAccountJoinsUnion = doUnion(stOnyxAndAccountWithFeaturesDF, stOnyxAndAccountsInnerJoinDF).get

    /*
    * Join ST ONYX And SKU Hierarchy on Product Base Id and SKU - 348
    * */
    val stOnyxAndSKUHierJoin = stOnyxSource.joinOperation(ST_ONYX_AND_SKU_HIERARCHY)
    val skuHierarchySelectRenamedDF = skuHierarchySelectDF.withColumnRenamed("SKU","Right_SKU")
      .withColumnRenamed("Consol SKU","SKU")
    val stOnyxAndSKUHierJoinMap = doJoinAndSelect(stOnyxAndAccountJoinsUnion, skuHierarchySelectRenamedDF, stOnyxAndSKUHierJoin)
    val stOnyxAndSKUHierLeftJoinDF = stOnyxAndSKUHierJoinMap(LEFT)
    val stOnyxAndSKUHierInnerJoinDF = stOnyxAndSKUHierJoinMap(INNER)

    /*
    * Group Left join on Product Base Id, Product Base Desc and sum Aggregate Sell Thru Qty, Sell-to Qty
    * And
    * Sort on Sell Thru and Sell to Quantities
    * */
    val stOnyxProductBaseGroup = stOnyxSource.groupOperation(PRODUCT_BASE_SUM_AGG_SELL_THRU_SELL_TO_QTY)
    val stOnyxProductBaseGroupDF = doGroup(stOnyxAndSKUHierLeftJoinDF, stOnyxProductBaseGroup).get
    val stOnyxSellThruAndToSort = stOnyxSource.sortOperation(DESC_SELL_THRU_SELL_TO)
    val stOnyxSellThruAndToSortDF = doSort(stOnyxProductBaseGroupDF, stOnyxSellThruAndToSort.ascending, stOnyxSellThruAndToSort.descending).get
    /* Used only for Browse */

    /*
    * Group Week on St ONYX and SKU Join - 350
    * */
    val stOnyxWeekJoin = stOnyxSource.joinOperation(ST_ONYX_AND_WED)
    val stOnyxWeekInnerJoinDF = doJoinAndSelect(stOnyxAndSKUHierInnerJoinDF, wedSelectDF, stOnyxWeekJoin)(INNER)

    /*
    * 351 - Group Deal ID 1, eTailers,
    * */
    val stOnyxGroup = stOnyxSource.groupOperation(DEAL_ETAILERS_ACCOUNT_SEASON_CAL_SKU_FISCAL_SUM_AGG_SELL_THRU_TO_QTY)
    val stOnyxGroupDF = doGroup(stOnyxWeekInnerJoinDF, stOnyxGroup).get

    /*
    * Join ST and Promo ID list - 352
    * */





  }
}
