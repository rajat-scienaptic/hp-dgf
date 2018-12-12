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
import org.apache.spark.sql.types._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


object CommercialTransform {

  val SELECT01="select01" 
  val SELECT02="select02"
  val SELECT03="select03" 
  val SELECT04="select04" 
  val SELECT05="select05" 
  val SELECT06="select06" 
 
  val FILTER01="filter01" 
  val FILTER02="filter02" 
  val FILTER03="filter03" 
  val FILTER04="filter04" 
  val FILTER05="filter05" 
  val FILTER06="filter06" 
 
  val JOIN01="join01" 
  val JOIN02="join02" 
  val JOIN03="join03" 
  val JOIN04="join04" 
  val JOIN05="join05" 
  val JOIN06="join06" 
  val JOIN07="join07" 
 
  val SORT01="sort01" 
  val SORT02="sort02" 
  val SORT03="sort03" 
  val SORT04="sort04" 
 
  val NUMERAL0=0 
  val NUMERAL1=1 
 
  val GROUP01="group01" 
  val GROUP02="group02" 
  val GROUP03="group03" 
  val GROUP04="group04" 
  val GROUP05="group05"
  val GROUP06="group06"
  val GROUP07="group07"

  val RENAME01="rename01"
  val RENAME02="rename02"

  val IEC_SOURCE="IEC_CLAIMS" 
  val XS_CLAIMS_SOURCE="XS_CLAIMS" 
  val RAW_CALENDAR_SOURCE="RAW_MASTER_CALENDAR"
  val AUX_WED_SOURCE="AUX_WEEK_END_DATE" 
  val AUX_SKU_HIERARCHY_SOURCE="AUX_SKU_HIERARCHY"
  val ST_ORCA_SOURCE="ST_ORCA" 
  val STT_ORCA_SOURCE="STT_ORCA"
  val AUX_COMM_RESELLER_SOURCE="AUX_COMM_RESELLER_OPTIONS"
  val AUX_ETAILER_SOURCE="AUX_ETAILER"
  val CI_ORCA_SOURCE="CI_ORCA"
  val HISTORICAL_POS_SOURCE="TIDY_HPS_2016_17_HISTORICAL_POS"
  val ARCHIVE_POS_QTY_COMMERCIAL_SOURCE="ARCHIVE_POS_QTY_COMMERCIAL"


  def execute(executionContext: ExecutionContext): Unit = {
    /* Source Map with all sources' information */
    val sourceMap = executionContext.configuration.sources

    /* Map with all operations for IEC_Claims source operations */
    val iecSource = sourceMap(IEC_SOURCE)
    val xsClaimsSource = sourceMap(XS_CLAIMS_SOURCE)
    val rawCalendarSource = sourceMap(RAW_CALENDAR_SOURCE)
    val auxWEDSource = sourceMap(AUX_WED_SOURCE)
    val auxSkuHierarchySource = sourceMap(AUX_SKU_HIERARCHY_SOURCE)
    val stORCASource = sourceMap(ST_ORCA_SOURCE)
    val sttORCASource = sourceMap(STT_ORCA_SOURCE)
    val auxCommResellerSource = sourceMap(AUX_COMM_RESELLER_SOURCE)
    val auxEtailerSource = sourceMap(AUX_ETAILER_SOURCE)
    val ciORCASource = sourceMap(CI_ORCA_SOURCE)
    val histPOSSource = sourceMap(HISTORICAL_POS_SOURCE)
    val archivePOSSource = sourceMap(ARCHIVE_POS_QTY_COMMERCIAL_SOURCE)

    /* Load all sources */
    val iecDF = Utils.loadCSV(executionContext, iecSource.filePath).get.cache()
    val xsClaimsDF = Utils.loadCSV(executionContext, xsClaimsSource.filePath).get.cache()
    val rawCalendarDF = Utils.loadCSV(executionContext, rawCalendarSource.filePath).get.cache()
    val auxWEDDF = Utils.loadCSV(executionContext, auxWEDSource.filePath).get.cache()
    val auxSKUHierDF = Utils.loadCSV(executionContext, auxSkuHierarchySource.filePath).get.cache()
    val stORCADF = Utils.loadCSV(executionContext, stORCASource.filePath).get.cache()
    val sttORCADF = Utils.loadCSV(executionContext, sttORCASource.filePath).get.cache()
    val auxCommResellerDF = Utils.loadCSV(executionContext, auxCommResellerSource.filePath).get.cache()
    val auxEtailerDF = Utils.loadCSV(executionContext, auxEtailerSource.filePath).get.cache()
    val ciORCADF = Utils.loadCSV(executionContext, ciORCASource.filePath).get.cache()
    val archivePOSDF = Utils.loadCSV(executionContext, archivePOSSource.filePath).get.cache()

    //141
    val auxWEDSelect01 = auxWEDSource.selectOperation(SELECT01)
    var auxWEDSelectDF = doSelect(auxWEDDF, auxWEDSelect01.cols, auxWEDSelect01.isUnknown).get
    val auxWEDRename022 = auxWEDSource.renameOperation(RENAME02)
    auxWEDSelectDF = Utils.convertListToDFColumnWithRename(auxWEDRename022, auxWEDSelectDF)
        .withColumnRenamed("Season","season")

    //233 - Select
    val iecSelect01 = iecSource.selectOperation(SELECT01)
    val iecSelectDF = doSelect(iecDF, iecSelect01.cols, iecSelect01.isUnknown).get

    //286 - Filter
    val iecClaimFilter01 = iecSource.filterOperation(FILTER01)
    //val iecFiltered01DF = doFilter(iecSelectDF, iecClaimFilter01, iecClaimFilter01.conditionTypes(NUMERAL0))
    val iecFiltered01DF = iecSelectDF.filter(col("Partner Ship Calendar Date") > lit("2018-10-31"))

    //285 - Select
    val xsClaimsSelect01 = xsClaimsSource.selectOperation(SELECT01)
    val xsClaimsSelect01DF = doSelect(xsClaimsDF, xsClaimsSelect01.cols,xsClaimsSelect01.isUnknown).get
      .withColumnRenamed("Claim Partner Name","Partner Desc").withColumnRenamed("Manufacturing Product Identifier","Product ID")
      .withColumnRenamed("Claim Partner Unit Rebate USD","Claim Partner Unit Rebate")
    /*TODO: Rename 3 columns*/

    //287 - Union
    val xsClaimsUnionDF = doUnion(iecFiltered01DF, xsClaimsSelect01DF).get

    //126 - Formula
    val baseSKUproductDF = xsClaimsUnionDF.withColumn("Base SKU",createBaseSKUFromProductIDUDF(col("Product ID")))
    val tempDateCalDF = baseSKUproductDF.withColumn("Temp Date Calc String", weekofyear(to_date(col("Partner Ship Calendar Date"))))//.withColumn("Temp Date Calc String",extractWeekFromDateUDF(col("Partner Ship Calendar Date"), col("week")))
      .withColumn("Temp Date Calc", unix_timestamp(col("Temp Date Calc String")))
      //.withColumn("Temp Date Calc", getEpochNumberFromDateString(col("Temp Date Calc String")))
    val weekEndDateDF = tempDateCalDF.withColumn("Week End Date",addDaystoDateStringUDF(col("Partner Ship Calendar Date"), col("Temp Date Calc")))
    val xsClaimsBaseSKUDF = weekEndDateDF.withColumn("Base SKU",baseSKUFormulaUDF(col("Base SKU")))

    //231 - Not implemented as used for browsing
    //127
    val xsClaimsGroup01 = xsClaimsSource.groupOperation(GROUP01)
    val xsClaimsGroupedClaimQuanAggDF = doGroup(xsClaimsBaseSKUDF,xsClaimsGroup01).get

    //172
    val xsClaimsGroup02 = xsClaimsSource.groupOperation(GROUP02)
    val xsClaimsGroupSumClaimQuanAggDF = doGroup(xsClaimsGroupedClaimQuanAggDF, xsClaimsGroup02).get

    //120
    val rawCalendarSelect01= rawCalendarSource.selectOperation(SELECT01)
    val rawCalendarSelectDF = doSelect(rawCalendarDF, rawCalendarSelect01.cols, rawCalendarSelect01.isUnknown).get
    //rawCalendarSelectDF = rawCalendarSelectDF.withColumn("dummy",lit(1))

    //124
    val rawCalendarGroup01 = rawCalendarSource.groupOperation(GROUP01)
    /*var rawCalendarGroupDF = doGroup(rawCalendarSelectDF, rawCalendarGroup01).get
    rawCalendarGroupDF = rawCalendarGroupDF.drop("dummy")*/
    var rawCalendarGroupDF = rawCalendarSelectDF.dropDuplicates(rawCalendarGroup01.cols)
    rawCalendarGroupDF = rawCalendarGroupDF.select(Utils.convertListToDFColumn(rawCalendarGroup01.selectCols, rawCalendarGroupDF): _*)

    //243 - Join
    val iecXSJoin01 = xsClaimsSource.joinOperation(JOIN01)
    var iecXSJoinMap = doJoinAndSelect(xsClaimsGroupedClaimQuanAggDF, rawCalendarGroupDF, iecXSJoin01)
    val iecXSLeftJoinDF = iecXSJoinMap("left")
    val iecXSInnerJoinDF = iecXSJoinMap("inner")

    //245 - Formula
    val iecXSLeftJoinPromoAmountDF = iecXSLeftJoinDF.withColumn("Promo",lit("N"))
      .withColumn("Total Amount",col("Claim Partner Unit Rebate")*col("Sum_Claim Quantity"))
    //244 - Formula
    val iecXSInnerJoinPromoAmountDF = iecXSInnerJoinDF.withColumn("Promo",lit("Y"))
      .withColumn("Total Amount",col("Claim Partner Unit Rebate")*col("Sum_Claim Quantity"))
    //246 - Union
    val iecXSUnionDF = doUnion(iecXSLeftJoinPromoAmountDF, iecXSInnerJoinPromoAmountDF).get

    //252 - Join
    val iecXsClaimsJoin02 = xsClaimsSource.joinOperation(JOIN02)
    val xsInnerJoinAuxWEDMap = doJoinAndSelect(iecXSUnionDF, auxWEDSelectDF, iecXsClaimsJoin02)
    val xsInnerJoinAuxWEDDF = xsInnerJoinAuxWEDMap("inner")
    //249 - 253 : Not implemented

    //254 - join
    val xsAuxSkuHierJoin03 = xsClaimsSource.joinOperation(JOIN03)
    val xsAuxSkuJoinMap = doJoinAndSelect(xsInnerJoinAuxWEDDF, auxSKUHierDF, xsAuxSkuHierJoin03)
    val xsAuxSkuLeftJoinDF = xsAuxSkuJoinMap("left")
    val xsAuxSkuInnerJoinDF = xsAuxSkuJoinMap("inner")

    //256 - Union
    val xsAuxSKUHierUnionDF = doUnion(xsAuxSkuLeftJoinDF, xsAuxSkuInnerJoinDF).get

    //291
    xsAuxSKUHierUnionDF.show()
    //TODO: create WriteCSV utility and make changes to Source bean to accept 'outFilePath' attribute, name: claims_consolidated.csv

    //121 - Join
    val rawCalendarJoin01 = rawCalendarSource.joinOperation(JOIN01)
    val rawXSJoinMap = doJoinAndSelect(xsClaimsGroupedClaimQuanAggDF, rawCalendarGroupDF, rawCalendarJoin01)
    //val rawXSLeftJoin01 = rawXSJoinMap("left")   //Used for Dump!
    val rawXSInnerJoin01DF = rawXSJoinMap("inner")

    //129
    val rawXSJoinChkOutsidePromoDF = rawXSInnerJoin01DF/*.withColumn("Outside Promo Date", checkOutsidePromoDateUDF(col("Partner Ship Calendar Date"),col("End Date")))*/
      .withColumn("EndDatePlus3", date_add(col("End Date").cast("timestamp"), 3))
      .withColumn("Outside Promo Date", when(col("Partner Ship Calendar Date").cast("timestamp")>col("EndDatePlus3").cast("timestamp"),"Y")
        .otherwise("N"))
    /*CHECK THIS PART*/
    //131
    val rawCalendarFilter01 = rawCalendarSource.filterOperation(FILTER01)
    val rawXSJoinOutsidePromoTrueDF = doFilter(rawXSJoinChkOutsidePromoDF, rawCalendarFilter01, rawCalendarFilter01.conditionTypes(NUMERAL0)).get
    val rawCalendarFilter02 = rawCalendarSource.filterOperation(FILTER02)
    val rawXSJoinOutsidePromoFalseDF = doFilter(rawXSJoinChkOutsidePromoDF, rawCalendarFilter02, rawCalendarFilter02.conditionTypes(NUMERAL0)).get

    //132
    val rawCalendarGroup06 = rawCalendarSource.groupOperation(GROUP06)
    val rawCalendarGroup06DF = doGroup(rawXSJoinOutsidePromoTrueDF, rawCalendarGroup06).get

    //134
    val rawCalendarGroup07 = rawCalendarSource.groupOperation(GROUP07)
    val rawCalendarGroup07DF = doGroup(rawXSJoinOutsidePromoFalseDF, rawCalendarGroup07).get

    //133
    val rawCalendarGroup02 = rawCalendarSource.groupOperation(GROUP02)
    val rawCalendarGroup02DF = doGroup(rawCalendarGroup06DF, rawCalendarGroup02).get

    //135 - Join
    val rawCalendarJoin02 = rawCalendarSource.joinOperation(JOIN02)
    val rawCalendarJoin02Map = doJoinAndSelect(rawCalendarGroup07DF, rawCalendarGroup02DF, rawCalendarJoin02)
    val rawCalendarJoin02DF = rawCalendarJoin02Map("inner")

    //136 - Formula
    val rawCalendarIncludeVarDF = rawCalendarJoin02DF.withColumn("Include",
      when(col("Sum_Sum_Claim Quantity") > lit(0.2)*col("Avg_Sum_Sum_Claim Quantity"),"Y")
      .otherwise("N"))

    //138 - Filter
    val rawCalendarFilter03 = rawCalendarSource.filterOperation(FILTER03)
    val rawCalendarFilterIncludeDF = doFilter(rawCalendarIncludeVarDF, rawCalendarFilter03, rawCalendarFilter03.conditionTypes(NUMERAL0)).get

    //144 - Summarize
    val rawCalendarGroup03 = rawCalendarSource.groupOperation(GROUP03)
    val rawCalendarPromonSKUGroupDF = doGroup(rawCalendarFilterIncludeDF,rawCalendarGroup03).get

    //146 - Join
    val rawCalendarPromonSKUGroupRenamedDF = rawCalendarPromonSKUGroupDF.withColumnRenamed("C2B Promo Code","Right_C2B Promo Code")
    val rawCalendarJoin03 = rawCalendarSource.joinOperation(JOIN03)
    val rawCalendarJoin03Map = doJoinAndSelect(rawCalendarSelectDF,rawCalendarPromonSKUGroupRenamedDF,rawCalendarJoin03)
    val rawCalendarLeftJoin03DF = rawCalendarJoin03Map("left")
    val rawCalendarInnerJoin03DF = rawCalendarJoin03Map("inner")

    //151 - Formula
    val rawCalendarLftJNewEndDateDF = rawCalendarLeftJoin03DF.withColumn("New End Date",col("End Date"))

    //148 - Formula
    val rawCalendarInnJNewEndDateDF = rawCalendarInnerJoin03DF/*.withColumn("New End Date", newEndDateFromMaxWED(col("Max_Week_End_Date"), col("End Date")))*/
      .withColumn("MaxWeekDiff", datediff(col("Max_Week_End_Date").cast("timestamp"), col("End Date").cast("timestamp")))
      .withColumn("New End Date",when(col("MaxWeekDiff")>14, date_add(col("End Date").cast("timestamp"), 14))
        .when(col("Max_Week_End_Date").cast("timestamp")>col("End Date").cast("timestamp"),col("Max_Week_End_Date"))
        .otherwise(col("End Date"))).drop("MaxWeekDiff")

    //150 - Union
    val rawCalendarSelect03 = rawCalendarSource.selectOperation(SELECT03) //rawCalendarInnJNewEndDateDF
    val rawCalendarSelect04 = rawCalendarSource.selectOperation(SELECT04) //rawCalendarLftJNewEndDateDF
    val rawCalendarInnJNewEndDateSelectedDF = doSelect(rawCalendarInnJNewEndDateDF, rawCalendarSelect03.cols, rawCalendarSelect03.isUnknown).get
    val rawCalendarLftJNewEndDateSelectedDF = doSelect(rawCalendarLftJNewEndDateDF, rawCalendarSelect04.cols, rawCalendarSelect04.isUnknown).get
    var rawCalendarEndDateUnionDF = doUnion(rawCalendarInnJNewEndDateSelectedDF,rawCalendarLftJNewEndDateSelectedDF).get
    //rawCalendarEndDateUnionDF = doSelect(rawCalendarEndDateUnionDF, rawCalendarSelect03.cols, rawCalendarSelect03.isUnknown)

    //155 - Formula
    val rawCalendarEndDateChangeDF = rawCalendarEndDateUnionDF.withColumn("End Date Change",
      when(col("End Date")===col("New End Date"),"N")
          .otherwise("Y"))

    //157 - Unique
    val rawCalendarEndDateChangeUnionDF = rawCalendarEndDateChangeDF.dropDuplicates(List("SKU","C2B Promo Code","Start Date","New End Date"))

    //158 - Append (Cartesian join)
    val rawCalendarnAuxWEDAppendDF = rawCalendarEndDateChangeUnionDF.join(auxWEDSelectDF.drop("fy_week"))

    //159 - Formula
    val rawCalStartsubWEDDF = rawCalendarnAuxWEDAppendDF/*.withColumn("Start - Week End", dateTimeDiff(col("Start Date"),col("Week.End.Date")))
      .withColumn("Week End - End",dateTimeDiff(col("Week.End.Date"),col("New End Date")))*/
      .withColumn("Start - Week End", datediff(col("Start Date").cast("timestamp"),col("Week_End_Date").cast("timestamp")))
      .withColumn("Week End - End", datediff(col("Week_End_Date").cast("timestamp"),col("New End Date").cast("timestamp")))
      .withColumn("Option Type",
        when(col("Promo Name").contains("Option B"),"Option B")
        .when(col("Promo Name").contains("Option C"),"Option C")
        .otherwise("All"))

    //160 - Filter [Start - Week End]<=-3 AND [Week End - End]<=3
    //CHECK THIS
    val rawCalendarEndDateFilteredDF = rawCalStartsubWEDDF.withColumn("StartMinusEnd", col("Start - Week End"))
      .withColumn("WeekEndMinusEnd", col("Week End - End"))
      .filter("StartMinusEnd <= -3").filter("WeekEndMinusEnd <= 3")

    //161 - Select
    val rawCalendarSelect02 = rawCalendarSource.selectOperation(SELECT02)
    val rawCalendarEndDateFilterSelectDF = doSelect(rawCalendarEndDateFilteredDF, rawCalendarSelect02.cols, rawCalendarSelect02.isUnknown).get
    rawCalendarEndDateFilterSelectDF.cache()
    /*  -------------  USED TWICE ---------  */

    //162 - Sort
    val rawCalendarSort01 = rawCalendarSource.sortOperation(SORT01)
    val rawCalendarSelectSortDF = doSort(rawCalendarEndDateFilterSelectDF, rawCalendarSort01.ascending, rawCalendarSort01.descending).get
    /*  ------------- USED IN 2nd SECTION --------------*/

    //163 - Unique
    val rawCalendarSortDistinctDF = rawCalendarSelectSortDF.dropDuplicates(List("SKU","C2B Promo Code","Week_End_Date"))
    //DONE TILL HERE
    //196 - Join
    val rawCalendarJoin04 = rawCalendarSource.joinOperation(JOIN04)
    val rawCalendarDistinctLeftJoinDF = doJoinAndSelect(xsClaimsGroupSumClaimQuanAggDF, rawCalendarSortDistinctDF, rawCalendarJoin04)("left")
    /*  ------------- USED IN 2nd SECTION --------------*/

    //19 - Summarize (Group on C2B Promo Code)
    var rawCalendarEndDateSelectGroupDF = rawCalendarEndDateFilterSelectDF//.withColumn("dummy",lit(1))
    val rawCalendarGroup04 = rawCalendarSource.groupOperation(GROUP04)
    rawCalendarEndDateSelectGroupDF = rawCalendarEndDateSelectGroupDF.select(Utils.convertListToDFColumn(rawCalendarGroup04.cols, rawCalendarEndDateSelectGroupDF):_*).dropDuplicates(rawCalendarGroup04.cols) //doGroup(rawCalendarEndDateSelectGroupDF, rawCalendarGroup04).get.drop("dummy").cache()
    /*  ------------- USED IN 2nd SECTION -------- */

    //85 - Summarize
    val rawCalendarGroup05 = rawCalendarSource.groupOperation(GROUP05)
    var rawCalendarSortGroupDF = doGroup(rawCalendarSelectSortDF, rawCalendarGroup05).get
    /*val wind = Window.partitionBy(col("C2B Promo Code"))
    rawCalendarSortGroupDF = rawCalendarSortGroupDF.withColumn("rn",row_number.over(wind))
      .where(col("rn") === 1)
      .drop("rn")*/
    /*  ------------- USED IN 2nd SECTION --------------*/


    //13 - Select
    val stORCASelect01 = stORCASource.selectOperation(SELECT01)
    val stORCASelectDF = doSelect(stORCADF, stORCASelect01.cols, stORCASelect01.isUnknown).get

    //3 - Filter
    val stORCAProductIDNotNullDF = stORCASelectDF.filter(col("Product Base ID").isNotNull)

    //16 - Formula
    var stORCAFormulaDF = stORCAProductIDNotNullDF.withColumn("Product Base ID",
      when(col("Product Base ID")==="M9L74A", "M9L75A")
      .when((col("Product Base ID")==="J9V91A") || (col("Product Base ID")==="J9V92A"),"J9V90A")
      .otherwise(col("Product Base ID")))

    stORCAFormulaDF = stORCAFormulaDF.withColumn("Product Base ID",
      when((col("Product Base ID")==="X7N08A")||(col("Product Base ID")==="Z9L26A")||(col("Product Base ID")==="Z9L25A")||(col("Product Base ID")==="Z9L25A")||(col("Product Base ID")==="Z3Z94A"),"X7N07A")
        .when(col("Product Base ID")==="B3Q17A","B3Q11A")
        .otherwise(col("Product Base ID")))

    stORCAFormulaDF = stORCAFormulaDF.withColumn("Qty",col("Dist Qty")+col("POS Qty"))
      .withColumn("Big Deal",
        when((col("Big Deal Nbr")==="?") || (col("Big Deal Nbr").contains("B01OP")),0)
        .otherwise(1))
      .withColumn("Week End Date", dateTimeParseWithLastCharacters(col("Week Desc"),lit("%b %d, %Y"),lit(12)))

    //20 - join
    val stORCAJoin01 = stORCASource.joinOperation(JOIN01)
    val stORCAJoin01Map = doJoinAndSelect(stORCAFormulaDF, rawCalendarEndDateSelectGroupDF, stORCAJoin01)
    val stORCAInnerJoinDF = stORCAJoin01Map("inner")
    val stORCALeftJoinDF = stORCAJoin01Map("left")

    //21 - Formula
    val stORCAInnerBigDealDF = stORCAInnerJoinDF.withColumn("Big Deal",lit(0))

    //22 - Union
    val stORCAUnionDF = doUnion(stORCALeftJoinDF, stORCAInnerBigDealDF).get

    //12 - Select
    val sttORCASelect01 = sttORCASource.selectOperation(SELECT01)
    val sttORCASelectDF = doSelect(sttORCADF, sttORCASelect01.cols, sttORCASelect01.isUnknown).get

    //6 - Filter
    val sttORCAFilterProdctIDNotNullDF = sttORCASelectDF.filter(col("Product Base ID").isNotNull)

    //17 - Formula
    var sttORCAFormulaDF = sttORCAFilterProdctIDNotNullDF.withColumn("Product Base ID",
      when(col("Product Base ID")==="M9L74A","M9L75A")
        .when((col("Product Base ID")==="J9V91A") || (col("Product Base ID")==="J9V92A"),"J9V90A")
        .otherwise(col("Product Base ID")))

    sttORCAFormulaDF = sttORCAFormulaDF.withColumn("Product Base ID",
      when((col("Product Base ID")==="X7N08A")||(col("Product Base ID")==="Z9L26A")||(col("Product Base ID")==="Z9L25A")||(col("Product Base ID")==="Z9L25A")||(col("Product Base ID")==="Z3Z93A")||(col("Product Base ID")==="Z3Z94A"),"X7N07A")
        .when(col("Product Base ID")==="B3Q17A","B3Q11A")
        .otherwise(col("Product Base ID")))

    sttORCAFormulaDF = sttORCAFormulaDF.withColumn("Qty", col("Dist Qty")+col("POS Qty"))
      .withColumn("Big Deal",
        when((col("Big Deal Nbr")==="?") || (col("Big Deal Nbr").contains("B01OP")),0)
          .otherwise(1))
      .withColumn("Week End Date", dateTimeParseWithLastCharacters(col("Week Desc"),lit("%b %d, %Y"),lit(12)))

    //25 - Join
    val sttORCAJoin01 = sttORCASource.joinOperation(JOIN01)
    val sttORCAJoin01Map = doJoinAndSelect(sttORCAFormulaDF, rawCalendarEndDateSelectGroupDF, sttORCAJoin01)
    val sttORCAInnerJoinDF = sttORCAJoin01Map("inner")
    val sttORCALeftJoinDF = sttORCAJoin01Map("left")

    //24 - Formula
    val sttORCAInnerBigDealDF = sttORCAInnerJoinDF.withColumn("Big Deal",lit(0))

    //23 - Union
    val sttORCAUnionDF = doUnion(sttORCAInnerBigDealDF, sttORCALeftJoinDF).get

    //26 - Filter
    val stORCAFilter01 = stORCASource.filterOperation(FILTER01)
    val stORCAFilterBigDealDF = doFilter(stORCAUnionDF, stORCAFilter01, stORCAFilter01.conditionTypes(NUMERAL0)).get

    //167 - Summarize (ST)
    val stORCAGroup01 = stORCASource.groupOperation(GROUP01)
    val stORCAGroupDealnAccountIDDF = doGroup(stORCAUnionDF, stORCAGroup01).get

    //168 - Summarize (STT)
    val sttORCAGroup02 = sttORCASource.groupOperation(GROUP02)
    val sttORCAGroupDealnAccountIDDF = doGroup(sttORCAUnionDF, sttORCAGroup02).get

    //28 - Summarize
    val stORCAGroup02 = stORCASource.groupOperation(GROUP02)
    val stORCAGroupIDnAccountnWEDDF = doGroup(stORCAUnionDF, stORCAGroup02).get

    //27 - Summarize
    val stORCAGroup03 = stORCASource.groupOperation(GROUP03)
    val stORCAGroupDealnAccountnIDDF = doGroup(stORCAFilterBigDealDF, stORCAGroup03).get

    //29 - Join
    val stORCAJoin02 = stORCASource.joinOperation(JOIN02)
    val stORCAGroupIDnAccountnWEDWithRenamedDF = stORCAGroupIDnAccountnWEDDF.withColumnRenamed("Product Base ID","Righ_Product Base ID")
      .withColumnRenamed("Product Base Desc","Right_Product Base Desc")
      .withColumnRenamed("Account Company","Right_Account Company")
      .withColumnRenamed("Week End Date","Right_Week End Date")
      .withColumnRenamed("Week","Right_Week")
    val stORCAJoin02Map = doJoinAndSelect(stORCAGroupDealnAccountnIDDF.withColumnRenamed("Qty","Big Deal Qty"), stORCAGroupIDnAccountnWEDWithRenamedDF, stORCAJoin02)
    val stORCAInnerJoin02DF = stORCAJoin02Map("inner")
    val stORCARightJoin02DF = stORCAJoin02Map("right")

    //169 - Union
    val stSttUnionDF = doUnion(stORCAGroupDealnAccountIDDF, sttORCAGroupDealnAccountIDDF).get

    //170 - Unique
    val stSttUnionDistinctDF = stSttUnionDF.dropDuplicates(List("Big Deal Nbr","Account Company","Product Base ID"))

    //171 - Join
    val stORCAJoin03 = stORCASource.joinOperation(JOIN03)
    val stORCAJoin03Map = doJoinAndSelect(stSttUnionDistinctDF, rawCalendarDistinctLeftJoinDF, stORCAJoin03)
    val stORCAJoin03DF = stORCAJoin03Map("right")

    //183 - Summarize
    val stORCAGroup04 = stORCASource.groupOperation(GROUP04)
    val stORCAGroupPartnerSKUWEDDF = doGroup(stORCAJoin03DF, stORCAGroup04).get

    //30 - Union
    val stORCAInnerRightJoinUnionDF = doUnion(stORCAInnerJoin02DF, stORCARightJoin02DF).get

    //33 - Multi-field
    val stORCAJoinUnionCastDF = stORCAInnerRightJoinUnionDF.withColumn("Big Deal Qty",col("Big Deal Qty").cast(DoubleType))
      .withColumn("Qty", col("Qty").cast(DoubleType))

    //31 - Formula
    val stORCANonBigDealDF = stORCAJoinUnionCastDF.withColumn("Non Big Deal Qty",col("Qty")-col("Big Deal Qty"))

    //34 - Filter
    val sttORCAFilter01 = sttORCASource.filterOperation(FILTER01)
    val sttORCABigDealFilterDF = doFilter(sttORCAUnionDF, sttORCAFilter01, sttORCAFilter01.conditionTypes(NUMERAL0)).get

    //35 - Summarize
    val sttORCAGroup03 = sttORCASource.groupOperation(GROUP03)
    val sttORCAGroup03DF = doGroup(sttORCABigDealFilterDF,sttORCAGroup03).get

    //36 - Summarize
    val sttORCAGroup04 = sttORCASource.groupOperation(GROUP04)
    val sttORCAGroup04DF = doGroup(sttORCAUnionDF,sttORCAGroup04).get

    //37 - Join
    val sttORCAJoin02 = sttORCASource.joinOperation(JOIN02)
    val sttORCAGroup04WithRenamedDF = sttORCAGroup04DF.withColumnRenamed("Product Base ID","Right_Product Base ID")
      .withColumnRenamed("Product Base Desc","Right_Product Base Desc")
      .withColumnRenamed("Account Company","Right_Account Company")
      .withColumnRenamed("Week End Date","Right_Week End Date")
      .withColumnRenamed("Week","Right_Week")
    val sttORCAJoin02Map = doJoinAndSelect(sttORCAGroup03DF.withColumnRenamed("Qty","Big Deal Qty"), sttORCAGroup04WithRenamedDF, sttORCAJoin02)
    val sttORCAInnerJoin02DF = sttORCAJoin02Map("inner")
    val sttORCARightJoin02DF = sttORCAJoin02Map("right")

    //38 - Union
    val sttORCAInnerRightJoinUnionDF = doUnion(sttORCAInnerJoin02DF, sttORCARightJoin02DF).get

    //41 - Multi field
    val sttORCACastedDF = sttORCAInnerRightJoinUnionDF.withColumn("Big Deal Qty",col("Big Deal Qty").cast(DoubleType))
      .withColumn("Qty",col("Qty").cast(DoubleType))

    //39 - Formula
    val sttORCACastedNonBigDealDF = sttORCACastedDF.withColumn("Non Big Deal Qty",col("Qty")-col("Big Deal Qty"))

    //42 - Join
    val stORCANonBigDealWithRenamedDF = stORCANonBigDealDF.withColumnRenamed("Big Deal Qty","ST Big Deal Qty")
      .withColumnRenamed("Qty","ST Qty")
      .withColumnRenamed("Non Big Deal Qty","ST Non Big Deal Qty")
    val sttORCACastedNonBigDealWithRenamedDF = sttORCACastedNonBigDealDF.withColumnRenamed("Product Base ID","Right_Product Base ID").withColumnRenamed("Product Base Desc","Right_Product Base Desc")
      .withColumnRenamed("Account Company","Right_Account Company").withColumnRenamed("Week End Date","Right_Week End Date").withColumnRenamed("Big Deal Qty","STT Big Deal Qty")
      .withColumnRenamed("Qty","STT Qty").withColumnRenamed("Week","Right_Week")
    val sttORCAJoin03 = sttORCASource.joinOperation(JOIN03)
    val sttORCAJoin03Map = doJoinAndSelect(stORCANonBigDealWithRenamedDF, sttORCACastedNonBigDealWithRenamedDF, sttORCAJoin03)
    val sttORCAInnerJoin03DF = sttORCAJoin03Map("inner")
    val sttORCALeftJoin03DF = sttORCAJoin03Map("left")
    val sttORCARightJoin03DF = sttORCAJoin03Map("right")

    //47 - Union
    val sttORCALeftJoin03ReArrangedDF = sttORCALeftJoin03DF.withColumnRenamed("Big Deal Qty","ST Big Deal Qty").withColumnRenamed("Qty","ST Qty").withColumnRenamed("Non Big Deal Qty", "ST Non Big Deal Qty").withColumn("STT Big Deal",lit(null)).withColumn("STT Qty",lit(null)).withColumn("STT Non Big Deal Qty",lit(null))
    val sttORCARightJoin03ReArrangedDF = sttORCARightJoin03DF.withColumnRenamed("Big Deal Qty","STT Big Deal Qty").withColumnRenamed("Qty","STT Qty").withColumnRenamed("Non Big Deal Qty","STT Non Big Deal Qty").withColumn("ST Big Deal Qty",lit(null)).withColumn("ST Qty",lit(null)).withColumn("ST Non Big Deal Qty",lit(null))
    val sttORCA03InnerLeftUnionDF = doUnion(sttORCAInnerJoin03DF, sttORCALeftJoin03ReArrangedDF).get
    val sttORCA03JoinsUnionDF = doUnion(sttORCA03InnerLeftUnionDF, sttORCARightJoin03ReArrangedDF).get

    //TODO: Create Utility to type cast the columns. Accept map "column name" -> "type"
    //190 - Multi field cast
    val sttORCAJoinsUnionCastedDF = sttORCA03JoinsUnionDF.withColumn("ST Big Deal Qty", col("ST Big Deal Qty").cast(DoubleType))
      .withColumn("ST Qty",col("ST Qty").cast(DoubleType))
      .withColumn("ST Non Big Deal Qty", col("ST Non Big Deal Qty").cast(DoubleType))
      .withColumn("STT Big Deal Qty", col("STT Big Deal Qty").cast(DoubleType))
      .withColumn("STT Qty", col("STT Qty").cast(DoubleType))
      .withColumn("STT Non Big Deal Qty", col("STT Non Big Deal Qty").cast(DoubleType))

    //182 - Summarize
    val sttORCAGroup05 = sttORCASource.groupOperation(GROUP05)
    val sttORCAAccountWEDProductGroupedDF = doGroup(sttORCAJoinsUnionCastedDF, sttORCAGroup05).get

    //176 - Join
    val sttORCAJoin04 = sttORCASource.joinOperation(JOIN04)
    val sttORCAJoin04Map = doJoinAndSelect(sttORCAAccountWEDProductGroupedDF, stORCAGroupPartnerSKUWEDDF.withColumnRenamed("Week End Date","Right_Week End Date"), sttORCAJoin04)
    val sttORCALeftJoin04DF = sttORCAJoin04Map("left")
    val sttORCAInnerJoin04DF = sttORCAJoin04Map("inner")

    //185 - Unique
    val sttORCALeftJoin04UniqueDF = sttORCALeftJoin04DF.dropDuplicates(List("Account Company","Weeek End Date","Product Base ID"))

    //184 - Unique
    val sttORCAInnerJoin04UniqueDF = sttORCAInnerJoin04DF.dropDuplicates(List("Account Company","Week End date", "Product Base ID"))

    //179 - Formula
    val sttORCAJoin04STnSTTDealQty = sttORCAInnerJoin04DF.withColumn("STT Big Deal Qty",
        when(col("STT Qty")===0,col("STT Big Deal Qty")).otherwise(col("STT Big Deal Qty")+col("Claim Quantity")))
      .withColumn("STT Non Big Deal Qty", when(col("STT Qty")===0,col("STT Non Big Deal Qty")).otherwise(col("STT Non Big Deal Qty")-col("Claim Quantity")))
      .withColumn("ST Big Deal Qty", when(col("ST Qty")===0,col("ST Big Deal Qty")).otherwise(col("ST Big Deal Qty")+col("Claim Quantity")))
      .withColumn("ST Non Big Deal Qty", when(col("ST Qty")===0, col("ST Non Big Deal Qty")).otherwise(col("ST Non Big Deal Qty")-col("Claim Quantity")))

    /* Note: All operations 185, 184 outputs used in browse */

    //181 - Union
    val sttORCAUnionSTSTTFormulaAndJoinDF = doUnion(sttORCALeftJoin04DF, sttORCAJoin04STnSTTDealQty).get

    //46 - Reseller Formula
    val sttORCAUnionWithResellerFeatureDF = sttORCAUnionSTSTTFormulaAndJoinDF.withColumn("Reseller",
      when(col("Account Company")==="PC Connection Sales And Services",lit("PC Connection"))
      .when(col("Account Company")==="CDW Logistics Inc",lit("CDW"))
      .when(col("Account Company")==="PCM Sales",lit("PCM"))
      .otherwise(col("Account Company")))

    //199 - Summarize
    val sttORCAwithDummyDF = sttORCAUnionDF//.withColumn("dummy",lit(1))
    val sttORCAGroup01 = sttORCASource.groupOperation(GROUP01)
    var sttORCAGroupDF = sttORCAwithDummyDF.dropDuplicates(sttORCAGroup01.cols) //doGroup(sttORCAwithDummyDF, sttORCAGroup01).get.drop("dummy")
    sttORCAGroupDF = sttORCAGroupDF.select(Utils.convertListToDFColumn(sttORCAGroup01.cols, sttORCAGroupDF):_*)

    //213 - Formula
    val sttORCAValueDF = sttORCAGroupDF.withColumn("Value",lit(1))

    //212 - Cross Tab
    val sttORCAPivotDF = sttORCAValueDF.groupBy("SKU").pivot("Option Type").agg(sum("Value"))

    //211 - Formula
    val sttORCAOptionTypeDF = sttORCAPivotDF.withColumn("Option Type",
      when(col("All")===1,"All")
      .when((col("Option_B")===1)&&(col("Option_C")===1),"Option_C")
      .otherwise("Option_B"))

    //202 - Filter
    val sttORCAFilter02 = sttORCASource.filterOperation(FILTER02)
    val sttORCAFilterOptionTypeDF = doFilter(sttORCAOptionTypeDF, sttORCAFilter02, sttORCAFilter02.conditionTypes(NUMERAL0)).get

    //49 - Select
    val auxWEDRename02 = auxWEDSource.renameOperation(RENAME02)
    val auxWEDSelect01Rename02DF = Utils.convertListToDFColumnWithRename(auxWEDRename02, auxWEDSelectDF)

    //50 - Join
    val auxWEDJoin02 = auxWEDSource.joinOperation(JOIN02)
    val auxWEDJoinResellerMap = doJoinAndSelect(sttORCAUnionWithResellerFeatureDF, auxWEDSelect01Rename02DF, auxWEDJoin02)
    val auxWEDJoinResellerInnerJoinDF = auxWEDJoinResellerMap("inner")

    //53 - select
    val auxCommSelect01 = auxCommResellerSource.selectOperation(SELECT01)
    val auxCommSelectDF = doSelect(auxCommResellerDF, auxCommSelect01.cols, auxCommSelect01.isUnknown).get.withColumnRenamed("season","Season")

    //51 - Join
    val auxWEDJoin03 = auxWEDSource.joinOperation(JOIN03)
    val auxWEDSelect01Rename02RenamedDF = auxCommSelectDF/*auxWEDSelect01Rename02DF*/.withColumnRenamed("OPP Option","Promo Option")
      .withColumnRenamed("Season","Right_Season")
    val auxWEDCommResellerJoinMap = doJoinAndSelect(auxWEDJoinResellerInnerJoinDF, auxWEDSelect01Rename02RenamedDF, auxWEDJoin03)
    val auxWEDCommResellerLeftJoinDF = auxWEDCommResellerJoinMap("left")
    val auxWEDCommResellerInnerJoinDF = auxWEDCommResellerJoinMap("inner")

    //57 - Formula
    val auxWEDPromoOptionDF = auxWEDCommResellerLeftJoinDF.withColumn("Promo Option",lit("Option C"))

    //58 - Union
    val auxWEDCommResellerUnionDF = doUnion(auxWEDPromoOptionDF, auxWEDCommResellerInnerJoinDF).get

    //68 - Formula
    val auxWEDCommResellerClusterFeatureDF = auxWEDCommResellerUnionDF.withColumn("Reseller_Cluster",
      when(col("Reseller Category")==="DRC",col("Reseller"))
      .when(col("Promo Option")==="Option B",lit("Other - Option B"))
      .otherwise(lit("Other - Option C")))


    //69 - Formula
    val auxWEDCommResQtynBigDealQtynNonDealQtyDF = auxWEDCommResellerClusterFeatureDF.withColumn("Qty",
        when((col("Reseller_Cluster")==="Other - Option B") || (col("Reseller_Cluster")==="Other - Option C"),col("ST Qty"))
        .otherwise(col("STT Qty")))
      .withColumn("Big Deal Qty", when((col("Reseller_Cluster")==="Other - Option B") || (col("Reseller_Cluster")==="Other - Option C"),col("ST Big Deal Qty"))
        .otherwise(col("STT Big Deal Qty")))
      .withColumn("Non Big Deal Qty", when((col("Reseller_Cluster")==="Other - Option B") || (col("Reseller_Cluster")==="Other - Option C"),col("ST Non Big Deal Qty"))
        .otherwise(col("STT Non Big Deal Qty")))

    //62 - Join
    val auxWEDJoin04 = auxWEDSource.joinOperation(JOIN04)
    /* SKU renamed to join_SKU because after join Consol SKU being renamed to SKU and prev SKU is deselected */
    val auxSKUHierWithRenamedDF = auxSKUHierDF.withColumnRenamed("SKU","join_SKU")
      .withColumnRenamed("Consol SKU","SKU")
      .withColumnRenamed("L1: Use Case","L1_Category")
      .withColumnRenamed("L2: Key functionality","L2_Category")
      .withColumnRenamed("Abbreviated Name","SKU_Name")
      .withColumnRenamed("Street Price","Street_Price")
    val auxWEDSKUJoin04Map = doJoinAndSelect(auxWEDCommResQtynBigDealQtynNonDealQtyDF, auxSKUHierWithRenamedDF, auxWEDJoin04)
    val auxWEDSKUInnerJoinDF = auxWEDSKUJoin04Map("inner")
    val auxWEDSKULeftJoinDF = auxWEDSKUJoin04Map("left")

    //65 - Group Product, Aggregate ST & STT Qty
    val auxWEDGroup01 = auxWEDSource.groupOperation(GROUP01)
    val auxWEDProductGroupDF = doGroup(auxWEDSKULeftJoinDF, auxWEDGroup01).get

    //67 - Sort
    val auxWEDSort01 = auxWEDSource.sortOperation(SORT01)
    val auxWEDPRoductGroupSortedDF = doSort(auxWEDProductGroupDF, auxWEDSort01.ascending, auxWEDSort01.descending).get

    //201 - Join (SKU)
    val sttORCAJoin05 = sttORCASource.joinOperation(JOIN05)
    val sttORCAFilterOptionTypeWithRenamedDF = sttORCAFilterOptionTypeDF.withColumnRenamed("SKU","Right_SKU").withColumnRenamed("Option Type","Option Type (SKU)")
    val sttORCAJoin05Map = doJoinAndSelect(auxWEDSKUInnerJoinDF, sttORCAFilterOptionTypeWithRenamedDF, sttORCAJoin05)
    val sttORCALeftJoin05DF = sttORCAJoin05Map("left")
    val sttORCAInnerJoin05DF = sttORCAJoin05Map("inner")

    //203 - Union
    val sttORCAInnerLeftJoinUnionDF = doUnion(sttORCAInnerJoin05DF, sttORCALeftJoin05DF).get

    //197 - Formula (Reseller_Cluster)
    val sttORCAResellerClusterDF = sttORCAInnerLeftJoinUnionDF.withColumn("Reseller_Cluster",
      when((col("Option Type (SKU)")=!="Option C") && (col("Reseller_Cluster")==="Other - Option C"),"Other - Option B")
      .otherwise(col("Reseller_Cluster")))

    //70 - Multi field cast
    val sttORCAResellerClusterCastedDF = sttORCAResellerClusterDF.withColumn("Qty",col("Qty").cast(StringType))
      .withColumn("Big Deal Qty", col("Big Deal Qty").cast(StringType))
      .withColumn("Non Big Deal Qty", col("Non Big Deal Qty").cast(StringType))
    /* Used in 71 and 74*/

    //73 - Filter
    val auxWEDFilter01 = auxWEDSource.filterOperation(FILTER01)
    val auxWEDOptionTypFilteredDF = doFilter(auxWEDSelect01Rename02DF, auxWEDFilter01, auxWEDFilter01.conditionTypes(NUMERAL0)).get
    val auxWEDFilter02 = auxWEDSource.filterOperation(FILTER02)
    val auxWEDOptionTypNotFilteredDF = doFilter(auxWEDSelect01Rename02DF, auxWEDFilter02, auxWEDFilter02.conditionTypes(NUMERAL0)).get

    //71 - Join (SKU and Week_End_Date with filtered Option Typr)
    val auxWEDJoin05 = auxWEDSource.joinOperation(JOIN05)
    val auxWEDOptionTypFilteredWithRenamedDF = auxWEDOptionTypFilteredDF.withColumnRenamed("SKU","Right_SKU").withColumnRenamed("First_C2B Promo Code", "Promo ID").withColumnRenamed("Week_End_Date","Right_Week_End_Date")
    val auxWEDJoin05Map = doJoinAndSelect(sttORCAResellerClusterCastedDF, auxWEDOptionTypFilteredWithRenamedDF, auxWEDJoin05)
    val auxWEDLeftJoin05DF = auxWEDJoin05Map("left")
    val auxWEDInnerJoin05DF = auxWEDJoin05Map("inner")

    //74 - Join (SKU and Week_End_Date with unfiltered Option Type)
    val auxWEDJoin06 = auxWEDSource.joinOperation(JOIN06)
    val auxWEDOptionTypNotFilteredWithRenamedDF = auxWEDOptionTypNotFilteredDF.withColumnRenamed("SKU","Right_SKU")
      .withColumnRenamed("Week_End_Date","Right_Week_End_Date")
    val auxWEDJoin06Map = doJoinAndSelect(sttORCAResellerClusterCastedDF, auxWEDOptionTypNotFilteredWithRenamedDF, auxWEDJoin06)
    val auxWEDLeftJoin06DF = auxWEDJoin06Map("left")
    val auxWEDInnerJoin06DF = auxWEDJoin06Map("inner")

    //87 - Union
    val auxWED05JoinsUnionDF = doUnion(auxWEDLeftJoin05DF, auxWEDInnerJoin05DF).get

    //86 - Union
    val auxWED06JoinsUnionDF = doUnion(auxWEDLeftJoin06DF, auxWEDInnerJoin06DF).get
      .withColumnRenamed("IR","Option IR")

    //88 - Join (SKU, Reseller, Week_End_Date Unions of filtered and unfiltered joins)
    val auxWEDJoin07 = auxWEDSource.joinOperation(JOIN07)
    val auxWED06JoinsUnionWithRenamedDF = Utils.convertListToDFColumnWithRename(auxWEDSource.renameOperation(RENAME01), auxWED06JoinsUnionDF)
    val auxWEDJoin07Map = doJoinAndSelect(auxWED05JoinsUnionDF.withColumnRenamed("IR","All IR"), auxWED06JoinsUnionWithRenamedDF, auxWEDJoin07)
    val auxWEDInnerJoin07DF = auxWEDJoin07Map("inner")

    //114
    val auxEtailerSelect01 = auxEtailerSource.selectOperation(SELECT01)
    val auxEtailerSelectedDF = doSelect(auxEtailerDF, auxEtailerSelect01.cols, auxEtailerSelect01.isUnknown).get

    //83
    val ciORCASelect01 = ciORCASource.selectOperation(SELECT01)
    val ciORCASelectedDF = doSelect(ciORCADF, ciORCASelect01.cols, ciORCASelect01.isUnknown).get

    //280
    var ciORCAProdIDDF = ciORCASelectedDF.withColumn("Product Base ID",
      when(col("Product Base ID")==="M9L74A","M9L75A")
        .when((col("Product Base ID")==="J9V91A")||(col("Product Base ID")==="J9V92A"),"J9V90A")
        .otherwise(col("Product Base ID")))
    ciORCAProdIDDF = ciORCAProdIDDF.withColumn("Product Base ID",
      when((col("Product Base ID")==="X7N08A") || (col("Product Base ID")==="Z9L26A") || (col("Product Base ID")==="Z9L25A") || (col("Product Base ID")==="Z3Z93A") || (col("Product Base ID")==="Z3Z94A"),"X7N07A")
        .when(col("Product Base ID")==="B3Q17A","B3Q11A")
        .otherwise(col("Product Base ID")))

    //90 - Join
    val ciORCAJoin01 = ciORCASource.joinOperation(JOIN01)
    val auxWEDSelect01Rename02WithRenamedDF = auxWEDSelect01Rename02DF
      .withColumnRenamed("Week_End_Date","Right_Week_End_Date")
      .withColumnRenamed("wed","Week_End_Date")
    val ciORCAJoin01Map = doJoinAndSelect(ciORCAProdIDDF, auxWEDSelect01Rename02DF, ciORCAJoin01)
    val ciORCAauxWEDJoinDF = ciORCAJoin01Map("inner")

    //91 - Formula Beg_Inventory
    val ciORCABegInvFeatureDF = ciORCAauxWEDJoinDF.withColumn("Beg_Inventory",addDaystoDateStringUDF(col("Week_End_Date"),lit(7)))

    //94 - Summarize
    val ciORCAGroup01 = ciORCASource.groupOperation(GROUP01)
    val ciORCABegGroupDF = doGroup(ciORCABegInvFeatureDF, ciORCAGroup01).get

    //93     /* 331 not implemented coz used for browse*/
    val auxWEDIRInvClusterPromoIDDF = auxWEDInnerJoin07DF.withColumn("IR",
        when((col("All IR").isNull) && (col("Option IR").isNull),0)
        .otherwise(greatest(col("All IR"),col("Option IR"))))
        //.otherwise(findMaxBetweenTwo(col("All IR"),col("Option IR"))))
      .withColumn("Inventory Cluster", when((col("Reseller_Cluster")==="Other - Option B") || (col("Reseller_Cluster")=="Other - Option C"), lit("Distributor"))
          .otherwise(col("Reseller_Cluster")))
      .withColumn("Promo ID", findMaxBetweenTwo(col("Promo ID"),col("Right_First_C2B Promo Code")))

    //96 - Join input from 94
    /*292 not implemented*/
    val ciORCAJoin02 = ciORCASource.joinOperation(JOIN02)
    val ciORCAJoin02Map = doJoinAndSelect(auxWEDIRInvClusterPromoIDDF, ciORCABegGroupDF, ciORCAJoin02)
    val ciORCAInnerJoin02DF = ciORCAJoin02Map("inner")
    val ciORCALeftJoin02DF = ciORCAJoin02Map("left")

    //100
    val ciORCAJoin02UnionDF = doUnion(ciORCAInnerJoin02DF, ciORCALeftJoin02DF).get

    //102 - Select
    val ciORCASelect02 = ciORCASource.selectOperation(SELECT02)
    var ciORCAUnionSelectDF = doSelect(ciORCAJoin02UnionDF, ciORCASelect02.cols, ciORCASelect02.isUnknown).get
    ciORCAUnionSelectDF = Utils.convertListToDFColumnWithRename(ciORCASource.renameOperation(RENAME01), ciORCAUnionSelectDF)
    //DONE till here
    //115 - Join
    val ciORCAJoin03 = ciORCASource.joinOperation(JOIN03)
    val ciORCAJoin03Map = doJoinAndSelect(ciORCAUnionSelectDF, auxEtailerSelectedDF, ciORCAJoin03)
    val ciORCAInnerJoin03DF = ciORCAJoin03Map("inner")
    val ciORCALeftJoin03DF = ciORCAJoin03Map("left")

    //117 - Add eTailer
    val ciORCALeftJoineTailerFeatureDF = ciORCALeftJoin03DF.withColumn("eTailer",lit(0))

    //116 - Union
    val ciORCAJoins03UnionDF = doUnion(ciORCALeftJoineTailerFeatureDF, ciORCAInnerJoin03DF).get

    //104 - Join
    /*216 not implemented*/
    val ciORCAJoin04 = ciORCASource.joinOperation(JOIN04)
    val auxWEDPRoductGroupSortedRenamedDF = auxWEDPRoductGroupSortedDF.withColumnRenamed("SKU","Right_SKU")
    val ciORCAJoin04Map = doJoinAndSelect(ciORCAJoins03UnionDF, auxWEDPRoductGroupSortedRenamedDF, ciORCAJoin04)
    val ciORCAInnerJoin04DF = ciORCAJoin04Map("inner")
    val ciORCALeftJoin04DF = ciORCAJoin04Map("left")
    //110 /*Need not implement for now - browse*/

    //227 - Union
    val ciORCAJoins04UnionDF = doUnion(ciORCAInnerJoin04DF, ciORCALeftJoin04DF).get

    //112 - Formula
    val ciORCAInvQtySpendPromoFlgSeasonBigDealNonBigDealDF = ciORCAJoins04UnionDF.withColumn("Inv_Qty",
        when((col("Inv_Qty").isNull) || (col("Inv_Qty") < 0), 0)
        .otherwise(col("Inv_Qty")))
      .withColumn("Spend",col("IR")*col("Non_Big_Deal_Qty"))
      .withColumn("Promo Flag", when(col("IR")===0,0).otherwise(1))
      .withColumn("Big_Deal_Qty",when((col("Resller")==="General Data Tech") && (col("SKU")==="D3Q17A") && (col("Week_End_Datte")==="2017-04-29"),1183)
        .when((col("Reseller")==="General Data Tech") && (col("SKU")==="D3Q21A") && (col("Week_End_Date")==="2017-04-29"), 1181)
        .otherwise(col("Big_Deal_Qty")))
      .withColumn("Non_Big_Deal_Qty", when((col("Resller")==="General Data Tech") && (col("SKU")==="D3Q17A") && (col("Week_End_Date")==="2017-04-29"),10)
        .when((col("Reseller")==="General Data Tech") && (col("SKU")==="D3Q21A") && (col("Week_End_Date")==="2017-04-29"),12)
        .otherwise(col("Non_Big_Deal_Qty")))
      .withColumn("Season",when(col("IPSLES")==="IPS",col("Season"))
        .when(col("Week_End_Date")==="2016-10-01","BTS'16")
        .when(col("Week_End_Date")==="2016-12-31","HOL'16")
        .when(col("Week_End_Date")==="2017-04-01","BTB'17")
        .when(col("Week_End_Date")==="2017-07-01","STS'17")
        .otherwise(col("Season")))


    //330 - Summarize
    val ciORCAGroup02 = ciORCASource.groupOperation(GROUP02)
    val ciORCAGroup02DF = doGroup(ciORCAInvQtySpendPromoFlgSeasonBigDealNonBigDealDF, ciORCAGroup02).get

    //103
    val ciORCASort01 = ciORCASource.sortOperation(SORT01)
    val ciORCAFeaturesSortedDF = doSort(ciORCAGroup02DF, ciORCASort01.ascending, ciORCASort01.descending).get

    //322 - Read new source
    val histPOSDF = Utils.loadCSV(executionContext, histPOSSource.filePath).get.cache()

    //327 - Convert wed to dateFormat
    val histPOSDateFormattedDF = histPOSDF.withColumn("Week_End_Date", convertDatetoFormat(col("wed"),lit("MM/dd/yyyy")))
      .drop("wed")

    //323 - Select
    val histPOSSelect01 = histPOSSource.selectOperation(SELECT01)
    val histPOSSelectDF = doSelect(histPOSDateFormattedDF, histPOSSelect01.cols, histPOSSelect01.isUnknown).get
      .withColumnRenamed("Account","Reseller").withColumnRenamed("Account Consol","Resller_Cluster")
      .withColumnRenamed("Inv : Saleable Qty","Inv_Qty_Raw").withColumnRenamed("Sales : New Qty","Qty")
      .withColumnRenamed("season","Season")

    //324 - Filter channel
    val histPOSFilter01 = histPOSSource.filterOperation(FILTER01)
    val histPOSFilterChannelDF = doFilter(histPOSSelectDF, histPOSFilter01, histPOSFilter01.conditionTypes(NUMERAL0)).get

    //326 - Group
    val hisPOSGroup01 = histPOSSource.groupOperation(GROUP01)
    val histPOSGroupedDF = doGroup(histPOSFilterChannelDF, hisPOSGroup01).get
      .withColumnRenamed("season_ordered","Season_Ordered")
      .withColumnRenamed("cal_month","Cal_Month")
      .withColumnRenamed("cal_year","Cal_Year")
      .withColumnRenamed("fiscal_quarter","Fiscal_Quarter")
      .withColumnRenamed("fiscal_year","Fiscal_Year")

    //328 - Formula (Big_Deal_Qty, eTailer, IR, Non_Big_Deal_Qty, Qty, Promo_Flag, Spend)
    val histPOSNewVariablesDF = histPOSGroupedDF.withColumn("Big_Deal_Qty",lit(0))
      .withColumn("eTailer",lit(0))
      .withColumn("IR",lit(0))
      .withColumn("Non_Big_Deal_Qty",col("Sum_Qty"))
      .withColumn("Qty",col("Big_Deal_Qty")+col("Non_Big_Deal_Qty"))
      .withColumn("Promo Flag",lit(0))
      .withColumn("Spend",col("IR")*col("Non_Big_Deal_Qty"))

    //325 - Join
    val histPOSJoin01 = histPOSSource.joinOperation(JOIN01)
    val auxSKUHierRenamedDF = Utils.convertListToDFColumnWithRename(histPOSSource.renameOperation("rename01"), auxSKUHierDF)
    val histPOSJoinSKUHierMap = doJoinAndSelect(histPOSNewVariablesDF.withColumnRenamed("season","Season"), auxSKUHierRenamedDF, histPOSJoin01)
    val histPOSInnerJoinSKUDF = histPOSJoinSKUHierMap("inner")

    //329 - Union
    val histPOSUnionCIDF = doUnion(histPOSInnerJoinSKUDF, ciORCAFeaturesSortedDF).get

    //229 - Select
    /********  Main One  *******/
    val histPOSSelect02 = histPOSSource.selectOperation(SELECT02)
    val histPOSSelect02DF = doSelect(histPOSUnionCIDF, histPOSSelect02.cols, histPOSSelect02.isUnknown).get

    //221 /* Need not do it right now - browse*/
    val histPOSSelectUniqueDF = histPOSSelect02DF.dropDuplicates(List("IR","Reseller","Week_End_Date","SKU"))

    //270 - Workflow run date
    /********  Main One  *******/
    val histPOSWithRunDateDF = histPOSSelectUniqueDF.withColumn("Workflow Run Date", lit(current_date()/*LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss"))*/))

    //278 - Select
    val histPOSSelect03 = histPOSSource.selectOperation(SELECT03)
    val histPOSSelect03DF = doSelect(histPOSSelect02DF, histPOSSelect03.cols, histPOSSelect03.isUnknown).get

    //242 - Save posqty_output_commercial.csv
    histPOSSelect02DF.show()
    //TODO: histPOSSelect02DF - writeCSV utility

    //269 - Save posqty_output_commercial_with_date.csv
    histPOSWithRunDateDF.show()
    //TODO: writeCSV utility

    //264 - 267  --> Reading last written posqty_Commercial output.csv

    //262 - Select
    val archivePOSSelect01 = archivePOSSource.selectOperation(SELECT01)
    val archivePOS = doSelect(archivePOSDF, archivePOSSelect01.cols, archivePOSSelect01.isUnknown).get

    //281 - Join
    val archivePOSJoin01 = archivePOSSource.joinOperation(JOIN01)
    val archivePOSRenamed = archivePOS.withColumnRenamed("Right_Series","Left_Right_Series")
      .withColumnRenamed("Right_Need Big Data","Left_Right_Need Big Data")
    val histPOSSelect03RenamedDF = Utils.convertListToDFColumnWithRename(archivePOSSource.renameOperation(RENAME01), histPOSSelect03DF)
    val archivePOSJoin01Map = doJoinAndSelect(archivePOSRenamed, histPOSSelect03RenamedDF, archivePOSJoin01)
    val archivePOSInnerJoin01DF = archivePOSJoin01Map("inner")
    val archivePOSLeftJoin01DF = archivePOSJoin01Map("left")
    val archivePOSRightJoin01DF = archivePOSJoin01Map("right")

    //282 - Select
    val archivePOSSelect02 = archivePOSSource.selectOperation(SELECT02)
    val archivePOSRightJoinSelectDF = doSelect(archivePOSRightJoin01DF, archivePOSSelect02.cols, archivePOSSelect02.isUnknown).get
      .withColumnRenamed("Qty","New Qty").withColumnRenamed("Big_Deal_Qty","New Big_Deal_Qty").withColumnRenamed("Non_Big_Deal_Qty","New Non_Big_Deal_Qty")

    //283 - Union
    var archivePOSJoinsUnionDF = doUnion(doUnion(archivePOSInnerJoin01DF, archivePOSLeftJoin01DF).get, archivePOSRightJoinSelectDF).get

    //279 - Select
    val archivePOSSelect03 = archivePOSSource.selectOperation(SELECT03)
    val archivePOSSelect03DF = doSelect(archivePOSJoinsUnionDF, archivePOSSelect03.cols, archivePOSSelect03.isUnknown).get

    //260 - Formula
    val archivePOSNewVarsDF = archivePOSSelect03DF.withColumn("Qty", when(col("Qty").isNull,0).otherwise(col("Qty")))
      .withColumn("New Qty", when(col("New Qty").isNull,0).otherwise(col("New Qty")))
      .withColumn("Big_Deal_Qty", when(col("Big_Deal_Qty").isNull,0).otherwise(col("Big_Deal_Qty")))
      .withColumn("New Big_Deal_Qty", when(col("New Big_Deal_Qty").isNull,0).otherwise(col("New Big_Deal_Qty")))
      .withColumn("Non_Big_Deal_Qty", when(col("Non_Big_Deal_Qty").isNull,0).otherwise(col("Non_Big_Deal_Qty")))
      .withColumn("New Non_Big_Deal_Qty", when(col("New Non_Big_Deal_Qty").isNull,0).otherwise(col("New Non_Big_Deal_Qty")))
      .withColumn("Qty Diff", when((col("New Qty")-col("Qty")).isNull,0).otherwise(col("New Qty")-col("Qty")))
      .withColumn("Big Deal QTY Diff", when((col("New Big_Deal_Qty")-col("Big_Deal_Qty")).isNull,0).otherwise(col("New Big_Deal_Qty")-col("Big_Deal_Qty")))
      .withColumn("Non Big Deal QTY Qty", when((col("New Non_Big_Deal_Qty")-col("Non_Big_Deal_Qty")).isNull,0).otherwise(col("New Non_Big_Deal_Qty")-col("Non_Big_Deal_Qty")))
      .withColumn("Qty Diff", when(col("Qty Diff").isNull,0).otherwise(col("Qty Diff")))
      .withColumn("Big Deal QTY Diff", when(col("Big Deal QTY Diff").isNull,0).otherwise(col("Big Deal QTY Diff")))
      .withColumn("Non Big deal Qty Diff", when(col("Non Big deal Qty Diff").isNull,0).otherwise(col("Non Big deal Qty Diff")))

    //277 - Summarize
    //was archivePOSSource.groupOperation
    val archivePOSGroup01 = archivePOSSource.groupOperation(GROUP01)
    val archivePOSGroup01DF = archivePOSNewVarsDF.select(Utils.convertListToDFColumn(archivePOSGroup01.cols, archivePOSNewVarsDF):_*)
      .dropDuplicates(archivePOSGroup01.cols)
    //archivePOSGroup01DF = archivePOSGroup01DF.select(Utils.convertListToDFColumn(archivePOSGroup01.cols, archivePOSGroup01DF):_*)

    //295 - Filter Week_End_Date
    val archivePOSFilter01 = archivePOSSource.filterOperation(FILTER01)
    val archivePOSFilterWEDDF = archivePOSGroup01DF.filter(col("Week_End_Date") > lit("2016-01-01"))
    //val archivePOSFilterWEDDF = doFilter(archivePOSGroup01DF, archivePOSFilter01, archivePOSFilter01.conditionTypes(NUMERAL0)).get

    //294 - Write QA File
    archivePOSFilterWEDDF.show()
    //TODO: use writeCSV utility here

    //302 - summarize (Add new column max of Week_End_Date) & 303 - Append
    val max_WED = histPOSSelect03DF.agg(max("Week_End_date")).head().getInt(0)
    val histPOSSelect03MaxWEDDF = histPOSSelect03DF.withColumn("Max_Week_End_Date", lit(max_WED))
    //Select "Big_Deal_Qty","Brand","Category","Category Custom","Category_Subgroup","Category_1","Category_2","HPS/OPS","IPSLES","IR","Mono/Color","Non_Big_Deal_Qty","Promo ID","Promo_Option","Qty","Reseller","Reseller_Cluster","Season","Series","SKU","SKU_Name","Street_Price","Week_End_Date","Inv_Qty","Season_Ordered","Cal_Month","Cal_Year","Fiscal_Qtr","Fiscal_Year","eTailer","Spend","Promo Flag"

    //299 - Select : NOT REQUIRED as all fields getting selected

    //298 - Filter
    val histPOSFilter02 = archivePOSSource.filterOperation(FILTER02)
    val histPOSFilteredSeasonDF = doFilter(histPOSSelect03MaxWEDDF, histPOSFilter02, histPOSFilter02.conditionTypes(NUMERAL0)).get
    //Check if BTB'18: apostry will create any problem or not

    //297 - Summarize
    val histPOSGroup02 = archivePOSSource.groupOperation(GROUP02)
    val histPOSGroup02DF = doGroup(histPOSFilteredSeasonDF, histPOSGroup02).get

    //300 - Pivot
    val histPOSPivotDF = histPOSGroup02DF.groupBy("Reseller_Cluster").pivot("Week_End_Date").agg(first("Sum_Qty"))

    //304  - Save
    histPOSPivotDF.show()
    //TODO: save as Commercial qty check.csv using writeCSV

    //311 - 377 same as 302 - 298

    //306 - Summarize
    val histPOSGroup03 = archivePOSSource.groupOperation(GROUP03)
    val histPOSGroup03DF = doGroup(histPOSFilteredSeasonDF, histPOSGroup03).get

    //309 - Cross tab Pivot
    val histPOSPivot02DF = histPOSGroup03DF.groupBy("Reseller_Cluster").pivot("Week_End_Date").agg(first("Sum_Inv_Qty"))

    //313 - Save
    histPOSPivot02DF.show()
    //TODO: histPOSPivot02DF write this using writeCSV

    //314 - 317: Same as 302 - 298
    //318 - Group
    val histPOSGroup04 = archivePOSSource.groupOperation(GROUP04)
    val histPOSGroup04DF = doGroup(histPOSFilteredSeasonDF, histPOSGroup04).get

    //319 - add diff%
    val histPOSDiffPerDF = histPOSGroup04DF.withColumn("diff%", (col("Sum_Qty Diff")/col("Sum_Qty"))*100)
        .withColumn("flag",when((col("diff%")>10) || (col("diff%").isNull) || (col("diff%")<lit(-10)),1).otherwise(0))
        .withColumn("diff%",round(col("diff%"),1))

    //321 - Save
    histPOSDiffPerDF.show()
    //TODO: Save this as Commercial_POS_old_vs_new.csv
  }
}

