package com.scienaptic.jobs.core

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.bean.{UnionOperation, _}
import com.scienaptic.jobs.utility.Utils
import com.scienaptic.jobs.utility.CommercialUtility._
import io.netty.handler.ssl.ApplicationProtocolConfig.SelectorFailureBehavior
import org.apache.avro.generic.GenericData
import org.apache.xerces.impl.xpath.regex
//import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
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

    val auxWEDSelect01 = auxWEDSource.selectOperation(SELECT01)
    var auxWEDSelectDF = SelectOperation.doSelect(auxWEDDF, auxWEDSelect01.cols, auxWEDSelect01.isUnknown).get
    //TODO: Rename some columns  (Check 141)
    val auxWEDRename01 = auxWEDSource.renameOperation("rename01")
    //auxWEDSelectDF = RenameOpe

    val iecSelect01 = iecSource.selectOperation(SELECT01)
    val iecSelectDF = SelectOperation.doSelect(iecDF, iecSelect01.cols, iecSelect01.isUnknown).get

    val iecClaimFilter01 = iecSource.filterOperation(FILTER01)
    val iecFiltered01DF = FilterOperation.doFilter(iecSelectDF, iecClaimFilter01, iecClaimFilter01.conditionTypes(NUMERAL0))
    //285 - Select
    val xsClaimsSelect01 = xsClaimsSource.selectOperation(SELECT01)
    val xsClaimsSelect01DF = SelectOperation.doSelect(xsClaimsDF, xsClaimsSelect01.cols,xsClaimsSelect01.isUnknown).get
    /*TODO: Rename 3 columns*/

    /* Union XS Claims and IEC Claims DF TODO: Check if both Dataframes have same number of columns*/
    val xsClaimsUnionDF = UnionOperation.doUnion(iecFiltered01DF.get, xsClaimsSelect01DF).get

    val baseSKUproductDF = xsClaimsUnionDF.withColumn("Base SKU",createBaseSKUFromProductIDUDF(col("Product ID")))
    val tempDateCalDF = baseSKUproductDF.withColumn("Temp Date Calc String",extractWeekFromDateUDF(col("Partner Ship Calendar Date"), col("week")))
    val weekEndDateDF = tempDateCalDF.withColumn("Week End Date",addDaystoDateStringUDF(col("Partner Ship Calendar Date"), col("day")))
    val xsClaimsBaseSKUDF = weekEndDateDF.withColumn("Base SKU",baseSKUFormulaUDF(col("Base SKU")))

    //127
    val xsClaimsGroup01 = xsClaimsSource.groupOperation("group01")
    val xsClaimsGroupedClaimQuanAggDF = GroupOperation.doGroup(xsClaimsBaseSKUDF,xsClaimsGroup01.cols,xsClaimsGroup01.aggregations).get
      .withColumnRenamed("Claim Quantity","Sum_Claim Quantity")

    //172
    val xsClaimsGroup02 = xsClaimsSource.groupOperation("group02")
    var xsClaimsGroupSumClaimQuanAggDF = GroupOperation.doGroup(xsClaimsGroupedClaimQuanAggDF, xsClaimsGroup02.cols, xsClaimsGroup02.aggregations).get

    val rawCalendarSelect01= rawCalendarSource.selectOperation(SELECT01)
    var rawCalendarSelectDF = SelectOperation.doSelect(rawCalendarDF, rawCalendarSelect01.cols, rawCalendarSelect01.isUnknown).get
    rawCalendarSelectDF = rawCalendarSelectDF.withColumn("dummy",lit(1))

    //124
    val rawCalendarGroup01 = rawCalendarSource.groupOperation("group01")
    var rawCalendarGroupDF = GroupOperation.doGroup(rawCalendarSelectDF, rawCalendarGroup01.cols, rawCalendarGroup01.aggregations).get
    rawCalendarGroupDF = rawCalendarGroupDF.drop("dummy")//.drop("sum") //TODO: check what is name of sum aggregated column

    //243 - Join
    val iecXSJoin01 = xsClaimsSource.joinOperation(JOIN01)
    var iecXSJoinMap = JoinAndSelectOperation.doJoinAndSelect(xsClaimsGroupedClaimQuanAggDF, rawCalendarGroupDF, iecXSJoin01)
    val iecXSLeftJoinDF = iecXSJoinMap("left")
    val iecXSInnerJoinDF = iecXSJoinMap("inner")

    //245 - Formula
    val iecXSLeftJoinPromoAmountDF = iecXSLeftJoinDF.withColumn("Promo",lit("N"))
      .withColumn("Total Amount",col("Claim Partner Unit Rebate")*col("Sum_Claim Quantity"))
    //244 - Formula
    val iecXSInnerJoinPromoAmountDF = iecXSInnerJoinDF.withColumn("Promo",lit("Y"))
      .withColumn("Total Amount",col("Claim Partner Unit Rebate")*col("Sum_Claim Quantity"))
    //246 - Union
    val iecXSUnionDF = UnionOperation.doUnion(iecXSLeftJoinPromoAmountDF, iecXSInnerJoinPromoAmountDF).get

    //252 - Join
    val iecXsClaimsJoin02 = xsClaimsSource.joinOperation(JOIN02)
    val xsInnerJoinAuxWEDMap = JoinAndSelectOperation.doJoinAndSelect(iecXSUnionDF, auxWEDSelectDF, iecXsClaimsJoin02)
    val xsInnerJoinAuxWEDDF = xsInnerJoinAuxWEDMap("inner")
    //254 - join
    val xsAuxSkuHierJoin03 = xsClaimsSource.joinOperation(JOIN03)
    var xsAuxSkuJoinMap = JoinAndSelectOperation.doJoinAndSelect(xsInnerJoinAuxWEDDF, auxSKUHierDF, xsAuxSkuHierJoin03)
    val xsAuxSkuLeftJoinDF = xsAuxSkuJoinMap("left")
    val xsAuxSkuInnerJoinDF = xsAuxSkuJoinMap("inner")
    //TODO: Check if both dataframe have same number of columns
    val xsAuxSKUHierUnionDF = UnionOperation.doUnion(xsAuxSkuLeftJoinDF, xsAuxSkuInnerJoinDF)

    //TODO: create WriteCSV utility and make changes to Source bean to accept 'outFilePath' attribute
    // Utils.writeCSV(xsAuxSKUHierUnionDF,"/home/Avik/Scienaptic/RnD/OutData/claims_consolidated.csv")

    //121 - Join
    val rawCalendarJoin01 = rawCalendarSource.joinOperation(JOIN01)
    var rawXSJoinMap = JoinAndSelectOperation.doJoinAndSelect(xsClaimsGroupedClaimQuanAggDF, rawCalendarGroupDF, rawCalendarJoin01)
    //val rawXSLeftJoin01 = rawXSJoinMap("left")   //Being used for Dump!
    val rawXSInnerJoin01DF = rawXSJoinMap("inner")

    //129
    val rawXSJoinChkOutsidePromoDF = rawXSInnerJoin01DF.withColumn("Outside Promo Date", checkOutsidePromoDateUDF(col("Partner Ship Calendar Date"),col("End Date")))

    //131
    val rawCalendarFilter01 = rawCalendarSource.filterOperation(FILTER01)
    val rawCalendarFilter02 = rawCalendarSource.filterOperation(FILTER02)
    //132
    val rawXSJoinOutsidePromoTrueDF = FilterOperation.doFilter(rawXSJoinChkOutsidePromoDF, rawCalendarFilter01, rawCalendarFilter01.conditionTypes(NUMERAL0)).get
    //134
    val rawXSJoinOutsidePromoFalseDF = FilterOperation.doFilter(rawXSJoinChkOutsidePromoDF, rawCalendarFilter02, rawCalendarFilter02.conditionTypes(NUMERAL0)).get

    //133
    val rawCalendarGroup02 = rawCalendarSource.groupOperation(GROUP02)
    val rawCalendarGroup02DF = GroupOperation.doGroup(rawXSJoinOutsidePromoTrueDF, rawCalendarGroup02.cols, rawCalendarGroup02.aggregations).get
    //rawCalendarGroup02DF = rawCalendarGroup02DF.withColumnRenamed("Sum_Sum_Claim Quantity","Avg_Sum_Sum_Claim Quantity")

    //135 - Join
    val rawCalendarJoin02 = rawCalendarSource.joinOperation(JOIN02)
    val rawCalendarJoin02Map = JoinAndSelectOperation.doJoinAndSelect(rawXSJoinOutsidePromoFalseDF, rawCalendarGroup02DF, rawCalendarJoin02)
    val rawCalendarJoin02DF = rawCalendarJoin02Map("inner")
    //136 - Formula
    val rawCalendarIncludeVarDF = rawCalendarJoin02DF.withColumn("Include",
      when(col("Sum_Sum_Claim Quantity") > lit(0.2)*col("Avg_Sum_Sum_Claim Quantity"),"Y")
      .otherwise("N"))

    //138 - Filter
    val rawCalendarFilter03 = rawCalendarSource.filterOperation(FILTER03)
    val rawCalendarFilterIncldeDF = FilterOperation.doFilter(rawCalendarIncludeVarDF, rawCalendarFilter03, rawCalendarFilter03.conditionTypes(NUMERAL0)).get

    //144 - Summarize
    val rawCalendarGroup03 = rawCalendarSource.groupOperation(GROUP04)
    val rawCalendarPromonSKUGroupDF = GroupOperation.doGroup(rawCalendarFilterIncldeDF,rawCalendarGroup03.cols, rawCalendarGroup03.aggregations).get

    //146 - Join
    /*TODO: Suppose joining criteria is same but in select nothing given,
      in that case, Join Utility should exclude same columns*/
    val rawCalendarJoin03 = rawCalendarSource.joinOperation(JOIN03)
    val rawCalendarJoin03Map =JoinAndSelectOperation.doJoinAndSelect(rawCalendarSelectDF,rawCalendarPromonSKUGroupDF,rawCalendarJoin03)
    val rawCalendarLeftJoin03DF = rawCalendarJoin03Map("left")
    val rawCalendarInnerJoin03DF = rawCalendarJoin03Map("inner")

    //151 - Formula
    val rawCalendarLftJNewEndDateDF = rawCalendarLeftJoin03DF.withColumn("New End Date",col("End Date"))

    //148 - Formula
    val rawCalendarInnJNewEndDateDF = rawCalendarInnerJoin03DF.withColumn("New End Date", newEndDateFromMaxWED(col("Max_Week.End.Date"), col("End Date")))

    //150 - Union
    //TODO: Check if both dataframes have same number of columns and data type is same
    val rawCalendarEndDateUnionDF = UnionOperation.doUnion(rawCalendarLftJNewEndDateDF,rawCalendarInnJNewEndDateDF).get

    //155 - Formula
    val rawCalendarEndDateChangeDF = rawCalendarEndDateUnionDF.withColumn("End Date Change",
      when(col("End Date")===col("New End Date"),"N")
          .otherwise("Y"))

    //157 - Union
    val rawCalendarEndDateChangeUnionDF = rawCalendarEndDateChangeDF.dropDuplicates(List("SKU","C2B Promo Code","Start Date","New End Date"))

    //158 - Append (Cartesian join)
    //TODO: Partition data with >2000 partitions before join
    val rawCalendarnAuxWEDAppendDF = rawCalendarEndDateChangeUnionDF.drop("fy_week").join(auxWEDSelectDF)

    //159 - Formula
    val rawCalStartsubWEDDF = rawCalendarnAuxWEDAppendDF.withColumn("Start - Week End", dateTimeDiff(col("Start Date"),col("Week.End.Date")))
      .withColumn("Week End - End",dateTimeDiff(col("Week.End.Date"),col("New End Date")))
      .withColumn("Option Type",
        when(col("Promo Name").contains("Option B"),"Option B")
        .when(col("Promo Name").contains("Option C"),"Option C")
        .otherwise("All"))

    //160 - Filter
    //TODO: [Start - Week End]<=-3 AND [Week End - End]<=3
    val rawCalendarEndDateFilteredDF = rawCalStartsubWEDDF

    //161 - Select
    val rawCalendarSelect02 = rawCalendarSource.selectOperation(SELECT02)
    val rawCalendarEndDateFilterSelectDF = SelectOperation.doSelect(rawCalendarEndDateFilteredDF, rawCalendarSelect02.cols, rawCalendarSelect02.isUnknown).get
    rawCalendarEndDateFilterSelectDF.cache()
    /*  -------------  USED TWICE ---------  */

    //162 - Sort
    val rawCalendarSort01 = rawCalendarSource.sortOperation(SORT01)
    val rawCalendarSelectSortDF = SortOperation.doSort(rawCalendarEndDateFilterSelectDF, rawCalendarSort01.ascending, rawCalendarSort01.descending).get
    /*  ------------- USED IN 2nd SECTION --------------*/

    //163 - Unique
    val rawCalendarSortDistinctDF = rawCalendarSelectSortDF.dropDuplicates(List("SKU","C2B Promo Code","Week.End.Date"))

    //196 - Join
    val rawCalendarJoin04 = rawCalendarSource.joinOperation(JOIN04)
    val rawCalendarDistinctLeftJoinDF = JoinAndSelectOperation.doJoinAndSelect(xsClaimsGroupSumClaimQuanAggDF, rawCalendarSortDistinctDF, rawCalendarJoin04)("left")
    /*  ------------- USED IN 2nd SECTION --------------*/

    //19 - Summarize (Group on C2B Promo Code)
    var rawCalendarEndDateSelectGroupDF = rawCalendarEndDateFilterSelectDF.withColumn("dummy",lit(1))
    val rawCalendarGroup04 = rawCalendarSource.groupOperation(GROUP04)
    rawCalendarEndDateSelectGroupDF = GroupOperation.doGroup(rawCalendarEndDateSelectGroupDF, rawCalendarGroup04.cols, rawCalendarGroup04.aggregations).get
      .drop("dummy")
      .cache()
    /*  ------------- USED IN 2nd SECTION -------- */

    //85
    val rawCalendarGroup05 = rawCalendarSource.groupOperation(GROUP05)
    var rawCalendarSortGroupDF = GroupOperation.doGroup(rawCalendarSelectSortDF, rawCalendarGroup05.cols, rawCalendarGroup05.aggregations).get
    val wind = Window.partitionBy(col("C2B Promo Code"))
    rawCalendarSortGroupDF = rawCalendarSortGroupDF.withColumn("rn",row_number.over(wind))
      .where(col("rn") === 1)
      .drop("rn")
    /*  ------------- USED IN 2nd SECTION --------------*/


    //13 - Select
    val stORCASelect01 = stORCASource.selectOperation(SELECT01)
    val stORCASelectDF = SelectOperation.doSelect(stORCADF, stORCASelect01.cols, stORCASelect01.isUnknown).get

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
    val stORCAJoin01Map = JoinAndSelectOperation.doJoinAndSelect(stORCAFormulaDF, rawCalendarEndDateSelectGroupDF, stORCAJoin01)
    val stORCAInnerJoinDF = stORCAJoin01Map("inner")
    val stORCALeftJoinDF = stORCAJoin01Map("left")

    //21 - Formula
    val stORCAInnerBigDealDF = stORCAInnerJoinDF.withColumn("Big Deal",lit(0))

    //22 - Union
    val stORCAUnionDF = UnionOperation.doUnion(stORCALeftJoinDF, stORCAInnerBigDealDF).get

    //12 - Select
    val sttORCASelect01 = sttORCASource.selectOperation(SELECT01)
    val sttORCASelectDF = SelectOperation.doSelect(sttORCADF, sttORCASelect01.cols, sttORCASelect01.isUnknown).get

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
    val sttORCAJoin01Map = JoinAndSelectOperation.doJoinAndSelect(sttORCAFormulaDF, rawCalendarEndDateSelectGroupDF, stORCAJoin01)
    val sttORCAInnerJoinDF = sttORCAJoin01Map("inner")
    val sttORCALeftJoinDF = sttORCAJoin01Map("left")

    //24 - Formula
    val sttORCAInnerBigDealDF = sttORCAInnerJoinDF.withColumn("Big Deal",lit(0))

    //23 - Union
    val sttORCAUnionDF = UnionOperation.doUnion(sttORCAInnerBigDealDF, sttORCALeftJoinDF).get

    //26 - Filter
    val stORCAFilter01 = stORCASource.filterOperation(FILTER01)
    val stORCAFilterBigDealDF = FilterOperation.doFilter(stORCAUnionDF, stORCAFilter01, stORCAFilter01.conditionTypes(NUMERAL0)).get

    //167 - Summarize (ST)
    val stORCAGroup01 = stORCASource.groupOperation(GROUP01)
    val stORCAGroupDealnAccountIDDF = GroupOperation.doGroup(stORCAUnionDF, stORCAGroup01.cols, stORCAGroup01.aggregations).get

    //168 - Summarize (STT)
    val sttORCAGroup02 = sttORCASource.groupOperation(GROUP02)
    val sttORCAGroupDealnAccountIDDF = GroupOperation.doGroup(sttORCAUnionDF, sttORCAGroup02.cols, sttORCAGroup02.aggregations).get

    //28 - Summarize
    val stORCAGroup02 = stORCASource.groupOperation(GROUP02)
    val stORCAGroupIDnAccountnWEDDF = GroupOperation.doGroup(stORCAUnionDF, stORCAGroup02.cols, stORCAGroup02.aggregations).get

    //27 - Summarize
    val stORCAGroup03 = stORCASource.groupOperation(GROUP03)
    val stORCAGroupDealnAccountnIDDF = GroupOperation.doGroup(stORCAFilterBigDealDF, stORCAGroup03.cols, stORCAGroup03.aggregations).get

    //29 - Join
    val stORCAJoin02 = stORCASource.joinOperation(JOIN02)
    val stORCAJoin02Map = JoinAndSelectOperation.doJoinAndSelect(stORCAGroupDealnAccountnIDDF, stORCAGroupIDnAccountnWEDDF, stORCAJoin02)
    val stORCAInnerJoin02DF = stORCAJoin02Map("inner")
    val stORCARightJoin02DF = stORCAJoin02Map("right")

    //169 - Union
    val stSttUnionDF = UnionOperation.doUnion(stORCAGroupDealnAccountIDDF, sttORCAGroupDealnAccountIDDF).get

    //170 - Unique
    val stSttUnionDistinctDF = stSttUnionDF.dropDuplicates(List("Big Deal Nbr","Account Company","Product Base ID"))

    //171 - Join
    val stORCAJoin03 = sttORCASource.joinOperation(JOIN03)
    val stORCAJoin03Map = JoinAndSelectOperation.doJoinAndSelect(stSttUnionDistinctDF, rawCalendarDistinctLeftJoinDF, stORCAJoin03)
    val stORCAJoin03DF = stORCAJoin03Map("right")

    //183 - Summarize
    val stORCAGroup04 = stORCASource.groupOperation(GROUP04)
    val stORCAGroupPartnerSKUWEDDF = GroupOperation.doGroup(stORCAJoin03DF, stORCAGroup04.cols, stORCAGroup04.aggregations).get

    //30 - Union
    val stORCAInnerRightJoinUnionDF = UnionOperation.doUnion(stORCAInnerJoin02DF, stORCARightJoin02DF).get

    //33 - Multi-field
    val stORCAJoinUnionCastDF = stORCAInnerRightJoinUnionDF.withColumn("Big Deal Qty",col("Big Deal Qty").cast(DoubleType))
      .withColumn("Qty", col("Qty").cast(DoubleType))

    //31 - Formula
    val stORCANonBigDealDF = stORCAJoinUnionCastDF.withColumn("Non Big Deal Qty",col("Qty")-col("Big Deal Qty"))

    //34 - Filter
    val sttORCAFilter01 = sttORCASource.filterOperation(FILTER01)
    val sttORCABigDealFilterDF = FilterOperation.doFilter(sttORCAUnionDF, sttORCAFilter01, sttORCAFilter01.conditionTypes(NUMERAL0)).get

    //35 - Summarize
    val sttORCAGroup03 = sttORCASource.groupOperation(GROUP03)
    val sttORCAGroup03DF = GroupOperation.doGroup(sttORCABigDealFilterDF,sttORCAGroup03.cols, sttORCAGroup03.aggregations).get

    //36 - Summarize
    val sttORCAGroup04 = sttORCASource.groupOperation(GROUP04)
    val sttORCAGroup04DF = GroupOperation.doGroup(sttORCAUnionDF,sttORCAGroup04.cols, sttORCAGroup04.aggregations).get

    //37 - Join
    val sttORCAJoin02 = sttORCASource.joinOperation(JOIN02)
    val sttORCAJoin02Map = JoinAndSelectOperation.doJoinAndSelect(sttORCAGroup03DF, sttORCAGroup04DF, sttORCAJoin02)
    val sttORCAInnerJoin02DF = sttORCAJoin02Map("inner")
    val sttORCARightJoin02DF = sttORCAJoin02Map("right")

    //38 - Union
    val sttORCAInnerRightJoinUnionDF = UnionOperation.doUnion(sttORCAInnerJoin02DF, sttORCARightJoin02DF).get

    //41 - Multi field
    val sttORCACastedDF = sttORCAInnerRightJoinUnionDF.withColumn("Big Deal Qty",col("Big Deal Qty").cast(DoubleType))
      .withColumn("Qty",col("Qty").cast(DoubleType))

    //39 - Formula
    val sttORCACastedNonBigDealDF = sttORCACastedDF.withColumn("Non Big Deal Qty",col("Qty")-col("Big Deal Qty"))

    //42 - Join
    val sttORCAJoin03 = sttORCASource.joinOperation(JOIN03)
    val sttORCAJoin03Map = JoinAndSelectOperation.doJoinAndSelect(stORCANonBigDealDF, sttORCACastedNonBigDealDF, sttORCAJoin03)
    val sttORCAInnerJoin03DF = sttORCAJoin03Map("inner")
    val sttORCALeftJoin03DF = sttORCAJoin03Map("left")
    val sttORCARightJoin03DF = sttORCAJoin03Map("right")

    //47 - Union
    val sttORCA03InnerLeftUnionDF = UnionOperation.doUnion(sttORCALeftJoin03DF, sttORCAInnerJoin03DF).get
    val sttORCA03JoinsUnionDF = UnionOperation.doUnion(sttORCA03InnerLeftUnionDF, sttORCARightJoin03DF).get

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
    val sttORCAAccountWEDProductGroupedDF = GroupOperation.doGroup(sttORCAJoinsUnionCastedDF, sttORCAGroup05.cols, sttORCAGroup05.aggregations).get

    //176 - Join
    val sttORCAJoin04 = sttORCASource.joinOperation(JOIN04)
    val sttORCAJoin04Map = JoinAndSelectOperation.doJoinAndSelect(sttORCAAccountWEDProductGroupedDF, stORCAGroupPartnerSKUWEDDF, sttORCAJoin04)
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
    val sttORCAUnionSTSTTFormulaAndJoinDF = UnionOperation.doUnion(sttORCALeftJoin04DF, sttORCAJoin04STnSTTDealQty).get

    //46 - Reseller Formula
    val sttORCAUnionWithResellerFeatureDF = sttORCAUnionSTSTTFormulaAndJoinDF.withColumn("Reseller",
      when(col("Account Company")==="PC Connection Sales And Services",lit("PC Connection"))
      .when(col("Account Company")==="CDW Logistics Inc",lit("CDW"))
      .when(col("Account Company")==="PCM Sales",lit("PCM"))
      .otherwise(col("Account Company")))

    //199 - Summarize
    val sttORCAwithDummyDF = sttORCAUnionDF.withColumn("dummy",lit(1))
    val sttORCAGroup01 = sttORCASource.groupOperation(GROUP01)
    val sttORCAGroupDF = GroupOperation.doGroup(sttORCAwithDummyDF, sttORCAGroup01.cols, sttORCAGroup01.aggregations)
      .get
      .drop("dummy")

    //213 - Formula
    val sttORCAValueDF = sttORCAGroupDF.withColumn("Value",lit(1))

    //212 - Cross Tab
    //TODO: Implement this
    val sttORCAPivotDF = sttORCAValueDF

    //211 - Formula
    val sttORCAOptionTypeDF = sttORCAPivotDF.withColumn("Option Type",
      when(col("All")===1,"All")
      .when((col("Option_B")===1)&&(col("Option_C")===1),"Option_C")
      .otherwise("Option_B"))

    //202 - Filter
    val sttORCAFilter02 = sttORCASource.filterOperation(FILTER02)
    val sttORCAFilterOptionTypeDF = FilterOperation.doFilter(sttORCAOptionTypeDF, sttORCAFilter02, sttORCAFilter02.conditionTypes(NUMERAL0)).get

    //49 - Select
    val auxWEDRename02 = auxWEDSource.renameOperation("rename02")
    val auxWEDSelect01Rename02DF = auxWEDSelectDF    //TODO: Rename using rename02

    //50 - Join
    val auxWEDJoin02 = auxWEDSource.joinOperation(JOIN02)
    val auxWEDJoinResellerMap = JoinAndSelectOperation.doJoinAndSelect(sttORCAUnionWithResellerFeatureDF, auxWEDSelect01Rename02DF, auxWEDJoin02)
    val auxWEDJoinResellerInnerJoinDF = auxWEDJoinResellerMap("inner")

    //53 - select
    val auxCommSelect01 = auxCommResellerSource.selectOperation(SELECT01)
    val auxCommSelectDF = SelectOperation.doSelect(auxCommResellerDF, auxCommSelect01.cols, auxCommSelect01.isUnknown).get

    //51 - Join
    val auxWEDJoin03 = auxWEDSource.joinOperation(JOIN03)
    val auxWEDCommResellerJoinMap = JoinAndSelectOperation.doJoinAndSelect(auxWEDJoinResellerInnerJoinDF, auxWEDSelect01Rename02DF, auxWEDJoin03)
    val auxWEDCommResellerLeftJoinDF = auxWEDCommResellerJoinMap("left")
    val auxWEDCommResellerInnerJoinDF = auxWEDCommResellerJoinMap("inner")

    //57 - Formula
    val auxWEDPromoOptionDF = auxWEDCommResellerLeftJoinDF.withColumn("Promo Option",lit("Option C"))

    //58 - Union
    val auxWEDCommResellerUnionDF = UnionOperation.doUnion(auxWEDPromoOptionDF, auxWEDCommResellerInnerJoinDF).get

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
    val auxWEDSKUJoin04Map = JoinAndSelectOperation.doJoinAndSelect(auxWEDCommResQtynBigDealQtynNonDealQtyDF, auxSKUHierDF, auxWEDJoin04)
    val auxWEDSKUInnerJoinDF = auxWEDSKUJoin04Map("inner")
    val auxWEDSKULeftJoinDF = auxWEDSKUJoin04Map("left")

    //65 - Group Product, Aggregate ST & STT Qty
    val auxWEDGroup01 = auxWEDSource.groupOperation(GROUP01)
    val auxWEDProductGroupDF = GroupOperation.doGroup(auxWEDSKULeftJoinDF, auxWEDGroup01.cols, auxWEDGroup01.aggregations).get

    //67 - Sort
    val auxWEDSort01 = auxWEDSource.sortOperation(SORT01)
    val auxWEDPRoductGroupSortedDF = SortOperation.doSort(auxWEDProductGroupDF, auxWEDSort01.ascending, auxWEDSort01.descending).get

    //201 - Join (SKU)
    val sttORCAJoin05 = sttORCASource.joinOperation(JOIN05)
    val sttORCAJoin05Map = JoinAndSelectOperation.doJoinAndSelect(auxWEDSKUInnerJoinDF, sttORCAFilterOptionTypeDF, sttORCAJoin05)
    val sttORCALeftJoin05DF = sttORCAJoin05Map("left")
    val sttORCAInnerJoin05DF = sttORCAJoin05Map("inner")

    //203 - Union
    val sttORCAInnerLeftJoinUnionDF = UnionOperation.doUnion(sttORCAInnerJoin05DF, sttORCALeftJoin05DF).get

    //197 - Formula (Reseller_Cluster)
    val sttORCAResellerClusterDF = sttORCAInnerLeftJoinUnionDF.withColumn("Reseller_Cluster",
      when((col("Option Type (SKU)")!=="Option C") && (col("Reseller_Cluster")==="Other - Option C"),"Other - Option B")
      .otherwise(col("Reseller_Cluster")))

    //70 - Multi field cast
    val sttORCAResellerClusterCastedDF = sttORCAResellerClusterDF.withColumn("Qty",col("Qty").cast(StringType))
      .withColumn("Big Deal Qty", col("Big Deal Qty").cast(StringType))
      .withColumn("Non Big Deal Qty", col("Non Big Deal Qty").cast(StringType))
    /* Used in 71 and 74*/

    //73 - Filter
    val auxWEDFilter01 = auxWEDSource.filterOperation(FILTER01)
    val auxWEDOptionTypFilteredDF = FilterOperation.doFilter(auxWEDSelect01Rename02DF, auxWEDFilter01, auxWEDFilter01.conditionTypes(NUMERAL0)).get
    val auxWEDFilter02 = auxWEDSource.filterOperation(FILTER02)
    val auxWEDOptionTypNotFilteredDF = FilterOperation.doFilter(auxWEDSelect01Rename02DF, auxWEDFilter02, auxWEDFilter02.conditionTypes(NUMERAL0)).get

    //71 - Join (SKU and Week_End_Date with filtered Option Typr)
    val auxWEDJoin05 = auxWEDSource.joinOperation(JOIN05)
    val auxWEDJoin05Map = JoinAndSelectOperation.doJoinAndSelect(sttORCAResellerClusterCastedDF, auxWEDOptionTypFilteredDF, auxWEDJoin05)
    val auxWEDLeftJoin05DF = auxWEDJoin05Map("left")
    val auxWEDInnerJoin05DF = auxWEDJoin05Map("inner")

    //74 - Join (SKU and Week_End_Date with unfiltered Option Type)
    val auxWEDJoin06 = auxWEDSource.joinOperation(JOIN06)
    val auxWEDJoin06Map = JoinAndSelectOperation.doJoinAndSelect(sttORCAResellerClusterCastedDF, auxWEDOptionTypNotFilteredDF, auxWEDJoin06)
    val auxWEDLeftJoin06DF = auxWEDJoin06Map("left")
    val auxWEDInnerJoin06DF = auxWEDJoin06Map("inner")

    //87 - Union
    val auxWED05JoinsUnionDF = UnionOperation.doUnion(auxWEDLeftJoin05DF, auxWEDInnerJoin05DF).get

    //86 - Union
    val auxWED06JoinsUnionDF = UnionOperation.doUnion(auxWEDLeftJoin06DF, auxWEDInnerJoin06DF).get
      .withColumnRenamed("IR","Option IR")

    //88 - Join (SKU, Reseller, Week_End_Date Unions of filtered and unfiltered joins)
    val auxWEDJoin07 = auxWEDSource.joinOperation(JOIN07)
    val auxWEDJoin07Map = JoinAndSelectOperation.doJoinAndSelect(auxWED05JoinsUnionDF, auxWED06JoinsUnionDF, auxWEDJoin07)
    val auxWEDInnerJoin07DF = auxWEDJoin07Map("inner")
      .withColumnRenamed("IR","All IR")
      .withColumnRenamed("First_C2B Promo Code","Right_First_C2B Promo Code")

    //114
    val auxEtailerSelect01 = auxEtailerSource.selectOperation(SELECT01)
    val auxEtailerSelectedDF = SelectOperation.doSelect(auxEtailerDF, auxEtailerSelect01.cols, auxEtailerSelect01.isUnknown).get

    //83
    val ciORCASelect01 = ciORCASource.selectOperation(SELECT01)
    val ciORCASelectedDF = SelectOperation.doSelect(ciORCADF, ciORCASelect01.cols, ciORCASelect01.isUnknown).get

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
    val ciORCAJoin01Map = JoinAndSelectOperation.doJoinAndSelect(ciORCAProdIDDF, auxWEDSelect01Rename02DF, ciORCAJoin01)
    val ciORCAauxWEDJoinDF = ciORCAJoin01Map("inner")

    //91 - Formula Beg_Inventory
    val ciORCABegInvFeatureDF = ciORCAauxWEDJoinDF.withColumn("Beg_Inventory",addDaystoDateStringUDF(col("Week_End_Date"),lit(7)))

    //94 - Summarize
    val ciORCAGroup01 = ciORCASource.groupOperation(GROUP01)
    val ciORCABegGroupDF = GroupOperation.doGroup(ciORCABegInvFeatureDF, ciORCAGroup01.cols, ciORCAGroup01.aggregations).get

    //93     /* 331 not implemented coz used for browse*/
    val auxWEDIRInvClusterPromoIDDF = auxWEDInnerJoin07DF.withColumn("IR",
        when((col("All IR").isNull) && (col("Option IR").isNull),0)
        .otherwise(findMaxBetweenTwo(col("All IR"),col("Option IR"))))
      .withColumn("Inventory Cluster", when((col("Reseller_Cluster")==="Other - Option B") || (col("Reseller_Cluster")=="Other - Option C"), lit("Distributor"))
          .otherwise(col("Reseller_Cluster")))
      .withColumn("Promo ID", findMaxBetweenTwo(col("Promo ID"),col("Right_First_C2B Promo Code")))

    //96 - Join input from 94
    /*292 not implemented*/
    val ciORCAJoin02 = ciORCASource.joinOperation(JOIN02)
    val ciORCAJoin02Map = JoinAndSelectOperation.doJoinAndSelect(auxWEDIRInvClusterPromoIDDF, ciORCABegGroupDF, ciORCAJoin02)
    val ciORCAInnerJoin02DF = ciORCAJoin02Map("inner")
    val ciORCALeftJoin02DF = ciORCAJoin02Map("left")

    //100
    val ciORCAJoin02UnionDF = UnionOperation.doUnion(ciORCAInnerJoin02DF, ciORCALeftJoin02DF).get

    //102 - Select
    val ciORCASelect02 = ciORCASource.selectOperation(SELECT02)
    val ciORCAUnionSelectDF = SelectOperation.doSelect(ciORCAJoin02UnionDF, ciORCASelect02.cols, ciORCASelect02.isUnknown).get

    //115 - Join
    val ciORCAJoin03 = ciORCASource.joinOperation(JOIN03)
    val ciORCAJoin03Map = JoinAndSelectOperation.doJoinAndSelect(ciORCAUnionSelectDF, auxEtailerSelectedDF, ciORCAJoin03)
    val ciORCAInnerJoin03DF = ciORCAJoin03Map("inner")
    val ciORCALeftJoin03DF = ciORCAJoin03Map("left")

    //117 - Add eTailer
    val ciORCALeftJoineTailerFeatureDF = ciORCALeftJoin03DF.withColumn("eTailer",lit(0))

    //116 - Union
    val ciORCAJoins03UnionDF = UnionOperation.doUnion(ciORCALeftJoineTailerFeatureDF, ciORCAInnerJoin03DF).get

    //104 - Join
    /*216 not implemented*/
    val ciORCAJoin04 = ciORCASource.joinOperation(JOIN04)
    val ciORCAJoin04Map = JoinAndSelectOperation.doJoinAndSelect(ciORCAJoins03UnionDF, auxWEDPRoductGroupSortedDF, ciORCAJoin04)
    val ciORCAInnerJoin04DF = ciORCAJoin04Map("inner")
    val ciORCALeftJoin04DF = ciORCAJoin04Map("left")
    //110 /*Need not implement for now - browse*/

    //227 - Union
    val ciORCAJoins04UnionDF = UnionOperation.doUnion(ciORCAInnerJoin04DF, ciORCALeftJoin04DF).get

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
    val ciORCAGroup02DF = GroupOperation.doGroup(ciORCAInvQtySpendPromoFlgSeasonBigDealNonBigDealDF, ciORCAGroup02.cols, ciORCAGroup02.aggregations).get

    //103
    val ciORCASort01 = ciORCASource.sortOperation(SORT01)
    val ciORCAFeaturesSortedDF = SortOperation.doSort(ciORCAGroup02DF, ciORCASort01.ascending, ciORCASort01.descending).get

    //322 - Read new source
    val histPOSDF = Utils.loadCSV(executionContext, histPOSSource.filePath).get.cache()

    //327 - Convert wed to dateFormat
    val histPOSDateFormattedDF = histPOSDF.withColumn("Week_End_Date", convertDatetoFormat(col("wed"),lit("mm-DD-yyyy"),lit("MM/dd/yyyy")))
      .drop("wed")

    //323 - Select
    val histPOSSelect01 = histPOSSource.selectOperation(SELECT01)
    val histPOSSelectDF = SelectOperation.doSelect(histPOSDateFormattedDF, histPOSSelect01.cols, histPOSSelect01.isUnknown).get

    //324 - Filter channel
    val histPOSFilter01 = histPOSSource.filterOperation(FILTER01)
    val histPOSFilterChannelDF = FilterOperation.doFilter(histPOSSelectDF, histPOSFilter01, histPOSFilter01.conditionTypes(NUMERAL0)).get

    //326 - Group
    val hisPOSGroup01 = histPOSSource.groupOperation(GROUP01)
    val histPOSGroupedDF = GroupOperation.doGroup(histPOSFilterChannelDF, hisPOSGroup01.cols, hisPOSGroup01.aggregations).get
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
    val histPOSJoinSKUHierMap = JoinAndSelectOperation.doJoinAndSelect(histPOSNewVariablesDF, auxSKUHierDF, histPOSJoin01)
    val histPOSInnerJoinSKUDF = histPOSJoinSKUHierMap("inner")

    //329 - Union
    val histPOSUnionCIDF = UnionOperation(histPOSInnerJoinSKUDF, ciORCAFeaturesSortedDF)

    //229 - Select
    /********  Main One  *******/
    val histPOSSelect02 = histPOSSource.selectOperation(SELECT02)
    val histPOSSelect02DF = SelectOperation.doSelect(histPOSUnionCIDF, histPOSSelect02.cols, histPOSSelect02.isUnknown).get

    //221 /* Need not do it right now - browse*/
    val histPOSSelectUniqueDF = histPOSSelect02DF.dropDuplicates(List("IR","Reseller","Week_End_Date","SKU"))

    //270 - Workflow run date
    /********  Main One  *******/
    val histPOSWithRunDateDF = histPOSSelectUniqueDF.withColumn("Workflow Run Date", lit(LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss"))))

    //278 - Select
    val histPOSSelect03 = histPOSSource.selectOperation(SELECT03)
    val histPOSSelect03DF = SelectOperation.doSelect(histPOSSelect02DF, histPOSSelect03.cols, histPOSSelect03.isUnknown).get

    //242 - Save posqty_output_commercial.csv
    histPOSSelect02DF.show()
    //TODO: histPOSSelect02DF - writeCSV utility

    //269 - Save posqty_output_commercial_with_date.csv
    histPOSWithRunDateDF.show()
    //TODO: writeCSV utility

    //264 - 267  --> Reading last written posqty_Commercial output.csv

    //262 - Select
    val archivePOSSelect01 = archivePOSSource.selectOperation(SELECT01)
    val archivePOS = SelectOperation.doSelect(archivePOSDF, archivePOSSelect01.cols, archivePOSSelect01.isUnknown).get

    //281 - Join
    val archivePOSJoin01 = archivePOSSource.joinOperation(JOIN01)
    val archivePOSJoin01Map = JoinAndSelectOperation.doJoinAndSelect(archivePOS, histPOSSelect03DF, archivePOSJoin01)
    val arhivePOSInnerJoin01DF = archivePOSJoin01Map("inner")
    val arhivePOSLeftJoin01DF = archivePOSJoin01Map("left")
    val arhivePOSRightJoin01DF = archivePOSJoin01Map("right")

    //282 - Select
    val archivePOSSelect02 = archivePOSSource.selectOperation(SELECT02)
    val archivePOSRightJoinSelectDF = SelectOperation.doSelect(arhivePOSRightJoin01DF, archivePOSSelect02.cols, archivePOSSelect02.isUnknown).get

    //283 - Union
    val archivePOSJoinsUnionDF = UnionOperation.doUnion(UnionOperation.doUnion(arhivePOSInnerJoin01DF, arhivePOSLeftJoin01DF).get, arhivePOSRightJoin01DF).get

    //279 - Select
    val archivePOSSelect03 = archivePOSSource.selectOperation(SELECT03)
    val archivePOSSelect03DF = SelectOperation.doSelect(archivePOSJoinsUnionDF, archivePOSSelect03.cols, archivePOSSelect03.isUnknown).get

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
    val archivePOSGroup01 = archivePOSSource.groupOperation(GROUP01)
    val archivePOSGroup01DF = GroupOperation.doGroup(archivePOSNewVarsDF.withColumn("dummy",lit(1)), archivePOSGroup01.cols, archivePOSGroup01.aggregations).get.drop("dummy").drop("sum_dummy")

    //295 - Filter Week_End_Date
    val archivePOSFilter01 = archivePOSSource.filterOperation(FILTER01)
    val archivePOSFilterWEDDF = FilterOperation.doFilter(archivePOSGroup01DF, archivePOSFilter01, archivePOSFilter01.conditionTypes(NUMERAL0)).get

    //294 - Write QA File
    archivePOSFilterWEDDF.show()
    //TODO: use writeCSV utility here

    //302 - summarize (Add new column max of Week_End_Date) & 303 - Append
    val histPOSSelect03MaxWEDDF = histPOSSelect03DF.withColumn("Max_Week_End_Date", max(col("Week_End_date")))

    //299 - Select : NOT REQUIRED as all fields getting selected

    //298 - Filter
    val histPOSFilter02 = histPOSSource.filterOperation(FILTER02)
    val histPOSFilteredSeasonDF = FilterOperation.doFilter(histPOSSelect03MaxWEDDF, histPOSFilter02, histPOSFilter02.conditionTypes(NUMERAL0)).get

    //297 - Summarize
    val histPOSGroup02 = histPOSSource.groupOperation(GROUP02)
    val histPOSGroup02DF = GroupOperation.doGroup(histPOSFilteredSeasonDF, histPOSGroup02.cols, histPOSGroup02.aggregations).get

    //300 - Pivot
    val histPOSPivotDF = histPOSGroup02DF.groupBy("Reseller_Cluster").pivot("Week_End_Date").agg(first("Sum_Qty"))

    //304  - Save
    histPOSPivotDF.show()
    //TODO: save as Commercial qty check.csv using writeCSV

    //311 - 377 same as 302 - 298

    //306 - Summarize
    val histPOSGroup03 = histPOSSource.groupOperation(GROUP03)
    val histPOSGroup03DF = GroupOperation.doGroup(histPOSFilteredSeasonDF, histPOSGroup03.cols, histPOSGroup03.aggregations).get

    //309 - Cross tab Pivot
    val histPOSPivot02DF = histPOSGroup03DF.groupBy("Reseller_Cluster").pivot("Week_End_Date").agg(first("Sum_Inv_Qty"))

    //313 - Save
    histPOSPivot02DF.show()
    //TODO: histPOSPivot02DF write this using writeCSV

    //314 - 317: Same as 302 - 298
    val histPOSGroup04 = histPOSSource.groupOperation(GROUP04)
    val histPOSGroup04DF = GroupOperation.doGroup(histPOSFilteredSeasonDF, histPOSGroup04.cols, histPOSGroup04.aggregations).get

    //319 - add diff%
    val histPOSDiffPerDF = histPOSGroup04DF.withColumn("diff%", (col("Sum_Qty Diff")/col("Sum_Qty"))*100)

    //321 - Save
    histPOSDiffPerDF.show()
    //TODO: Save this as Commercial_POS_old_vs_new.csv
  }
}

