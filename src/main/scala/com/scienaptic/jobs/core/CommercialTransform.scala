package com.scienaptic.jobs.core

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.bean.{UnionOperation, _}
import com.scienaptic.jobs.utility.Utils
import com.scienaptic.jobs.utility.CommercialUtility._
//import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window



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

    /* Load all sources */
    val iecDF = Utils.loadCSV(executionContext, iecSource.filePath).get.cache()
    val xsClaimsDF = Utils.loadCSV(executionContext, xsClaimsSource.filePath).get.cache()
    val rawCalendarDF = Utils.loadCSV(executionContext, rawCalendarSource.filePath).get.cache()
    val auxWEDDF = Utils.loadCSV(executionContext, auxWEDSource.filePath).get.cache()
    val auxSKUHierDF = Utils.loadCSV(executionContext, auxSkuHierarchySource.filePath).get.cache()
    val stORCADF = Utils.loadCSV(executionContext, stORCASource.filePath).get.cache()
    val sttORCADF = Utils.loadCSV(executionContext, sttORCASource.filePath).get.cache()

    val auxWEDSelect01 = auxWEDSource.selectOperation(SELECT01)
    val auxWEDSelectDF = SelectOperation.doSelect(auxWEDDF, auxWEDSelect01.cols, auxWEDSelect01.isUnknown).get

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

    //19
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


    //170 - Unique


    //30 - Union
    //val stORCAInnerRightJoinUnionDF =


    //171 - Join


    //33 - Multi-field


    //31 - Formula


    //183 - Summarize


    //

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
    //TODO: implement in utility format
    val sttORCAFilterOptionTypeDF = sttORCAOptionTypeDF.where(col("Option Type")===lit("Option C"))

    //50 - Join


    //201 - Join

  }
}
