package com.scienaptic.jobs.core

import java.util.Date

import com.scienaptic.jobs.{ExecutionContext, bean}
import com.scienaptic.jobs.bean._
import com.scienaptic.jobs.utility.Utils
import com.scienaptic.jobs.utility.CommercialUtility._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


object CommercialTransform {

  val SELECT01="select01"; val SELECT02="select02"; val SELECT03="select03"; val SELECT04="select04"; val SELECT05="select05"; val SELECT06="select06";
  val FILTER01="filter01"; val FILTER02="filter02"; val FILTER03="filter03"; val FILTER04="filter04"; val FILTER05="filter05"; val FILTER06="filter06";
  val JOIN01="join01"; val JOIN02="join02"; val JOIN03="join03"; val JOIN04="join04"; val JOIN05="join05"; val JOIN06="join06"; val JOIN07="join07";
  val NUMERAL0=0; val NUMERAL1=1;
  val GROUP01="group01"; val GROUP02="group02"; val GROUP03="group03";val GROUP04="group04";

  val IECSOURCE="IEC_CLAIMS"; val XSCLAIMSSOURCE="XS_CLAIMS"; val RAWCALENDARSOURCE="RAW_MASTER_CALENDAR";
  val AUXWEDSOURCE="AUX_WEEK_END_DATE"; val AUXSKUHIERARCHY="AUX_SKU_HIERARCHY";


  def execute(executionContext: ExecutionContext): Unit = {
    /* Source Map with all sources' information */
    val sourceMap = executionContext.configuration.sources

    /* Map with all operations for IEC_Claims source operations */
    val iecSource = sourceMap(IECSOURCE)
    val xsClaimsSource = sourceMap(XSCLAIMSSOURCE)
    val rawCalendarSource = sourceMap(RAWCALENDARSOURCE)
    val auxWEDSource = sourceMap(AUXWEDSOURCE)
    val auxSkuHierarchySource = sourceMap(AUXSKUHIERARCHY)

    /* Load all sources */
    val iecDF = Utils.loadCSV(executionContext, iecSource.filePath).get.cache()
    val xsClaimsDF = Utils.loadCSV(executionContext, xsClaimsSource.filePath).get.cache()
    val rawCalendarDF = Utils.loadCSV(executionContext, rawCalendarSource.filePath).get.cache()
    val auxWEDDF = Utils.loadCSV(executionContext, auxWEDSource.filePath).get.cache()
    val auxSKUHierDF = Utils.loadCSV(executionContext, auxSkuHierarchySource.filePath).get.cache()

    val auxWEDSelect01 = auxWEDSource.selectOperation(SELECT01)
    val auxWEDSelectDF = SelectOperation.doSelect(auxWEDDF, auxWEDSelect01.cols).get

    val iecSelect01 = iecSource.selectOperation(SELECT01)
    val iecSelectDF = SelectOperation.doSelect(iecDF, iecSelect01.cols).get

    val iecClaimFilter01 = iecSource.filterOperation(FILTER01)
    val iecFiltered01DF = FilterOperation.doFilter(iecSelectDF, iecClaimFilter01.conditions, iecClaimFilter01.conditionTypes(NUMERAL0))

    val xsClaimsSelect01 = xsClaimsSource.selectOperation(SELECT01)
    val xsClaimsSelect01DF = SelectOperation.doSelect(xsClaimsDF, xsClaimsSelect01.cols).get

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
      .withColumnRenamed("Sum_Claim Quantity","Claim Quantity")

    val rawCalendarSelect01= rawCalendarSource.selectOperation(SELECT01)
    var rawCalendarSelectDF = SelectOperation.doSelect(rawCalendarDF, rawCalendarSelect01.cols).get
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
    val rawXSJoinOutsidePromoTrueDF = FilterOperation.doFilter(rawXSJoinChkOutsidePromoDF, rawCalendarFilter01.conditions, rawCalendarFilter01.conditionTypes(NUMERAL0)).get
    //134
    val rawXSJoinOutsidePromoFalseDF = FilterOperation.doFilter(rawXSJoinChkOutsidePromoDF, rawCalendarFilter02.conditions, rawCalendarFilter02.conditionTypes(NUMERAL0)).get

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
    val rawCalendarFilterIncldeDF = FilterOperation.doFilter(rawCalendarIncludeVarDF,rawCalendarFilter03.conditions,rawCalendarFilter03.conditionTypes(0)).get

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

    //155 & 157 - Formula
    val rawCalendarEndDateChangeDF = rawCalendarEndDateUnionDF.withColumn("End Date Change",
      when(col("End Date")===col("New End Date"),"N")
          .otherwise("Y")).distinct()

    //158 - Append (Outer join)
    // MAJOR MAJOR DOUBT!!!!
    //TODO: Check how it is doing catesian join
    val rawCalendarJoin04= rawCalendarSource.joinOperation(JOIN04)
    val rawCalendarnAuxWEDAppendMap = JoinAndSelectOperation.doJoinAndSelect(rawCalendarEndDateChangeDF, auxWEDSelectDF, rawCalendarJoin04)
    val rawCalendarnAuxWEDAppendDF = rawCalendarnAuxWEDAppendMap("outer")

    //159 - Formula
    val rawCalStartsubWEDDF = rawCalendarnAuxWEDAppendDF.withColumn("Start - Week End", dateTimeDiff(col("Start Date"),col("Week.End.Date")))
      .withColumn("Week End - End",dateTimeDiff(col("Week.End.Date"),col("New End Date")))
      .withColumn("Option Type",
        when(col("Promo Name").contains("Option B"),"Option B")
        .when(col("Promo Name").contains("Option C"),"Option C")
        .otherwise("All"))

    //160 - Filter
    val rawCalendarAUXWEDAppendDF = rawCalStartsubWEDDF.join(auxWEDDF)







  }
}
