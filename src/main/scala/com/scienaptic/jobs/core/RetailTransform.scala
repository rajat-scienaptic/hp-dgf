package com.scienaptic.jobs.core

import java.text.SimpleDateFormat

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.bean._
import com.scienaptic.jobs.utility.Utils
import org.apache.spark.sql.{Encoders, SaveMode}
import org.apache.spark.sql.functions._

// TODO : FileNotFound Exception
//TODO: Check if argument lists empty. Don't call utility if empty!
//TODO: Implement Logger
//TODO: Implement Custom Exceptions
//TODO: GROUP with Select

object RetailTransform {

  val INNER_JOIN = "inner"
  val LEFT_JOIN = "leftanti"
  val RIGHT_JOIN = "rightanti"
  val SELECT01 = "select01"
  val SELECT02 = "select02"
  val SELECT03 = "select03"
  val SELECT04 = "select04"
  val SELECT05 = "select05"
  val SELECT06 = "select06"
  val FILTER01 = "filter01"
  val FILTER02 = "filter02"
  val FILTER03 = "filter03"
  val FILTER04 = "filter04"
  val FILTER05 = "filter05"
  val FILTER06 = "filter06"
  val JOIN01 = "join01"
  val JOIN02 = "join02"
  val JOIN03 = "join03"
  val JOIN04 = "join04"
  val JOIN05 = "join05"
  val JOIN06 = "join06"
  val JOIN07 = "join07"
  val NUMERAL0 = 0
  val NUMERAL1 = 1
  val GROUP01 = "group01"
  val GROUP02 = "group02"
  val GROUP03 = "group03"
  val GROUP04 = "group04"
  val GROUP05 = "group05"
  val RENAME01 = "rename01"
  val RENAME02 = "rename02"
  val RENAME03 = "rename03"
  val RENAME04 = "rename04"
  val RENAME05 = "rename05"
  val RENAME06 = "rename06"
  val SORT01 = "sort01"
  val SORT02 = "sort02"
  val SORT03 = "sort03"
  val SORT04 = "sort04"


  val ODOM_ONLINE_ORCA_SOURCE = "ODOM_ONLINE_ORCA"
  val STAPLES_COM_UNITS_SOURCE = "STAPLES_COM_UNITS"
  val HP_COM_SOURCE = "HP_COM"
  val AMAZON_ARAP_SOURCE = "AMAZON_ARAP"
  val AMAZON_ASIN_MAP_SOURCE = "AMAZON_ASIN_MAP"
  val S_PRINT_HISTORICAL_UNITS_SOURCE = "S_PRINT_HISTORICAL_UNITS"
  val ORCA_2014_16_ARCHIVE_SOURCE = "ORCA_2014_16_ARCHIVE"
  val ORCA_QRY_2017_TO_DATE_SOURCE = "ORCA_QRY_2017_TO_DATE"
  val AUX_TABLES_WEEKEND_SOURCE = "AUX_TABLES_WEEKEND"
  val AUX_TABLES_ONLINE_SOURCE = "AUX_TABLES_ONLINE"
  val AUX_TABLES_SKU_HIERARCHY_SOURCE = "AUX_TABLES_SKU_HIERARCHY"
  val BBY_BUNDLE_INFO_SOURCE = "BBY_BUNDLE_INFO"
  val EXISTING_POS_SOURCE = "EXISTING_POS"


  def execute(executionContext: ExecutionContext): Unit = {
    /* Source Map with all sources' information */
    val sourceMap = executionContext.configuration.sources

    /* Map with all operations source operations */
    val odomOrcaSource = sourceMap(ODOM_ONLINE_ORCA_SOURCE)
    val staplesComUnitsSource = sourceMap(STAPLES_COM_UNITS_SOURCE)
    val hpComSource = sourceMap(HP_COM_SOURCE)
    val amazonArapSource = sourceMap(AMAZON_ARAP_SOURCE)
    val amazonAsinMapSource = sourceMap(AMAZON_ASIN_MAP_SOURCE)
    val sPrintHistoricalUnitsSource = sourceMap(S_PRINT_HISTORICAL_UNITS_SOURCE)
    val orca201416ArchiveSource = sourceMap(ORCA_2014_16_ARCHIVE_SOURCE)
    val orcaQry2017ToDateSource = sourceMap(ORCA_QRY_2017_TO_DATE_SOURCE)
    val auxTablesWeekendSource = sourceMap(AUX_TABLES_WEEKEND_SOURCE)
    val auxTablesOnlineSource = sourceMap(AUX_TABLES_ONLINE_SOURCE)
    val auxTablesSKUHierarchySource = sourceMap(AUX_TABLES_SKU_HIERARCHY_SOURCE)
    val bbyBundleInfoSource = sourceMap(BBY_BUNDLE_INFO_SOURCE)
    val existingPOSSource = sourceMap(EXISTING_POS_SOURCE)


    val odomOrcaDF = Utils.loadCSV(executionContext, odomOrcaSource.filePath).get
    val staplesDotComUnitsDF = Utils.loadCSV(executionContext, staplesComUnitsSource.filePath).get
    val hpComDF = Utils.loadCSV(executionContext, hpComSource.filePath).get
    val amazonArapDF = Utils.loadCSV(executionContext, amazonArapSource.filePath).get
    val amazonAsinMapDF = Utils.loadCSV(executionContext, amazonAsinMapSource.filePath).get
    val sPrintHistoricalUnitsDF = Utils.loadCSV(executionContext, sPrintHistoricalUnitsSource.filePath).get
    val orca201416ARchive = Utils.loadCSV(executionContext, orca201416ArchiveSource.filePath).get
    val orcaQry2017ToDate = Utils.loadCSV(executionContext, orcaQry2017ToDateSource.filePath).get
    val auxTablesWeekend = Utils.loadCSV(executionContext, auxTablesWeekendSource.filePath).get
    val auxTablesOnline = Utils.loadCSV(executionContext, auxTablesOnlineSource.filePath).get
    val auxTablesSKUHierarchy = Utils.loadCSV(executionContext, auxTablesSKUHierarchySource.filePath).get
    val bbyBundleInfo = Utils.loadCSV(executionContext, bbyBundleInfoSource.filePath).get
    val existingPOS = Utils.loadCSV(executionContext, existingPOSSource.filePath).get

    /* AUX TABLES WEEKEND */
    val auxTablesWeekendselect01DF = SelectOperation.doSelect(auxTablesWeekend, auxTablesWeekendSource.selectOperation(SELECT01).cols, auxTablesWeekendSource.selectOperation(SELECT01).isUnknown).get
      .withColumn("wed", to_date(unix_timestamp(col("wed"), "dd/MM/yyyy").cast("timestamp"), "yyyy-MM-dd"))

    /* ODOOM ORCA */
    // Select01
    val odomOrcaselect01DF = SelectOperation.doSelect(odomOrcaDF, odomOrcaSource.selectOperation(SELECT01).cols,
      odomOrcaSource.selectOperation(SELECT01).isUnknown).get

    // formula
    val odomOrcaSubsStrFormalaDF = odomOrcaselect01DF.withColumn("Base SKU", substring(col("Vendor Product Code"),
      0, 6)).withColumn("Account Major", lit("Office Depot-Max"))

    // group
    val odomOrcaGroup01 = odomOrcaSource.groupOperation(GROUP01)
    val odomOrcaGroup01DF = GroupOperation.doGroup(odomOrcaSubsStrFormalaDF, odomOrcaGroup01).get

    // joins
    val odomOrcaJoin01 = odomOrcaSource.joinOperation(JOIN01)
    val odomOrcaJoin01Map = JoinAndSelectOperation.doJoinAndSelect(odomOrcaGroup01DF, auxTablesWeekendselect01DF, odomOrcaJoin01)
    val odomOrcaJoin01InnerDF = odomOrcaJoin01Map(INNER_JOIN).cache()

    // group
    val odomOrcaGroup02 = odomOrcaSource.groupOperation(GROUP02)
    val odomOrcaGroup02DF = GroupOperation.doGroup(odomOrcaJoin01InnerDF, odomOrcaGroup02).get

    // filter
    val odomOrcaFilter01 = odomOrcaSource.filterOperation(FILTER01)
    val odomOrcaWedGreaterThanFIxedDF = FilterOperation.doFilter(odomOrcaGroup02DF, odomOrcaFilter01, odomOrcaFilter01.conditionTypes(NUMERAL0)).get

    // sort
    val odomOrcaSort01 = odomOrcaSource.sortOperation(SORT01)
    val odomOrcaWedSortDescDF = SortOperation.doSort(odomOrcaWedGreaterThanFIxedDF, odomOrcaSort01.ascending, odomOrcaSort01.descending).get

    /* STAPLESDOTCOM UNITS */
    // Select01
    val staplesComUnitsSelect01DF = Utils.convertListToDFColumnWithRename(staplesComUnitsSource.renameOperation(RENAME01),
      SelectOperation.doSelect(staplesDotComUnitsDF, staplesComUnitsSource.selectOperation(SELECT01).cols, staplesComUnitsSource.selectOperation(SELECT01).isUnknown).get)

    // formula
    val staplesComUnitsFormula01DF = Utils.litColumn(staplesComUnitsSelect01DF, "Account Major", "Staples")

    // union
    val staplesComUnitsFormulaLitNullDF = staplesComUnitsFormula01DF.withColumn("season_ordered", lit(null: String))
      .withColumn("cal_month", lit(null: String))
      .withColumn("cal_year", lit(null: String))
      .withColumn("fiscal_quarter", lit(null: String))
      .withColumn("fiscal_year", lit(null: String))
    val staplesComUnitsUnionDF = UnionOperation.doUnion(odomOrcaJoin01InnerDF, staplesComUnitsFormulaLitNullDF).get

    /* Orca 2014 16 Archive */
    // select
    val orca201416ArchiveSelect01 = orca201416ArchiveSource.selectOperation(SELECT01)
    val orca201416ArchiveSelectDF = SelectOperation.doSelect(orca201416ARchive, orca201416ArchiveSelect01.cols, orca201416ArchiveSelect01.isUnknown).get

    /* Orca Qry 2017 to date */
    // select
    val orcaQry2017ToDateSelect01 = orcaQry2017ToDateSource.selectOperation(SELECT01)
    val orcaQry2017ToDateSelectDF = Utils.convertListToDFColumnWithRename(orcaQry2017ToDateSource.renameOperation(RENAME01), SelectOperation.doSelect(orcaQry2017ToDate, orcaQry2017ToDateSelect01.cols, orcaQry2017ToDateSelect01.isUnknown).get)

    // union
    val unionOrca201617AndOrca2017QryToDate = UnionOperation.doUnion(orca201416ArchiveSelectDF, orcaQry2017ToDateSelectDF).get.cache()

    // group
    val orca201617And2017QryGroup01 = orcaQry2017ToDateSource.groupOperation(GROUP01)
    val orca201617And2017QryGroup01DF = GroupOperation.doGroup(unionOrca201617AndOrca2017QryToDate, orca201617And2017QryGroup01).get

    // sort
    val orca201617And2017QrySort01 = orcaQry2017ToDateSource.sortOperation(SORT01)
    val orca201617And2017QrySort01DF = SortOperation.doSort(orca201617And2017QryGroup01DF, orca201617And2017QrySort01.ascending, orca201617And2017QrySort01.descending)
    // browse here

    // formula continued
    val orca201617And2017QryFormulaDF = unionOrca201617AndOrca2017QryToDate.withColumn("Account Major Consol",
      when(col("Account Major") === "Office Depot Inc", "Office Depot-Max")
        .when(col("Account Major") === "OfficeMax North America, Inc.", "Office Depot-Max")
        .when(col("Account Major") === "OfficeMax Inc", "Office Depot-Max")
        .when(col("Account Major") === "Office Depot Inc (Contract Stationers)", "Office Depot-Max")
        .when(col("Account Major") === "Amazon.com, Inc.", "Amazon.Com")
        .when(col("Account Major") === "Frys Electronics Inc", "Fry's Electronics Inc")
        .when(col("Account Major") === "Sams Club", "Fry's Electronics Inc")
        .when(col("Account Major") === "Target Corporation", "Target Stores")
        .when(col("Account Major") === "Wal Mart Online", "Wal-Mart Online")
        .otherwise(col("Account Major")))


    // join
    val orca201617And2017QryAndAuxTablesJoin01 = orcaQry2017ToDateSource.joinOperation(JOIN01)
    val orca201617And2017QryAndAuxTablesJoin01Map = JoinAndSelectOperation.doJoinAndSelect(orca201617And2017QryFormulaDF, auxTablesWeekendselect01DF, orca201617And2017QryAndAuxTablesJoin01)
    val orca201617And2017QryAndAuxTablesInnerJoin01 = orca201617And2017QryAndAuxTablesJoin01Map(INNER_JOIN).withColumn("wed", to_date(unix_timestamp(col("wed"), "dd/MM/yyyy").cast("timestamp"), "yyyy-MM-dd"))

    // browse here

    // filter
    val orca201617And2017QryFilter01IfFalse = orcaQry2017ToDateSource.filterOperation(FILTER01)
    val oorca201617And2017QryFilter01IfFalseDF = FilterOperation.doFilter(orca201617And2017QryAndAuxTablesInnerJoin01, orca201617And2017QryFilter01IfFalse, orca201617And2017QryFilter01IfFalse.conditionTypes(NUMERAL0)).get

    // filter
    val orca201617And2017QryFilter02IfTrue = orcaQry2017ToDateSource.filterOperation(FILTER02)
    val orca201617And2017QryFilter02IfTrueDF = FilterOperation.doFilter(oorca201617And2017QryFilter01IfFalseDF, orca201617And2017QryFilter02IfTrue, orca201617And2017QryFilter02IfTrue.conditionTypes(NUMERAL0)).get

    // formula
    val orca201617And2017Qry7DaysLessFormula = orca201617And2017QryFilter02IfTrueDF.withColumn("wed", date_sub(to_date(unix_timestamp(col("wed"), "dd-MM-yyyy").cast("timestamp"), "yyyy-MM-dd"), 7))

    // filter
    val orca201617And2017QryFilter03IfFalse = orcaQry2017ToDateSource.filterOperation(FILTER03)
    val orca201617And2017QryFilter03IfFalseDF = FilterOperation.doFilter(oorca201617And2017QryFilter01IfFalseDF, orca201617And2017QryFilter03IfFalse, orca201617And2017QryFilter03IfFalse.conditionTypes(NUMERAL0)).get

    // union
    val orca201617And2017QryUnion01DF = UnionOperation.doUnion(orca201617And2017Qry7DaysLessFormula, orca201617And2017QryFilter03IfFalseDF).get

    // join
    val orca201617And2017QryAndAuxTablesJoin02 = orcaQry2017ToDateSource.joinOperation(JOIN02)
    val orca201617And2017QryAndAuxTablesJoin02Map = JoinAndSelectOperation.doJoinAndSelect(orca201617And2017QryUnion01DF, auxTablesWeekendselect01DF.withColumnRenamed("wed", "Right_wed"), orca201617And2017QryAndAuxTablesJoin02)
    val orca201617And2017QryAndAuxTablesInnerJoin02 = orca201617And2017QryAndAuxTablesJoin02Map(INNER_JOIN)

    /* Aux Tables Online */
    // select
    val auxTablesOnlineSelect01 = auxTablesOnlineSource.selectOperation(SELECT01)
    val auxTablesOnlineSelect01DF = SelectOperation.doSelect(auxTablesOnline, auxTablesOnlineSelect01.cols, auxTablesOnlineSelect01.isUnknown).get

    // join
    val auxTablesOnlineAndOrcaJoin01 = auxTablesOnlineSource.joinOperation(JOIN01)
    val auxTablesOnlineAndOrcaJoin01Map = JoinAndSelectOperation.doJoinAndSelect(orca201617And2017QryAndAuxTablesInnerJoin02, auxTablesOnlineSelect01DF.withColumnRenamed("Entity ID", "Right_Entity ID"), auxTablesOnlineAndOrcaJoin01)
    val auxTablesOnlineAndOrcaJoin01leftJoin01 = auxTablesOnlineAndOrcaJoin01Map(LEFT_JOIN)
    val auxTablesOnlineAndOrcaJoin01InnerJoin01 = auxTablesOnlineAndOrcaJoin01Map(INNER_JOIN)

    // browse here

    // union
    val auxTablesOnlineLeftAndInnerJoinUnion01 = UnionOperation.doUnion(auxTablesOnlineAndOrcaJoin01leftJoin01, auxTablesOnlineAndOrcaJoin01InnerJoin01).get

    // browse

    // filter
    val auxTablesOnlineFilter01 = auxTablesOnlineSource.filterOperation(FILTER01)
    val auxTablesOnlineFilter01DF = FilterOperation.doFilter(auxTablesOnlineLeftAndInnerJoinUnion01, auxTablesOnlineFilter01, auxTablesOnlineFilter01.conditionTypes(NUMERAL0)).get

    // formula with max wed for Aux Table Online
    val maxWed = auxTablesOnlineFilter01DF.agg(max("wed")).head().getDate(NUMERAL0)
    val auxTablesOnlineFormula01DF = auxTablesOnlineFilter01DF.withColumn("Online",
      when(col("Type") === "Online", 1)
        .otherwise(0))
      .withColumn("Max_wed", lit(maxWed))
      .cache()

    // group
    val auxTablesOnlineGroup01 = auxTablesOnlineSource.groupOperation(GROUP01)
    val auxTablesOnlineGroup01DF = GroupOperation.doGroup(auxTablesOnlineFormula01DF, auxTablesOnlineGroup01).get

    // join Staples continued..
    val staplesComUnitsJoin01 = staplesComUnitsSource.joinOperation(JOIN01)
    val staplesComUnitsJoin01Map = JoinAndSelectOperation.doJoinAndSelect(auxTablesOnlineGroup01DF, staplesComUnitsUnionDF.withColumnRenamed("wed", "Right_wed"), staplesComUnitsJoin01)
    val staplesComUnitsJoin01LefttDF = staplesComUnitsJoin01Map(LEFT_JOIN)
    val staplesComUnitsJoin01InnerDF = staplesComUnitsJoin01Map(INNER_JOIN).cache()


    // select
    val staplesComUnitsSelect02DF = SelectOperation.doSelect(staplesComUnitsJoin01InnerDF, staplesComUnitsSource.selectOperation(SELECT02).cols, staplesComUnitsSource.selectOperation(SELECT02).isUnknown).get

    // select -> formula
    val staplesComUnitsSetOnline1FormalaDF = Utils.litColumn(staplesComUnitsSelect02DF, "Online", 1)

    // formula
    val staplesComUnitsCalcFormala01DF = staplesComUnitsJoin01InnerDF.withColumn("Offline Units", lit(staplesComUnitsJoin01InnerDF("Sum_POS Qty") - staplesComUnitsJoin01InnerDF("Online Units")))

    // formula -> select
    val staplesComUnitsSelect03DF = SelectOperation.doSelect(staplesComUnitsCalcFormala01DF, staplesComUnitsSource.selectOperation(SELECT03).cols, staplesComUnitsSource.selectOperation(SELECT03).isUnknown).get


    /* HP COM*/
    // select

    val hpComSelect01DF = SelectOperation.doSelect(hpComDF, hpComSource.selectOperation(SELECT01).cols, hpComSource.selectOperation(SELECT01).isUnknown).get

    // join
    val hpComJoin01 = hpComSource.joinOperation(JOIN01)
    val hpComJoin01Map = JoinAndSelectOperation.doJoinAndSelect(hpComSelect01DF, auxTablesWeekendselect01DF, hpComJoin01)
    val hpComJoin01InnerDF = hpComJoin01Map(INNER_JOIN)

    // formula
    val hpComSubsStrFormalaDF = hpComJoin01InnerDF
      .withColumn("Product Number", substring(col("Product Number"), 0, 6))
      .withColumn("Account Major", lit("HP Shopping"))
      .withColumn("Online", lit("1"))

    // browse here

    // AMAZON_ARAP
    // formula
    val amazonArapFormula01DF = amazonArapDF
      .withColumn("Ordered Units", regexp_replace(amazonArapDF("Ordered Units"), "[^0-9.]*", ""))

    // select
    val amazonArapSelectDF = SelectOperation.doSelect(amazonArapFormula01DF, amazonArapSource.selectOperation(SELECT01).cols, amazonArapSource.selectOperation(SELECT01).isUnknown).get

    // filter
    val amazonArapFilterDF = FilterOperation.doFilter(amazonArapSelectDF, amazonArapSource.filterOperation(FILTER01), amazonArapSource.filterOperation(FILTER01).conditionTypes(NUMERAL0)).get

    // convert to Date (MM/dd/yyyy)
    val amazonArapConvertDateDF = amazonArapFilterDF.withColumn("Week Beginning Conv", to_date(unix_timestamp(col("Week Beginning"), "MM/dd/yyyy").cast("timestamp"), "yyyy-mm-dd"))
    //      .drop("Week Beginning")

    // group
    val amazonArapGroup01 = amazonArapSource.groupOperation(GROUP01)
    val amazonArapGroup01DF = GroupOperation.doGroup(amazonArapConvertDateDF, amazonArapGroup01).get.withColumnRenamed("Week Beginning Conv", "Week Beginning")

    /* Amazon Asin Map*/
    // select
    val amazonAsinMapSelect01 = amazonAsinMapSource.selectOperation(SELECT01)
    val amazonAsinMapSelectDF = SelectOperation.doSelect(amazonAsinMapDF, amazonAsinMapSelect01.cols, amazonAsinMapSelect01.isUnknown).get

    /* Amazon arap continued..*/
    // join
    val amazonArapJoin01 = amazonArapSource.joinOperation(JOIN01)
    val amazonArapJoin01Map = JoinAndSelectOperation.doJoinAndSelect(amazonArapGroup01DF, amazonAsinMapSelectDF.withColumnRenamed("ASIN", "Right_ASIN"), amazonArapJoin01)
    val amazonArapJoin01LefttDF = amazonArapJoin01Map(LEFT_JOIN)
    val amazonArapJoin01InnerDF = amazonArapJoin01Map(INNER_JOIN)

    // group
    val amazonArapGroup02 = amazonArapSource.groupOperation(GROUP02)
    val amazonArapGroup02DF = GroupOperation.doGroup(amazonArapJoin01LefttDF, amazonArapGroup02).get

    // sort
    val amazonArapSort01 = amazonArapSource.sortOperation(SORT01)
    val amazonArapSumOrderedUnitsSortDescDF = SortOperation.doSort(amazonArapGroup02DF, amazonArapSort01.ascending, amazonArapSort01.descending).get

    // filter
    val amazonArapFilter02 = amazonArapSource.filterOperation(FILTER02)
    val amazonArapSumOrderedUnitsGreaterThanZeroDF = FilterOperation.doFilter(amazonArapSumOrderedUnitsSortDescDF, amazonArapFilter02, amazonArapFilter02.conditionTypes(NUMERAL0)).get

    // browse here

    // formula
    val amazonArapAddAmazonFormula01 = Utils.litColumn(amazonArapJoin01InnerDF, "Account", "Amazon.com")
    val amazonArapAddOnlineFormula01 = Utils.litColumn(amazonArapAddAmazonFormula01, "Online", 1)
    val amazonArapAdd6DaysMoreFormula01 = amazonArapAddOnlineFormula01.withColumn("Week_End_Date", date_add(to_date(col("Week Beginning").cast("timestamp"), "yyyy-mm-dd"), 6))

    // select
    val amazonArapSelect02 = amazonArapSource.selectOperation(SELECT02)
    val amazonArapSelect02DF = Utils.convertListToDFColumnWithRename(amazonArapSource.renameOperation(RENAME02),
      SelectOperation.doSelect(amazonArapAdd6DaysMoreFormula01, amazonArapSelect02.cols, amazonArapSelect02.isUnknown).get)

    // filter
    val amazonArapFilter03 = amazonArapSource.filterOperation(FILTER03)
    val amazonArapSKUNotNaFilter03DF = FilterOperation.doFilter(amazonArapSelect02DF, amazonArapFilter03, amazonArapFilter03.conditionTypes(NUMERAL0)).get

    // group
    val amazonArapGroup03 = amazonArapSource.groupOperation(GROUP03)
    val amazonArapGroup03DF = GroupOperation.doGroup(amazonArapSKUNotNaFilter03DF, amazonArapGroup03).get

    // join
    val amazonArapJoin02 = amazonArapSource.joinOperation(JOIN02)
    val amazonArapJoin02Map = JoinAndSelectOperation.doJoinAndSelect(amazonArapGroup03DF, auxTablesWeekendselect01DF, amazonArapJoin02)
    val amazonArapJoin02InnerDF = amazonArapJoin02Map(INNER_JOIN)

    // select
    val amazonArapSelect03 = amazonArapSource.selectOperation(SELECT02)
    val amazonArapSelect03DF = SelectOperation.doSelect(amazonArapJoin02InnerDF, amazonArapSelect03.cols, amazonArapSelect03.isUnknown).get

    /* S Prints Historical Units */
    // convert to Date (MM/dd/yyyy)
    val sPrintsHistoricalUnitsDateConvDF = sPrintHistoricalUnitsDF.withColumn("Week_End_Date", to_date(unix_timestamp(col("wed"), "MM/dd/yyyy").cast("timestamp")))

    // select
    val sPrintsHistoricalUnitsSelect01 = sPrintHistoricalUnitsSource.selectOperation(SELECT01)
    val sPrintsHistoricalUnitsSelect01DF = SelectOperation.doSelect(sPrintsHistoricalUnitsDateConvDF, sPrintsHistoricalUnitsSelect01.cols, sPrintsHistoricalUnitsSelect01.isUnknown).get

    // filter
    val sPrintsHistoricalUnitsChannelRetailFilter01 = sPrintHistoricalUnitsSource.filterOperation(FILTER01)
    val sPrintsHistoricalUnitsChannelRetailFilter01DF = FilterOperation.doFilter(sPrintsHistoricalUnitsSelect01DF, sPrintsHistoricalUnitsChannelRetailFilter01, sPrintsHistoricalUnitsChannelRetailFilter01.conditionTypes(NUMERAL0)).get.withColumnRenamed("Week_End_Date", "wed")

    // group
    val sPrintsHistoricalUnitsGroup01 = sPrintHistoricalUnitsSource.groupOperation(GROUP01)
    val sPrintsHistoricalUnitsGroup01DF = GroupOperation.doGroup(sPrintsHistoricalUnitsChannelRetailFilter01DF, sPrintsHistoricalUnitsGroup01).get

    // formula
    val sPrintsHistoricalFormula01DF = sPrintsHistoricalUnitsGroup01DF.withColumn("Sum_Inv : Saleable Qty",
      when(col("Sum_Inv : Saleable Qty") > lit(0), 1)
        .otherwise(0))

    // Union
    val mainUnion01StaplesLeftAndStaplesSelect = UnionOperation.doUnion(staplesComUnitsJoin01LefttDF, staplesComUnitsSelect03DF).get
    val mainUnion02Union01AndStaplesFormula = UnionOperation.doUnion(mainUnion01StaplesLeftAndStaplesSelect, staplesComUnitsSetOnline1FormalaDF).get
    val mainUnion03Union02AndHPQryFormula = UnionOperation.doUnion(mainUnion02Union01AndStaplesFormula, hpComSubsStrFormalaDF).get
    val mainUnion04Union03AndAmazonArapSelect = UnionOperation.doUnion(mainUnion03Union02AndHPQryFormula, amazonArapSelect03DF.withColumn("Product Base Desc", lit(null: String))).get
    val mainUnion05Union04AndSPrintFormula = UnionOperation.doUnion(mainUnion04Union03AndAmazonArapSelect, sPrintsHistoricalFormula01DF).get

    /* Aux Table online continued.. */

    // formula
    val auxTablesOnlineFormula02DF = auxTablesOnlineFormula01DF.withColumn("Product Base ID",
      when(col("Product Base ID") === "M9L74A", "M9L75A")
        .when((col("Product Base ID") === "J9V91A") || (col("Product Base ID")) === "J9V92A", "J9V90A")
        .when(col("Product Base ID") === "Z3M52A", "K7G93A")
        .otherwise(col("Product Base ID")))

    // group
    val auxTablesOnlineGroup02 = auxTablesOnlineSource.groupOperation(GROUP02)
    val auxTablesOnlineGroup02DF = GroupOperation.doGroup(auxTablesOnlineFormula02DF, auxTablesOnlineGroup02).get

    // filter
    val maxWedIncluding7000DaysDF = auxTablesOnlineGroup02DF.withColumn("date_last_52weeks", date_sub(col("Max_wed"), 365))
    val auxTablesOnlineWedGreaterThan7000Filter02 = auxTablesOnlineSource.filterOperation(FILTER02)
    val auxTablesOnlineWedGreaterThan7000Filter02DF = FilterOperation.doFilter(maxWedIncluding7000DaysDF, auxTablesOnlineWedGreaterThan7000Filter02, auxTablesOnlineWedGreaterThan7000Filter02.conditionTypes(NUMERAL0)).get

    // distribution calculation starts
    // filter
    val auxTablesOnlineIfOnline1Filter = auxTablesOnlineSource.filterOperation(FILTER03)
    val auxTablesOnlineIfOnline1FilterDF = FilterOperation.doFilter(auxTablesOnlineWedGreaterThan7000Filter02DF, auxTablesOnlineIfOnline1Filter, auxTablesOnlineIfOnline1Filter.conditionTypes(NUMERAL0)).get.cache()

    // group
    val auxTablesOnlineGroup03 = auxTablesOnlineSource.groupOperation(GROUP03)
    val auxTablesOnlineGroup03DF = GroupOperation.doGroup(auxTablesOnlineIfOnline1FilterDF, auxTablesOnlineGroup03).get

    // group
    val auxTablesOnlineGroup04 = auxTablesOnlineSource.groupOperation(GROUP04)
    val auxTablesOnlineGroup04DF = GroupOperation.doGroup(auxTablesOnlineGroup03DF, auxTablesOnlineGroup04).get

    // formula
    val auxTablesOnlineFormula03DF = auxTablesOnlineGroup03DF.withColumn("Sum_POS Sales NDP",
      when(col("Sum_POS Sales NDP") < 0, 0)
        .otherwise(col("Sum_POS Sales NDP")))

    // join
    val auxTablesOnlineJoin02 = auxTablesOnlineSource.joinOperation(JOIN02)
    val auxTablesOnlineFormula03ColsRenamedDF = Utils.convertListToDFColumnWithRename(auxTablesOnlineSource.renameOperation(RENAME02), auxTablesOnlineFormula03DF)
    val auxTablesOnlineJoin02Map = JoinAndSelectOperation.doJoinAndSelect(auxTablesOnlineIfOnline1FilterDF, auxTablesOnlineFormula03ColsRenamedDF, auxTablesOnlineJoin02)
    val auxTablesOnlineJoin02InnerDF = auxTablesOnlineJoin02Map(INNER_JOIN)

    // formula
    val auxTablesOnlineFormula04DF = auxTablesOnlineJoin02InnerDF.withColumn("Store POS",
      when(col("POS Qty") > 0, col("Sum_POS Sales NDP"))
        .otherwise(0))
      .withColumn("Store Inv",
        when(col("Inventory Total Qty") > 0, col("Sum_POS Sales NDP"))
          .otherwise(0))

    // group
    val auxTablesOnlineGroup05 = auxTablesOnlineSource.groupOperation(GROUP05)
    val auxTablesOnlineGroup05DF = GroupOperation.doGroup(auxTablesOnlineFormula04DF, auxTablesOnlineGroup05).get

    // join
    val auxTablesOnlineJoin03 = auxTablesOnlineSource.joinOperation(JOIN03)
    val auxTablesOnlineGroup04RenamedDF = Utils.convertListToDFColumnWithRename(auxTablesOnlineSource.renameOperation(RENAME03), auxTablesOnlineGroup04DF)
    val auxTablesOnlineJoin03Map = JoinAndSelectOperation.doJoinAndSelect(auxTablesOnlineGroup05DF, auxTablesOnlineGroup04RenamedDF, auxTablesOnlineJoin03)
    val auxTablesOnlineJoin03InnerDF = auxTablesOnlineJoin03Map(INNER_JOIN)

    // formula
    val auxTablesOnlineFormula05DF = auxTablesOnlineJoin03InnerDF.withColumn("SKU ACV POS",
      when(col("Sum_Sum_POS Sales NDP") < 1, 0)
        .when((col("Sum_Store POS") / col("Sum_Sum_POS Sales NDP")) > 1, 1)
        .otherwise(col("Sum_Store POS") / col("Sum_Sum_POS Sales NDP")))
      .withColumn("SKU ACV Inv", when(col("Sum_Sum_POS Sales NDP") < 1, 0)
        .when(col("Sum_Sum_POS Sales NDP") > 1, 1)
        .otherwise(col("Sum_Sum_POS Sales NDP")))

    // select with rename
    val auxTablesOnlineSelect02 = auxTablesOnlineSource.selectOperation(SELECT02)
    val auxTablesOnlineSelect02DF = Utils.convertListToDFColumnWithRename(auxTablesOnlineSource.renameOperation(RENAME01),
      SelectOperation.doSelect(auxTablesOnlineFormula05DF, auxTablesOnlineSelect02.cols, auxTablesOnlineSelect02.isUnknown).get)

    // browse here
    // distribution calculation Ends

    /* Aux Table SKU Hierarchy */
    //select
    val auxTablesSKUHierarchySelect01 = auxTablesSKUHierarchySource.selectOperation(SELECT01)
    val auxTablesSKUHierarchySelect01DF = SelectOperation.doSelect(auxTablesSKUHierarchy, auxTablesSKUHierarchySelect01.cols, auxTablesSKUHierarchySelect01.isUnknown).get

    // join
    val auxTablesSKUHierarchyJoin01 = auxTablesSKUHierarchySource.joinOperation(JOIN01)
    val auxTablesSKUHierarchyJoin01Map = JoinAndSelectOperation.doJoinAndSelect(mainUnion05Union04AndSPrintFormula, auxTablesSKUHierarchySelect01DF, auxTablesSKUHierarchyJoin01)
    val auxTablesSKUHierarchyJoin01LeftDF = auxTablesSKUHierarchyJoin01Map(LEFT_JOIN)
    val auxTablesSKUHierarchyJoin01InnerDF = auxTablesSKUHierarchyJoin01Map(INNER_JOIN)

    // group
    val auxTablesSKUHierarchyGroup01 = auxTablesSKUHierarchySource.groupOperation(GROUP01)
    val auxTablesSKUHierarchyGroup01DF = GroupOperation.doGroup(auxTablesSKUHierarchyJoin01LeftDF, auxTablesSKUHierarchyGroup01).get

    // sort
    val auxTablesSKUHierarchySort01 = auxTablesSKUHierarchySource.sortOperation(SORT01)
    val auxTablesSKUHierarchySort01DF = SortOperation.doSort(auxTablesSKUHierarchyGroup01DF, auxTablesSKUHierarchySort01.ascending, auxTablesSKUHierarchySort01.descending).get

    // browse or write to CSV

    // group
    val auxTablesSKUHierarchyGroup02 = auxTablesSKUHierarchySource.groupOperation(GROUP02)
    val auxTablesSKUHierarchyGroup02DF = Utils.convertListToDFColumnWithRename(auxTablesSKUHierarchySource.renameOperation(RENAME01), GroupOperation.doGroup(auxTablesSKUHierarchyJoin01InnerDF, auxTablesSKUHierarchyGroup02).get).cache()

    // formula
    val auxTablesSKUHierarchyFormula01 = auxTablesSKUHierarchyGroup02DF.withColumn("Raw POS Qty", col("POS Qty"))

    // unique
    val auxTablesSKUHierarchyDistinctDF = auxTablesSKUHierarchyGroup02DF.dropDuplicates(List("Account Major", "Online", "SKU", "WED"))

    // browse here

    // filter
    val auxTablesSKUHierarchyFilter01 = auxTablesSKUHierarchySource.filterOperation(FILTER01)
    val auxTablesSKUHierarchyFilter01DF = FilterOperation.doFilter(auxTablesSKUHierarchyGroup02DF, auxTablesSKUHierarchyFilter01, auxTablesSKUHierarchyFilter01.conditionTypes(NUMERAL0)).get

    /* BBY Bundle Info */

    implicit val bBYBundleEncoder = Encoders.product[BBYBundle]
    implicit val bBYBundleTranspose = Encoders.product[BBYBundleTranspose]

    val bbyDataSet = bbyBundleInfo.as[BBYBundle]
    val names = bbyDataSet.schema.fieldNames

    val transposedData = bbyDataSet.flatMap(row => Array(BBYBundleTranspose(row.`HP SKU`, row.`Week Ending`, row.Units, names(3), row.`B&M Units`),
      BBYBundleTranspose(row.`HP SKU`, row.`Week Ending`, row.Units, names(4), row.`_COM Units`)))

    val bbyBundleInfoTransposeDF = transposedData.toDF()

    // select
    val bbyBundleInfoSelect01 = bbyBundleInfoSource.selectOperation(SELECT01)
    val bbyBundleInfoSelect01DF = SelectOperation.doSelect(bbyBundleInfoTransposeDF, bbyBundleInfoSelect01.cols, bbyBundleInfoSelect01.isUnknown).get

    // formula
    val bbyBundleInfoFormula01DF = bbyBundleInfoSelect01DF.withColumn("Online",
      when(col("Online") === "B&M Units", 0)
        .otherwise(1))

    // group
    val bbyBundleInfoGroup01 = bbyBundleInfoSource.groupOperation(GROUP01)
    val bbyBundleInfoGroup01DF = GroupOperation.doGroup(bbyBundleInfoFormula01DF, bbyBundleInfoGroup01).get

    // join
    val bbyBundleInfoJoin01 = bbyBundleInfoSource.joinOperation(JOIN01)
    val bbyBundleInfoJoin01Map = JoinAndSelectOperation.doJoinAndSelect(auxTablesSKUHierarchyFilter01DF, bbyBundleInfoGroup01DF.withColumnRenamed("Online", "Right_Online"), bbyBundleInfoJoin01)
    val bbyBundleInfoJoin01InnerDF = bbyBundleInfoJoin01Map(INNER_JOIN)

    // unique
    val bbyBundleInfoDistinctDF = bbyBundleInfoJoin01InnerDF.dropDuplicates(List("Online", "SKU", "WED"))

    // browse here

    // group
    val bbyBundleInfoGroup02 = bbyBundleInfoSource.groupOperation(GROUP02)
    val bbyBundleInfoGroup02DF = GroupOperation.doGroup(bbyBundleInfoJoin01InnerDF, bbyBundleInfoGroup02).get

    // join
    val bbyBundleInfoJoin02 = bbyBundleInfoSource.joinOperation(JOIN02)
    val bbyBundleInfoJoin01InnerRenamedDF = Utils.convertListToDFColumnWithRename(bbyBundleInfoSource.renameOperation(RENAME03), bbyBundleInfoJoin01InnerDF)
    val bbyBundleInfoJoin02Map = JoinAndSelectOperation.doJoinAndSelect(bbyBundleInfoGroup02DF, bbyBundleInfoJoin01InnerRenamedDF, bbyBundleInfoJoin02)
    val bbyBundleInfoJoin02InnerDF = bbyBundleInfoJoin02Map(INNER_JOIN)

    // formula
    val bbyBundleInfoFormula02DF = bbyBundleInfoJoin02InnerDF.withColumn("Ratio", (col("POS Qty") / col("Sum_POS Qty")))

    // formula
    val bbyBundleInfoFormula03DF = bbyBundleInfoFormula02DF.withColumn("Bundle Qty Est",
      when(col("Online") === 1, floor(col("Ratio") * col("Units")))
        .otherwise(ceil(col("Ratio") * col("Units"))))

    // formula
    val bbyBundleInfoFormula04DF = bbyBundleInfoFormula03DF.withColumn("Bundle Qty",
      when(!isnull(col("Bundle Qty Raw")), col("Bundle Qty Raw"))
        .otherwise(col("Bundle Qty Est")))
      .withColumn("Bundle Qty Source",
        when(!isnull(col("Bundle Qty Raw")), "Raw")
          .otherwise("Est"))

    // formula
    val bbyBundleInfoFormula05DF = bbyBundleInfoFormula04DF.withColumn("POS Qty", (col("POS Qty") - col("Bundle Qty")))
      .withColumn("Account Major", lit("Best Buy"))

    // join
    val bbyBundleInfoJoin03 = bbyBundleInfoSource.joinOperation(JOIN03)
    val bbyBundleInfoFormula05RenamedDF = Utils.convertListToDFColumnWithRename(bbyBundleInfoSource.renameOperation(RENAME04), bbyBundleInfoFormula05DF)
    val bbyBundleInfoJoin03Map = JoinAndSelectOperation.doJoinAndSelect(auxTablesSKUHierarchyFormula01, bbyBundleInfoFormula05RenamedDF, bbyBundleInfoJoin03)
    val bbyBundleInfoJoin03LeftDF = bbyBundleInfoJoin03Map(LEFT_JOIN)
    val bbyBundleInfoJoin03InnerDF = bbyBundleInfoJoin03Map(INNER_JOIN)

    // union
    val unionLeftAndInnerJoinDF = UnionOperation.doUnion(bbyBundleInfoJoin03LeftDF, bbyBundleInfoJoin03InnerDF).get

    // join
    val bbyBundleInfoAndAuxTablesOnlineJoin04 = bbyBundleInfoSource.joinOperation(JOIN04)

    val unionLeftAndInnerJoinDFLeftRenamedDF = Utils.convertListToDFColumnWithRename(bbyBundleInfoSource.renameOperation(RENAME05), unionLeftAndInnerJoinDF)
    val auxTablesOnlineSelect02RightRenamedDF = Utils.convertListToDFColumnWithRename(bbyBundleInfoSource.renameOperation(RENAME06), auxTablesOnlineSelect02DF)
    val bbyBundleInfoJoin04Map = JoinAndSelectOperation.doJoinAndSelect(unionLeftAndInnerJoinDFLeftRenamedDF, auxTablesOnlineSelect02RightRenamedDF, bbyBundleInfoAndAuxTablesOnlineJoin04)
    val bbyBundleInfoJoin04LeftDF = bbyBundleInfoJoin04Map(LEFT_JOIN).withColumnRenamed("SKU ACV Inv", "Distribution_Inv")
    val bbyBundleInfoJoin04InnerDF = bbyBundleInfoJoin04Map(INNER_JOIN).withColumnRenamed("SKU ACV Inv", "Distribution_Inv")
    val bbyBundleInfoJoin04RightDF = bbyBundleInfoJoin04Map(RIGHT_JOIN)

    // formula
    val bbyBundleInfoFormula06DF = bbyBundleInfoJoin04LeftDF.withColumn("Distribution_Inv",
      when(col("POS_Qty") > 0, 1)
        otherwise (0))

    // union with append max weekend date
    val unionFormulaAndInnerJoinDF = UnionOperation.doUnion(bbyBundleInfoFormula06DF, bbyBundleInfoJoin04InnerDF).get
    val calMaxWED = unionFormulaAndInnerJoinDF.agg(max("Week_End_Date")).head().getDate(NUMERAL0)
    val unionAppendMaxWeekEndDate = unionFormulaAndInnerJoinDF.withColumn("Max_Week_End_Date", lit(calMaxWED))


    // browse here

    // filter
    val last26Weeks70000DaysDF = unionAppendMaxWeekEndDate.withColumn("date_last_26weeks", date_sub(col("Max_Week_End_Date"), 182))
    val bbyBundleInfoWedGreaterThan70000Filter01 = bbyBundleInfoSource.filterOperation(FILTER01)
    val bbyBundleInfoWedGreaterThan70000Filter01DF = FilterOperation.doFilter(last26Weeks70000DaysDF, bbyBundleInfoWedGreaterThan70000Filter01, bbyBundleInfoWedGreaterThan70000Filter01.conditionTypes(NUMERAL0)).get


    /* Existing POS */
    val existingPOSSelect01 = existingPOSSource.selectOperation(SELECT01)
    val existingPOSSelect01DF = SelectOperation.doSelect(existingPOS, existingPOSSelect01.cols, existingPOSSelect01.isUnknown).get

    // join
    val bbyBundleInfoAndAuxTablesOnlineJoin05 = bbyBundleInfoSource.joinOperation(JOIN05)
    val bbyBundleInfoLast26WeeksRenamedDF = Utils.convertListToDFColumnWithRename(bbyBundleInfoSource.renameOperation(RENAME02), bbyBundleInfoWedGreaterThan70000Filter01DF)
    val bbyBundleInfoJoin05Map = JoinAndSelectOperation.doJoinAndSelect(existingPOSSelect01DF, bbyBundleInfoLast26WeeksRenamedDF, bbyBundleInfoAndAuxTablesOnlineJoin05)
    val bbyBundleInfoJoin05LeftDF = /*Utils.convertListToDFColumnWithRename(bbyBundleInfoSource.renameOperation(RENAME02), */ bbyBundleInfoJoin05Map(LEFT_JOIN)

    // union
    val unionFilterAndJoin05DF = UnionOperation.doUnion(bbyBundleInfoJoin05LeftDF, bbyBundleInfoWedGreaterThan70000Filter01DF).get

    // formula
    val bbyBundleInfoFormula07DF = unionFilterAndJoin05DF.withColumn("Season",
      when(col("IPSLES") === "IPS", col("Season"))
        .otherwise(when(col("Week_End_Date") === "2016-10-01", "BTS'16")
          .when(col("Week_End_Date") === "2016-12-31", "HOL'16")
          .when(col("Week_End_Date") === "2017-04-01", "BTB'17")
          .when(col("Week_End_Date") === "2017-07-01", "STS'17")
          otherwise (col("Season"))))

    // sort
    val bbyBundleInfoSort01 = bbyBundleInfoSource.sortOperation(SORT01)
    val bbyBundleInfoSort01DF = SortOperation.doSort(bbyBundleInfoFormula07DF, bbyBundleInfoSort01.ascending, bbyBundleInfoSort01.descending).get

    // unique
    val format = new SimpleDateFormat("d-M-y h-m-s")
    import java.util.Calendar;

    val currentTS = executionContext.spark.read.json("/etherData/state/currentTS.json").select("ts").head().getString(0)

    bbyBundleInfoSort01DF.dropDuplicates(List("Account", "Online", "SKU", "Week_End_Date", "Max_Week_End_Date"))
      .write.mode(SaveMode.Overwrite).option("header", true).csv("/etherData/Pricing/Output/POS_Retail/posqty_retail_output_"+currentTS+".csv")
    // formula
    //val bbyBundleInfoFormula08DF = bbyBundleInfoSort01DF.withColumn("Workflow Run Date", current_date())

    // browse
  }

}