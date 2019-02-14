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

    auxTablesOnlineFormula01DF.write.option("header", true).mode(SaveMode.Overwrite).csv("/etherData/retailTemp/retailAlteryx/auxTablesOnlineFormula01DF.csv")

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

    mainUnion05Union04AndSPrintFormula.write.option("header", true).mode(SaveMode.Overwrite).csv("/etherData/retailTemp/retailAlteryx/mainUnion05Union04AndSPrintFormula.csv")



  }

}