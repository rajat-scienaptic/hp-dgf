package com.scienaptic.jobs.core

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.bean._
import com.scienaptic.jobs.utility.Utils
import org.apache.spark.sql.functions._

    //TODO: Check if argument lists empty. Don't call utility if empty!
    //TODO: Implement Logger
    //TODO: Implement Custom Exceptions
object RetailTransform {

  val SELECT01 = "select01";
  val SELECT02 = "select02";
  val SELECT03 = "select03";
  val SELECT04 = "select04";
  val SELECT05 = "select05";
  val SELECT06 = "select06";
  val FILTER01 = "filter01";
  val FILTER02 = "filter02";
  val FILTER03 = "filter03";
  val FILTER04 = "filter04";
  val FILTER05 = "filter05";
  val FILTER06 = "filter06";
  val JOIN01 = "join01";
  val JOIN02 = "join02";
  val JOIN03 = "join03";
  val JOIN04 = "join04";
  val JOIN05 = "join05";
  val JOIN06 = "join06";
  val JOIN07 = "join07";
  val NUMERAL0 = 0;
  val NUMERAL1 = 1;
  val GROUP01 = "group01";
  val GROUP02 = "group02";
  val GROUP03 = "group03";
  val GROUP04 = "group04";

  val ODOM_ONLINE_ORCA_SOURCE = "ODOM_ONLINE_ORCA";
  val STAPLES_COM_UNITS_SOURCE = "STAPLES_COM_UNITS";
  val HP_COM_SOURCE = "HP_COM";
  val AMAZON_ARAP_SOURCE = "AMAZON_ARAP";
  val S_PRINT_HISTORICAL_UNITS_SOURCE = "S_PRINT_HISTORICAL_UNITS";
  val ORCA_2014_16_ARCHIVE_SOURCE = "ORCA_2014_16_ARCHIVE";
  val ORCA_QRY_2017_TO_DATE_SOURCE = "ORCA_QRY_2017_TO_DATE";
  val AUX_TABLES_WEEKEND_SOURCE = "AUX_TABLES_WEEKEND";
  val AUX_TABLES_ONLINE_SOURCE = "AUX_TABLES_ONLINE";
  val AUX_TABLES_SKU_HIERARCHY_SOURCE = "AUX_TABLES_SKU_HIERARCHY";
  val BBY_BUNDLE_INFO_SOURCE = "BBY_BUNDLE_INFO";

  def execute(executionContext: ExecutionContext): Unit = {
    /* Source Map with all sources' information */
    val sourceMap = executionContext.configuration.sources

    /* Map with all operations source operations */
    val orcaOdomSource = sourceMap(ODOM_ONLINE_ORCA_SOURCE)
    val staplesComUnitsSource = sourceMap(STAPLES_COM_UNITS_SOURCE)
    val hpComSource = sourceMap(HP_COM_SOURCE)
    val amazomArapSource = sourceMap(AMAZON_ARAP_SOURCE)
    val sPrintHistoricalUnitsSource = sourceMap(S_PRINT_HISTORICAL_UNITS_SOURCE)
    val orca201416ArchiveSource = sourceMap(ORCA_2014_16_ARCHIVE_SOURCE)
    val orcaQry2017ToDateSource = sourceMap(ORCA_QRY_2017_TO_DATE_SOURCE)
    val auxTablesWeekendSource = sourceMap(AUX_TABLES_WEEKEND_SOURCE)
    val auxTablesOnlineSource = sourceMap(AUX_TABLES_ONLINE_SOURCE)
    val auxTablesSKUHierarchySource = sourceMap(AUX_TABLES_SKU_HIERARCHY_SOURCE)
    val bbyBundleInfoSource = sourceMap(BBY_BUNDLE_INFO_SOURCE)


    val odomOrcaDF = Utils.loadCSV(executionContext, orcaOdomSource.filePath).get
    val staplesDotCom_UnitsDF = Utils.loadCSV(executionContext, staplesComUnitsSource.filePath).get
    val hpComDF = Utils.loadCSV(executionContext, hpComSource.filePath).get
    val amazomArapDF = Utils.loadCSV(executionContext, amazomArapSource.filePath).get
    val sPrintHistoricalUnitsDF = Utils.loadCSV(executionContext, sPrintHistoricalUnitsSource.filePath).get
    val orca201416ARchive = Utils.loadCSV(executionContext, orca201416ArchiveSource.filePath).get
    val orcaQry2017ToDate = Utils.loadCSV(executionContext, orcaQry2017ToDateSource.filePath).get
    val auxTablesWeekend = Utils.loadCSV(executionContext, auxTablesWeekendSource.filePath).get
    val auxTablesOnline = Utils.loadCSV(executionContext, auxTablesOnlineSource.filePath).get
    val auxTablesSKUHierarchy = Utils.loadCSV(executionContext, auxTablesSKUHierarchySource.filePath).get
    val bbyBundleInfo = Utils.loadCSV(executionContext, bbyBundleInfoSource.filePath).get

    // AUX TABLES WEEKEND
    val auxTablesWeekendselect01DF = SelectOperation.doSelect(odomOrcaDF, auxTablesWeekendSource.selectOperation("select01").cols).get

    // ODOOM ORCA
    // Select01
    val odomOrcaselect01DF = SelectOperation.doSelect(odomOrcaDF, orcaOdomSource.selectOperation(SELECT01).cols,
      orcaOdomSource.selectOperation(SELECT01).isUnknown).get

    // formula
    val odomOrcaSubsStrFormalaDF = odomOrcaselect01DF.withColumn("Base SKU", substring(col("Vendor Product Code"),
      0, 6)).withColumn("Account Major", lit("Office Depot-Max"))

    // group
    val odomOrcaGroup01 = sourceMap("ODOM_ONLINE_ORCA").groupOperation("group01")
    val odomOrcaGroup01DF = GroupOperation.doGroup(odomOrcaSubsStrFormalaDF, odomOrcaGroup01.cols, odomOrcaGroup01.aggregations).get
    val odomOrcaGroupByColsDF = odomOrcaSubsStrFormalaDF

    // join
    val odomOrcaJoin01 = sourceMap("ODOM_ONLINE_ORCA").joinOperation("join01")
    var odomOrcaJoin01Map = JoinAndSelectOperation.doJoinAndSelect(odomOrcaGroupByColsDF, auxTablesWeekendselect01DF, odomOrcaJoin01)
    val odomOrcaJoinRightDF = odomOrcaJoin01Map("inner").cache()

    // STAPLESDOTCOM UNITS
    // Select01
    val staplesComUnitsselect01DF = Utils.convertListToDFColumnWithRename(sourceMap("STAPLES_COM_UNITS").renameOperation("rename01"),
      SelectOperation.doSelect(odomOrcaDF, sourceMap("STAPLES_COM_UNITS").selectOperation("select01").cols)
        .get)

    // TODO : create Utils
    // formula
    val staplesComUnitsFormula01DF = staplesComUnitsselect01DF.withColumn("Account Major", lit("staples"))

    // union
    val staplesComUnitsUnionDF = UnionOperation.doUnion(odomOrcaJoinRightDF, staplesComUnitsFormula01DF)


    // AMAZON_ARAP
    val amazonArapSelectDF = SelectOperation.doSelect(staplesComUnitsFormula01DF,amazomArapSource.selectOperation(SELECT01).cols).get

    // filter
    val amazonArapFilterDF = FilterOperation.doFilter(amazonArapSelectDF, amazomArapSource.filterOperation(FILTER01), amazomArapSource.filterOperation(FILTER01).conditionTypes(NUMERAL0))
  }

}

