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

  val INITIAL_SELECT="INITIAL_SELECT"
  val RENAME_INITIAL_SELECT="RENAME_INITIAL_SELECT"

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
    val xsClaimsSelectDF = doSelect(iecDF, iecInitialSelect.cols,iecInitialSelect.isUnknown).get
    val rawCalendarSelectDF = doSelect(iecDF, iecInitialSelect.cols,iecInitialSelect.isUnknown).get
    val wedSelectDF = doSelect(iecDF, iecInitialSelect.cols,iecInitialSelect.isUnknown).get
    val skuHierarchySelectDF = doSelect(iecDF, iecInitialSelect.cols,iecInitialSelect.isUnknown).get
    val commAccountsSelectDF = doSelect(iecDF, iecInitialSelect.cols,iecInitialSelect.isUnknown).get
    val stONYXSelectDF = doSelect(iecDF, iecInitialSelect.cols,iecInitialSelect.isUnknown).get
    val sttONYXSelectDF = doSelect(iecDF, iecInitialSelect.cols,iecInitialSelect.isUnknown).get
    val tidyHistSelectDF = doSelect(iecDF, iecInitialSelect.cols,iecInitialSelect.isUnknown).get

    /*
    * Rename Columns after initial Select Operation
    * */
    val xsClaimsInitialRename = xsClaimsSource.renameOperation(RENAME_INITIAL_SELECT)
    val rawCalendarInitialRename = rawCalendarSource.renameOperation(RENAME_INITIAL_SELECT)
    val tidyHistInitialRename = tidyHistSource.renameOperation(RENAME_INITIAL_SELECT)

    val xsClaimsRenamedDF = Utils.convertListToDFColumnWithRename(xsClaimsInitialRename, xsClaimsDF)
    val wedRenamedDF = Utils.convertListToDFColumnWithRename(rawCalendarInitialRename, WEDDF)
    val tidyHistRenamedDF = Utils.convertListToDFColumnWithRename(tidyHistInitialRename, tidyHistDF)

    /* Filter IEC Claims for Partner Ship Calendar Date */
    val iecFilterShipDateDF = iecSelectDF.filter(col("Partner Ship Calendar Date") > lit("2016-12-31"))

    /* Union IEC and XS Dataset */
    val iecUnionXSDF = doUnion(iecFilterShipDateDF, xsClaimsRenamedDF).get

    /* Create Features - Base SKU, Temp Date Calc String, Temp Date Calc, Week End Date */
    val iecXSNewFeaturesDF = iecUnionXSDF.withColumn("Base SKU",createBaseSKUFromProductIDUDF(col("Product ID")))
      .withColumn("Temp Date Calc String", weekofyear(to_date(col("Partner Ship Calendar Date"))))
      .withColumn("Temp Date Calc", col("Temp Date Calc String").cast(DoubleType))
      .withColumn("Week End Date",addDaystoDateStringUDF(col("Partner Ship Calendar Date"), col("Temp Date Calc")))
      .withColumn("Base SKU",baseSKUFormulaUDF(col("Base SKU")))

  }
}
