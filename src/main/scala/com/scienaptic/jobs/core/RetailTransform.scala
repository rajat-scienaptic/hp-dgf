package com.scienaptic.jobs.core

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.bean._
import com.scienaptic.jobs.utility.Utils
import org.apache.spark.sql.functions._

object RetailTransform {

  def execute(executionContext: ExecutionContext): Unit = {

    val sourceMap = executionContext.configuration.sources

    print("Source Name " + sourceMap("odom_online_orca").name)
    //TODO: Check if argument lists empty. Don't call utility if empty!
    //TODO: Implement Logger
    //TODO: Implement Custom Exceptions

    val odomOrcaDF = Utils.loadCSV(executionContext, sourceMap("ODOM_ONLINE_ORCA").filePath).get
    val staplesDotCom_UnitsDF = Utils.loadCSV(executionContext, sourceMap("STAPLES_COM_UNITS").filePath).get
    val hpComDF = Utils.loadCSV(executionContext, sourceMap("HP_COM").filePath).get
    val amazomArapDF = Utils.loadCSV(executionContext, sourceMap("AMAZON_ARAP").filePath).get
    val sPrintHistoricalUnitsDF = Utils.loadCSV(executionContext, sourceMap("S_PRINT_HISTORICAL_UNITS").filePath).get
    val orca201416ARchive = Utils.loadCSV(executionContext, sourceMap("ORCA_2014_16_ARCHIVE").filePath).get
    val orcaQry2017ToDate = Utils.loadCSV(executionContext, sourceMap("ORCA_QRY_2017_TO_DATE").filePath).get
    val auxTablesWeekend = Utils.loadCSV(executionContext, sourceMap("AUX_TABLES_WEEKEND").filePath).get
    val auxTablesOnline = Utils.loadCSV(executionContext, sourceMap("AUX_TABLES_ONLINE").filePath).get
    val auxTablesSKUHierarchy = Utils.loadCSV(executionContext, sourceMap("AUX_TABLES_SKU_HIERARCHY").filePath).get
    val bbyBundleInfo = Utils.loadCSV(executionContext, sourceMap("BBY_BUNDLE_INFO").filePath).get

    // AUX TABLES WEEKEND
    val auxTablesWeekendselect01DF = SelectOperation.doSelect(odomOrcaDF, sourceMap("AUX_TABLES_WEEKEND").selectOperation("select01").cols).get

    // ODOOM ORCA
    // Select01
    val odomOrcaselect01DF = SelectOperation.doSelect(odomOrcaDF, sourceMap("ODOM_ONLINE_ORCA").selectOperation("select01").cols).get

    // formula
    val odomOrcaSubsStrFormalaDF = odomOrcaselect01DF.withColumn("Base SKU", substring(col("Vendor Product Code"),
      0, 6)).withColumn("Account Major", lit("Office Depot-Max"))


    // STAPLESDOTCOM UNITS
    // Select01
    val staplesComUnitsselect01DF = SelectOperation.doSelect(odomOrcaDF, sourceMap("STAPLES_COM_UNITS").selectOperation("select01").cols).get


    // formula
    val staplesComUnitsFormula01DF = staplesComUnitsselect01DF.withColumn("Account Major", lit("staples"))





  }

}

