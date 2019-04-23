package com.scienaptic.jobs.core

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.bean.FilterOperation._
import com.scienaptic.jobs.bean.GroupOperation._
import com.scienaptic.jobs.bean.JoinAndSelectOperation._
import com.scienaptic.jobs.bean.SelectOperation._
import com.scienaptic.jobs.bean.UnionOperation._
import com.scienaptic.jobs.utility.Utils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

object RetailTransform3 {
  val INNER_JOIN = "inner"
  val LEFT_JOIN = "leftanti"
  val RIGHT_JOIN = "rightanti"
  val SELECT01 = "select01"
  val NUMERAL0 = 0
  val NUMERAL1 = 1

  val SELECT_LAST_WALMART = "SELECT_LAST_WALMART"
  val SELECT_ALL = "SELECT_ALL"
  val SELECT_EXCEPT_WED = "SELECT_EXCEPT_WED"
  val RENAME_WED_PRODUCT_BASEID_AND_ACCOUNT_MAJOR_CONSOL = "RENAME_WED_PRODUCT_BASEID_AND_ACCOUNT_MAJOR_CONSOL"
  val WALMART_STORE_TYPE_FILTER = "WALMART_STORE_TYPE_FILTER"
  val GROUP_WEEK_BASESKU_STORENBR_AGG_POSQTY_POSSALES_STRONHANDQTY = "GROUP_WEEK_BASESKU_STORENBR_AGG_POSQTY_POSSALES_STRONHANDQTY"
  val GROUP_STORENBR_AGG_SUMPOSQTY_SUMPOSSALES = "GROUP_STORENBR_AGG_SUMPOSQTY_SUMPOSSALES"
  val GROUP_WEEK_BASESKU_AGG_STOREPOS_STOREINV = "GROUP_WEEK_BASESKU_AGG_STOREPOS_STOREINV"
  val JOIN_TOTALPOSSALESFORMULA_AND_INITIALGROUP = "JOIN_TOTALPOSSALESFORMULA_AND_INITIALGROUP"
  val JOIN_WALMART_LASTSELECT_AND_AUXTABLES = "JOIN_WALMART_LASTSELECT_AND_AUXTABLES"
  val JOIN_POS_AND_AUXTABLES_SELECT_EXCEPT_WED = "JOIN_POS_AND_AUXTABLES_SELECT_EXCEPT_WED"

  val WALMART = "WALMART"
  val AUX_TABLES_WEEKEND_SOURCE = "AUX_TABLES_WEEKEND"
  val EXISTING_POS_SOURCE = "EXISTING_POS"
  val TEMP_POS_RETAIL_PATH = "/etherData/retailTemp/retailAlteryx/temp_posqty_output_retail.csv"

  def execute(executionContext: ExecutionContext): Unit = {

    val currentTS = executionContext.spark.read.json("/etherData/state/currentTS.json").select("ts").head().getString(0)

    val sourceMap = executionContext.configuration.sources

    val walmartSource = sourceMap(WALMART)
    val walmart = Utils.loadCSV(executionContext, walmartSource.filePath).get

    val existingPOSSource = sourceMap(EXISTING_POS_SOURCE)
    val existingPOS = Utils.loadCSV(executionContext, existingPOSSource.filePath.format(currentTS)).get
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("Max_Week_End_Date", to_date(unix_timestamp(col("Max_Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))

    val auxTablesWeekendSource = sourceMap(AUX_TABLES_WEEKEND_SOURCE)
    val auxTablesWeekend = Utils.loadCSV(executionContext, auxTablesWeekendSource.filePath).get

    // walmart
    // select
    val walmartInitialSelectDF = doSelect(walmart, walmartSource.selectOperation(SELECT01).cols, walmartSource.selectOperation(SELECT01).isUnknown).get

    // filter
    val walmartFilterStoreType = walmartSource.filterOperation(WALMART_STORE_TYPE_FILTER)
    val walmartFilterStoreTypeDF = doFilter(walmartInitialSelectDF, walmartFilterStoreType, walmartFilterStoreType.conditionTypes(NUMERAL0)).get

    // formula
    val walmartFormulaExtractBaseSKU = walmartFilterStoreTypeDF.withColumn("Base SKU", substring(col("Vendor Stk/Part Nbr"), 0, 6))

    // group
    val walmartInitialGroup = walmartSource.groupOperation(GROUP_WEEK_BASESKU_STORENBR_AGG_POSQTY_POSSALES_STRONHANDQTY)
    val walmartInitialGroupDF = doGroup(walmartFormulaExtractBaseSKU, walmartInitialGroup).get

    // group
    val walmartGroupStoreNbrAndAggPOSQTYAndPOSSales = walmartSource.groupOperation(GROUP_STORENBR_AGG_SUMPOSQTY_SUMPOSSALES)
    val walmartGroupStoreNbrAndAggPOSQTYAndPOSSalesDF = doGroup(walmartInitialGroupDF, walmartGroupStoreNbrAndAggPOSQTYAndPOSSales).get

    val sumSumPOSQty = walmartGroupStoreNbrAndAggPOSQTYAndPOSSalesDF.agg(sum("Sum_Sum_POS QTY")).head().getLong(NUMERAL0)
    val sumSumPOSSales = walmartGroupStoreNbrAndAggPOSQTYAndPOSSalesDF.agg(sum("Sum_Sum_POS Sales")).head().getDouble(NUMERAL0)

    // formula
    val walmartTotalPOSSales = walmartGroupStoreNbrAndAggPOSQTYAndPOSSalesDF
      .withColumn("Total POS Sales", when(col("Sum_Sum_POS Sales") < 0, 0)
        .otherwise(col("Sum_Sum_POS Sales")))

    // join
    val joinTotalPOSSalesFormulaWithInitialGroup = walmartSource.joinOperation(JOIN_TOTALPOSSALESFORMULA_AND_INITIALGROUP)
    val joinTotalPOSSalesFormulaWithInitialGroupMap = doJoinAndSelect(walmartInitialGroupDF, walmartTotalPOSSales, joinTotalPOSSalesFormulaWithInitialGroup)
    val joinTotalPOSSalesFormulaWithInitialGroupDF = joinTotalPOSSalesFormulaWithInitialGroupMap(INNER_JOIN)

    // formula
    val walmartStorePOS = joinTotalPOSSalesFormulaWithInitialGroupDF
      .withColumn("Store POS", when(col("Sum_POS Qty") > 0, col("Total POS Sales"))
      .otherwise(lit(0)))
      .withColumn("Store Inv", when(col("Sum_Str On Hand Qty") > 0, col("Total POS Sales"))
        .otherwise(lit(0)))

    // group with append
    val walmartGroupWeekAndBaseSKUAndAggStorePOSStoreInv = walmartSource.groupOperation(GROUP_WEEK_BASESKU_AGG_STOREPOS_STOREINV)
    val walmartGroupWeekAndBaseSKUAndAggStorePOSStoreInvDF = doGroup(walmartStorePOS, walmartGroupWeekAndBaseSKUAndAggStorePOSStoreInv).get
      .withColumn("Sum_Sum_Sum_POS Qty", lit(sumSumPOSQty))
      .withColumn("Sum_Sum_Sum_POS Sales", lit(sumSumPOSSales))

    // formula
    val formulaForSKUACVPOSAndSKUACVINV = walmartGroupWeekAndBaseSKUAndAggStorePOSStoreInvDF
      .withColumn("SKU ACV POS", when(col("Sum_Sum_Sum_POS Sales") < 1, 0)
        .otherwise(when((col("Sum_Store POS") / col("Sum_Sum_Sum_POS Sales")) > 1, 1)
        .otherwise(col("Sum_Store POS") / col("Sum_Sum_Sum_POS Sales"))))
      .withColumn("SKU ACV Inv", when(col("Sum_Sum_Sum_POS Sales") < 1, 0)
        .otherwise(when((col("Sum_Store Inv") / col("Sum_Sum_Sum_POS Sales")) > 1, 1)
          .otherwise(col("Sum_Store Inv") / col("Sum_Sum_Sum_POS Sales"))))

    // select
//    val walmartLastSelectDF = Utils.convertListToDFColumnWithRename(walmartSource.renameOperation(RENAME_WED_PRODUCT_BASEID_AND_ACCOUNT_MAJOR_CONSOL),
//      doSelect(formulaForSKUACVPOSAndSKUACVINV, walmartSource.selectOperation(SELECT_LAST_WALMART).cols, walmartSource.selectOperation(SELECT_LAST_WALMART).isUnknown).get)

    // AUX TABLES WEEKEND
    val auxTablesWeekendselect01DF = doSelect(auxTablesWeekend, auxTablesWeekendSource.selectOperation(SELECT01).cols, auxTablesWeekendSource.selectOperation(SELECT01).isUnknown).get
      .withColumn("wed", to_date(col("wed")))

    // join
    val joinWalmartLastSelectWithAuxTables = walmartSource.joinOperation(JOIN_WALMART_LASTSELECT_AND_AUXTABLES)
    val joinWalmartLastSelectWithAuxTablesMap = doJoinAndSelect(formulaForSKUACVPOSAndSKUACVINV
      .withColumnRenamed("Base SKU", "SKU")
      .withColumnRenamed("SKU ACV Inv", "Distribution"), auxTablesWeekendselect01DF, joinWalmartLastSelectWithAuxTables)
    val joinWalmartLastSelectWithAuxTablesDF = joinWalmartLastSelectWithAuxTablesMap(INNER_JOIN)

    // formula
    val add7DaysToWEDFormula = joinWalmartLastSelectWithAuxTablesDF
      .withColumn("Week_End_Date", date_add(to_date(col("wed").cast("timestamp"), "yyyy-mm-dd"), 7))
      .withColumn("Account",  lit("Wal-Mart Online"))

    // select
    val walmartSelectExceptWedDF = doSelect(add7DaysToWEDFormula, walmartSource.selectOperation(SELECT_EXCEPT_WED).cols, walmartSource.selectOperation(SELECT_EXCEPT_WED).isUnknown).get

    // POS QTY SELECT
    val posQTYSelectALL = doSelect(existingPOS, existingPOSSource.selectOperation(SELECT_ALL).cols, existingPOSSource.selectOperation(SELECT_ALL).isUnknown).get

    // join
    val joinPOSWithAuxTablesSelectExceptWED = walmartSource.joinOperation(JOIN_POS_AND_AUXTABLES_SELECT_EXCEPT_WED)
    val joinPOSWithAuxTablesSelectExceptWEDMap = doJoinAndSelect(posQTYSelectALL, walmartSelectExceptWedDF
      .withColumnRenamed("Distribution", "Distribution_Inv"), joinPOSWithAuxTablesSelectExceptWED)
    val joinPOSWithAuxTablesSelectExceptWEDLeftDF = joinPOSWithAuxTablesSelectExceptWEDMap(LEFT_JOIN)
    val joinPOSWithAuxTablesSelectExceptWEDInnerDF = joinPOSWithAuxTablesSelectExceptWEDMap(INNER_JOIN)

    // union
    val unionFinal = doUnion(joinPOSWithAuxTablesSelectExceptWEDInnerDF, joinPOSWithAuxTablesSelectExceptWEDLeftDF).get

    // write
    unionFinal
      .select("Account","Online","SKU","SKU_Name","IPSLES","Week_End_Date","POS_Qty","Season","Street_Price",
        "Category","Category_Subgroup","Line","PL","L1_Category","L2_Category","Raw_POS_Qty","GA_date","ES_date",
        "Distribution_Inv","Category_1","Category_2","Category_3","HPS/OPS","Series","Category Custom","Brand",
        "Max_Week_End_Date","Season_Ordered","Cal_Month","Cal_Year","Fiscal_Qtr","Fiscal_Year")
      .write.mode(SaveMode.Overwrite).option("header", true).csv(TEMP_POS_RETAIL_PATH)


    val posRetail = Utils.loadCSV(executionContext, TEMP_POS_RETAIL_PATH).get
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("Max_Week_End_Date", to_date(unix_timestamp(col("Max_Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))

    // write
    posRetail
      .select("Account","Online","SKU","SKU_Name","IPSLES","Week_End_Date","POS_Qty","Season","Street_Price",
        "Category","Category_Subgroup","Line","PL","L1_Category","L2_Category","Raw_POS_Qty","GA_date","ES_date",
        "Distribution_Inv","Category_1","Category_2","Category_3","HPS/OPS","Series","Category Custom","Brand",
        "Max_Week_End_Date","Season_Ordered","Cal_Month","Cal_Year","Fiscal_Qtr","Fiscal_Year")
      .coalesce(1)
      .write.mode(SaveMode.Overwrite).option("header", true).csv("/etherData/Pricing/Outputs/POS_Retail/posqty_output_retail_" + currentTS + ".csv")
  }


}
