package com.scienaptic.jobs.core.npd.pc.weekly.transformations

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.NPDUtility
import org.apache.spark.sql.functions._

object CATransformer {

  def execute(executionContext: ExecutionContext): Unit = {

    val spark = executionContext.spark

    val SANDBOX_DATAMART = "npd_sandbox"
    val AMS_DATAMART = "ams_datamart_pc"
    val TABLE_NAME = "fct_tbl_ca_weekly_pc"

    val DM_PC_CA_Weekly_Retail = "DM_PC_CA_Weekly_Retail"

    val CAMthRetail_int = spark.sql("select * from "+SANDBOX_DATAMART+".Stg_"+DM_PC_CA_Weekly_Retail)

    val historicalFact = spark.sql("select * from npd_sandbox.fct_tbl_ca_weekly_pc_historical")

    val cols1 = CAMthRetail_int.columns.toSet
    val historic_columns = historicalFact.columns.toSet

    val all_columns = historic_columns ++ cols1 // union

    def missingToNull(myCols: Set[String]) = {
      all_columns.toList.map(x => x match {
        case x if myCols.contains(x) => col(x)
        case _ => lit("NA").as(x)
      })
    }

    val finalDf = historicalFact.select(missingToNull(historic_columns):_*)
      .union(CAMthRetail_int.select(missingToNull(cols1):_*))

    NPDUtility.writeToDataMart(spark,finalDf,AMS_DATAMART,TABLE_NAME)

  }

}
