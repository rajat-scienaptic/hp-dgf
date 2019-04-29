package com.scienaptic.jobs.core.npd.pc.monthly.transformations

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object CommonTransformations {
  /*
  This function updates AMS_L3M,AMS_L6M,AMS_L12M,AMS_L13M,AMS_Year,AMS_Quarter,AMS_Year_Quarter,
  AMS_Year_Quarter_(Fiscal),AMS_QTD_Current/Prior,AMS_Promo_Season,AMS_CURRENT/PRIOR

  Stored PROC : Proc_Monthly_Update_Master_CalendarDetails
  Master Table : Tbl_Master_L3M_L12M_L13M
  */

  //TODO
  def withCalenderDetails(df: DataFrame): DataFrame = {
    val spark = df.sparkSession;

    val tmpTblMasterMonthNum = df.select("time_periods").distinct()

    val masterMonthNumRawData = tmpTblMasterMonthNum
      .withColumn("rowmonthnum",
        row_number().over(
          Window.orderBy(col("time_periods").desc)
        ).alias("row_num"))
      .withColumn("year",year(col("time_periods")))
      .withColumn("quarter",concat(lit("Q"),quarter(col("time_periods"))))
      .withColumn("monthnum",month(col("time_periods")))
      .withColumn("fiscalquarter",
        concat(col("year"),col("quarter")))


    val tbl_Master_L3M_L12M_L13M = spark.sql("select * from ams_datamart_pc.tbl_master_l3m_l12m_l13m")

    val withLValues =  masterMonthNumRawData.join(tbl_Master_L3M_L12M_L13M,
      masterMonthNumRawData("rowmonthnum")===tbl_Master_L3M_L12M_L13M("monthnum"),"left")
      .drop(tbl_Master_L3M_L12M_L13M("monthnum"))
      .drop(tbl_Master_L3M_L12M_L13M("qtd"))


    val withcp = withLValues.withColumn("ams_current/prior",currentPriorUDF(col("rowmonthnum")))

    val withqtd = withcp.withColumn("qtd_current/prior",qtdCurrentPriorUDF(col("rowmonthnum"),col("FiscalQuarter")))

    val tbl_Master_Month = spark.sql("select * from ams_datamart_pc.tbl_master_month_pc")

    val withMasterMonth = withqtd.join(tbl_Master_Month,
      withqtd("monthnum")===tbl_Master_Month("month_number"),"left")
      .drop(tbl_Master_Month("month_number"))
      .drop(tbl_Master_Month("month_name"))
      .drop(tbl_Master_Month("calendar_quarter"))
      .withColumnRenamed("l3m","ams_l3m")
      .withColumnRenamed("l6m","ams_l6m")
      .withColumnRenamed("l12m","ams_l12m")
      .withColumnRenamed("l13m","ams_l13m")
      .withColumnRenamed("l13m","ams_l13m")
      .withColumnRenamed("year","ams_year")
      .withColumnRenamed("quarter","ams_quarter")
      .withColumnRenamed("qtd_current/prior","ams_qtd_current/prior")
      .withColumnRenamed("fiscalquarter","ams_year_quarter")
      .withColumnRenamed("hpcalendar_quarter","ams_year_quarter_fiscal")
      .withColumnRenamed("promo_season","ams_promo_season")
      .na.fill("-")


    val onlyAMS = withMasterMonth.select("time_periods",
      "ams_year","ams_quarter","ams_l3m","ams_l6m","ams_l12m","ams_l13m",
      "ams_qtd_current/prior","ams_year_quarter","ams_year_quarter_fiscal"
      ,"ams_promo_season")


    val finalDf = df.join(onlyAMS,df("time_periods")===onlyAMS("time_periods"),"left")
      .drop(onlyAMS("time_periods"))

    finalDf

  }

  /*
  This procedure updates AMS_Smart_Buys
  Stored PROC : Proc_Update_Master_SmartBuy/Proc_Update_Master_SmartBuy_CA
  */

  def withSmartBuy(df: DataFrame): DataFrame = {

    val spark = df.sparkSession

    val withSmartBuysDf = df.withColumn("ams_smart_buys",
      smartBuysUDF(col("brand"),col("model")))

    withSmartBuysDf
  }


  def withOSGroup(df: DataFrame): DataFrame = {

    val spark = df.sparkSession

    val masterOS = spark
      .sql("select ams_os_detail,ams_os_name_chrome_win_mac from ams_datamart_pc.tbl_master_os" +
        " group by ams_os_detail,ams_os_name_chrome_win_mac")

    val withOSGroup= df.join(masterOS,
      lower(df("op_sys"))===lower(masterOS("ams_os_detail")),"left")

    val finalDf = withOSGroup
      .withColumnRenamed("ams_os_name_chrome_win_mac","ams_os_group")

    finalDf

  }

}
