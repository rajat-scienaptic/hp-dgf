package com.scienaptic.jobs.core.npd.pc.weekly.transformations

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

object USWeeklyTransformations {

  def withWeeksToDisplay(df :DataFrame) : DataFrame ={
    val spark = df.sparkSession

    val master_Month = spark.sql("select * from ams_datamart_pc.tbl_master_month_pc");

    val withTempMonth = df.withColumn("temp_month",upper(substring(col("time_periods"),15,3)))

    val joinedDf = withTempMonth.join(master_Month, withTempMonth("temp_month")===trim(master_Month("month_name")),"left")

    val withNewDate = joinedDf
      .withColumn("ams_newdate",
        to_date(
          concat(col("month_number"),
          lit("/"),
          substring(col("time_periods"),19,2),
          lit("/"),
          substring(col("time_periods"),22,4)
          ),
          "MM/dd/yyyy")
      )
      .withColumn("ams_qtr",
      concat(substring(col("time_periods"),22,4),
        col("calendar_quarter")
      ))
      .withColumn("ams_qtrweek",
        concat(substring(col("time_periods"),22,4),
          weekofyear(col("ams_newdate"))
        )
      ).drop("temp_month")

    val dataLoadDayCount = 80*7
    val weeksDisplayDayCount = 6*7

    val maxDatLoadDF = withNewDate.select(date_add(max("ams_newdate"),-dataLoadDayCount) as "data_load_maxdate")
    val maxWeeksDisplayDF = withNewDate.select(date_add(max("ams_newdate"),-weeksDisplayDayCount) as "weeks_display_maxdate")

    /*val withMaxDate = withNewDate
      .withColumn("data_load_maxdate",date_add(max("ams_newdate"),-dataLoadDayCount))
      .withColumn("weeks_display_maxdate",date_add(max("ams_newdate"),-weeksDisplayDayCount))
      .withColumn("ams_datatoload",
        when(col("ams_newdate") > col("data_load_maxdate"),"T").otherwise("F"))
      .withColumn("ams_weekstodisplay",
        when(col("ams_newdate") > col("weeks_display_maxdate"),"T").otherwise("F"))
*/
    val withMaxDate = withNewDate.crossJoin(maxDatLoadDF).crossJoin(maxWeeksDisplayDF)
      .withColumn("ams_datatoload",
        when(col("ams_newdate") > col("data_load_maxdate"),"T").otherwise("F"))
      .withColumn("ams_weekstodisplay",
        when(col("ams_newdate") > col("weeks_display_maxdate"),"T").otherwise("F"))

    withMaxDate

  }


  def withItemDescription(df :DataFrame) : DataFrame ={
    df
      .withColumn("ams_vendorfamily",col("brand"))
      .withColumn("ams_item_model_description",concat(col("model"),col("item_description")))
  }

  def withCDWFormFactor(df :DataFrame) : DataFrame ={

    val spark = df.sparkSession

    val master_cdw_formfactor = spark.sql("select model,form_factor from ams_datamart_pc.tbl_master_cdw_formfactor")

    val withFormFactor = df.join(master_cdw_formfactor,
      df("model")===master_cdw_formfactor("model"),"left")

    withFormFactor
      .withColumn("ams_cdw_formfactor",col("form_factor"))
      .drop("form_factor")
      .drop(master_cdw_formfactor("model"))
      .na.fill("NA",Seq("ams_cdw_formfactor"))

  }

  def withTopVendors(df :DataFrame) : DataFrame ={

    val spark = df.sparkSession

    val master_TopVendors = spark.sql("select ams_top_vendors from ams_datamart_pc.tbl_master_topvendors")

    val withTopVendors = df.join(master_TopVendors,
      df("ams_vendorfamily")===master_TopVendors("ams_top_vendors"),"left")

    withTopVendors.na.fill("All Others",Seq("ams_top_vendors"))

  }

  def withOSDetails (df :DataFrame) : DataFrame ={

    val spark = df.sparkSession

    val master_os = spark.sql("select ams_os_detail,ams_os_group,ams_os_sub_group,ams_os_name,ams_os_name_chrome,ams_os_name_chrome_win_mac from ams_datamart_pc.tbl_master_os")

    val withOS = df.join(master_os,
      df("op_sys")===master_os("ams_os_detail"),"left")

    withOS
      .drop(master_os("ams_os_detail"))

  }




}
