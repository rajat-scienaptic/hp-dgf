package com.scienaptic.jobs.core.npd.pc.weekly.transformations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object USWeeklyTransformations {

  def withWeeksToDisplay(df :DataFrame) : DataFrame ={
    val spark = df.sparkSession

    val master_Month = spark.sql("select * from ams_datamart_pc.Tbl_Master_Month");

    val withTempMonth = df.withColumn("temp_month",substring(col("time_periods"),15,3))

    val joinedDf = withTempMonth.join(master_Month,
      lower(withTempMonth("temp_month"))===lower(master_Month("month_name")),"inner")

    val withNewDate = joinedDf
      .withColumn("ams_newdate",
      concat(col("month_number"),
        lit("/"),
        substring(col("time_periods"),19,2),
        lit("/"),
        substring(col("time_periods"),22,4)
      ))
      .withColumn("ams_qtr",
      concat(substring(col("time_periods"),22,4),
        col("calendar_quarter")
      ))
      .withColumn("ams_qtrweek",
        concat(substring(col("time_periods"),22,4),
          weekofyear(col("ams_newdate"))
        )
      )


    joinedDf
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
      .withColumn("AMS_CDW_FormFactor",col("form_factor"))
      .drop("form_factor")
      .drop(master_cdw_formfactor("model"))
      .na.fill("NA",Seq("AMS_CDW_FormFactor"))

  }

  def withTopVendors(df :DataFrame) : DataFrame ={

    val spark = df.sparkSession

    val master_TopVendors = spark.sql("select ams_top_vendors from ams_datamart_pc.tbl_master_topvendors")

    val withTopVendors = df.join(master_TopVendors,
      df("ams_vendorfamily")===master_TopVendors("ams_top_vendors"),"left")

    withTopVendors
      .drop(master_TopVendors("ams_top_vendors")).na.fill("All Others",Seq("ams_top_vendors"))

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
