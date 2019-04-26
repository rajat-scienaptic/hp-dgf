package com.scienaptic.jobs.core.npd.pc.monthly.transformations

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.core.npd.common.CommonTransformations._
import com.scienaptic.jobs.core.npd.pc.monthly.transformations.USTransformations._
import com.scienaptic.jobs.core.npd.pc.monthly.transformations.CommonTransformations._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}

object USTransformer {

  val all_columns = Set("time_periods",
    "industry",
    "industry_segment",
    "category_group",
    "category",
    "sub_category",
    "brand",
    "model",
    "item_description",
    "all_in_one",
    "battery_cell",
    "battery_run_time_in_hours",
    "biometric",
    "blu_ray_technology",
    "bluetooth",
    "cellular_ready_technology",
    "consumer_commercial",
    "density_optimized",
    "device_color",
    "digital_connector",
    "display_resolution",
    "display_size",
    "display_type",
    "flash_drive_capacity",
    "gps_included",
    "gaming",
    "graphics_controller",
    "graphics_controller_brand",
    "graphics_memory",
    "hd_form_factor",
    "hard_drive_disk_size",
    "height_inches",
    "included_drives",
    "egrated_camera",
    "ernal_memory",
    "keyboard_style",
    "mhz",
    "max_processors",
    "number_of_processors_included",
    "op_sys",
    "pc_family",
    "pc_form",
    "pc_memory",
    "pc_technology",
    "pc_technology_family",
    "processor_brand_name",
    "processor_core",
    "rack_units",
    "refurbished",
    "removable_media_type",
    "screen_finish",
    "screen_format",
    "storage_drive_type",
    "touchscreen_display",
    "usb_type_c_connector",
    "weight_lbs#",
    "wireless_display_erface",
    "wireless_wan",
    "wireless_technology",
    "destination_channel",
    "channel_level_0",
    "dollars",
    "units",
    "ams_temp_units",
    "ams_temp_dollars",
    "ams_asp",
    "ams_quarter",
    "ams_vendorfamily",
    "ams_screen_size_group",
    "ams_os_group",
    "ams_smart_buys",
    "ams_top_sellers",
    "ams_smartbuy_topseller",
    "transactional_nontransactional_skus",
    "ams_aup",
    "ams_focus",
    "ams_lenovo_focus",
    "ams_lenovo_system_type",
    "ams_lenovo_form_factor",
    "ams_lenovo_list_price",
    "ams_lenovo_topseller_transitions",
    "ams_hp_form_factor",
    "ams_channel",
    "ams_y_n",
    "ams_price_band",
    "ams_price_band_map",
    "ams_price_band_100",
    "ams_source",
    "channel_level_2_total_retail",
    "mobile_workstation",
    "ams_l3m",
    "ams_l6m",
    "ams_l12m",
    "ams_l13m",
    "ams_cdw_os",
    "ams_cdw_price",
    "ams_year_quarter",
    "ams_year_quarter_fiscal",
    "ams_qtd_current_prior",
    "ams_promo_season",
    "ams_current_prior",
    "ams_sub_category",
    "map_price_band_detailed",
    "map_price_band4",
    "ams_sub_category_temp",
    "ams_catgrp",
    "ams_npd_category",
    "pen_stylus",
    "pen_stylus_technology",
    "headphone_series",
    "ams_sku_date",
    "ams_transactional_nontransactional_skus",
    "ams_newdate",
    "ams_year")

  def withAllTransformations(df : DataFrame) = {

    val cleanUpDollers = (str : String) => {
      str.replace("$","").replace(",","").toDouble
    }

    def cleanDollersUDF = udf(cleanUpDollers)

    val finalDF = df
      .transform(timePeriodsToDate)
      .transform(withCalenderDetails)
      .withColumn("tmp_dollars",
      cleanDollersUDF(col("dollars")))
      .drop("dollars")
      .withColumnRenamed("tmp_dollars","dollars")
      .transform(withASP)
      .transform(withSmartBuy)
      //.transform(withTopSellers)
      //.transform(withLenovoFocus)
      .transform(withVendorFamily)
      .transform(withCategory)
      //.transform(withCDW)
      .transform(withOSGroup)
      .transform(withPriceBand)

    finalDF

  }

  def execute(executionContext: ExecutionContext): Unit = {

    val spark = executionContext.spark

//    spark.conf.set("hive.exec.dynamic.partition", "true")
//    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    val DATAMART = "npd_sandbox"
    val TABLE_NAME = "fct_tbl_us_monthly_pc"

    val DM_PC_US_Mth_Reseller_STG = "Stg_DM_PC_US_Mth_Reseller"
    val DM_PC_US_Mth_Reseller_BTO_STG = "Stg_DM_PC_US_Mth_Reseller_BTO"
    val DM_US_PC_Monthly_Dist_STG = "Stg_DM_US_PC_Monthly_Dist"
    val DM_US_PC_Monthly_Dist_BTO_STG = "Stg_DM_US_PC_Monthly_Dist_BTO"
    val DM_US_PC_Monthly_Retail_STG = "Stg_DM_US_PC_Monthly_Retail"

    val USMthReseller_stg  = spark.sql("select * from "+DATAMART+"."+DM_PC_US_Mth_Reseller_STG)
      .withColumn("ams_source",lit("Reseller"))
    val USMthResellerBTO_stg  = spark.sql("select * from "+DATAMART+"."+DM_PC_US_Mth_Reseller_BTO_STG)
      .withColumn("ams_source",lit("ResellerBTO"))
    val USMthDist_stg  = spark.sql("select * from "+DATAMART+"."+DM_US_PC_Monthly_Dist_STG)
      .withColumn("ams_source",lit("Dist"))
    val USMthDistBTO_stg  = spark.sql("select * from "+DATAMART+"."+DM_US_PC_Monthly_Dist_BTO_STG)
      .withColumn("ams_source",lit("DistBTO"))
    val USMthRetail_stg  = spark.sql("select * from "+DATAMART+"."+DM_US_PC_Monthly_Retail_STG)
      .withColumn("ams_source",lit("Retail"))


    val USMthReseller_int  = USMthReseller_stg.transform(withAllTransformations)
      .withColumn("ams_source",lit("Reseller"))

    USMthReseller_int.write.mode(SaveMode.Overwrite)
      .saveAsTable(DATAMART+"."+"Int_Fact_DM_PC_US_Mth_Reseller");

    val USMthResellerBTO_int  = USMthResellerBTO_stg.transform(withAllTransformations)
      .withColumn("ams_source",lit("ResellerBTO"))

    USMthResellerBTO_int.write.mode(SaveMode.Overwrite)
      .saveAsTable(DATAMART+"."+"Int_Fact_DM_PC_US_Mth_Reseller_BTO");

    val USMthDist_int  = USMthDist_stg.transform(withAllTransformations)
      .withColumn("ams_source",lit("Dist"))

    USMthDist_int.write.mode(SaveMode.Overwrite)
      .saveAsTable(DATAMART+"."+"Int_Fact_DM_US_PC_Monthly_Dist");


    val USMthDistBTO_int  = USMthDistBTO_stg.transform(withAllTransformations)
      .withColumn("ams_source",lit("DistBTO"))

    USMthDistBTO_int.write.mode(SaveMode.Overwrite)
      .saveAsTable(DATAMART+"."+"Int_Fact_DM_US_PC_Monthly_Dist_BTO");


    val USMthRetail_int = USMthRetail_stg.transform(withAllTransformations)
      .withColumn("ams_source",lit("Retail"))

    USMthRetail_int.write.mode(SaveMode.Overwrite)
      .saveAsTable(DATAMART+"."+"Int_Fact_DM_US_PC_Monthly_Retail");


    val cols1 = USMthReseller_int.columns.toSet
    val cols2 = USMthResellerBTO_int.columns.toSet
    val cols3 = USMthDist_int.columns.toSet
    val cols4 = USMthDistBTO_int.columns.toSet
    val cols5 = USMthRetail_int.columns.toSet

    def missingToNull(myCols: Set[String]) = {
      all_columns.toList.map(x => x match {
        case x if myCols.contains(x) => col(x)
        case _ => lit("NA").as(x)
      })
    }

    val final_fact_tbl = USMthReseller_int
      .select(missingToNull(cols1):_*)
      .union(USMthResellerBTO_int
        .select(missingToNull(cols2):_*))
      .union(USMthDist_int
        .select(missingToNull(cols3):_*))
      .union(USMthDistBTO_int
        .select(missingToNull(cols4):_*))
      .union(USMthRetail_int
        .select(missingToNull(cols5):_*))

    final_fact_tbl.createOrReplaceTempView("final_fact")

    spark.sql(s"""
                 |INSERT OVERWRITE TABLE npd_sandbox.fct_tbl_us_monthly_pc
                 |PARTITION(ams_year)
                 |SELECT
                 |time_periods,
                 |industry,
                 |industry_segment,
                 |category_group,
                 |category,
                 |sub_category,
                 |brand,
                 |model,
                 |item_description,
                 |all_in_one,
                 |battery_cell,
                 |battery_run_time_in_hours,
                 |biometric,
                 |blu_ray_technology,
                 |bluetooth,
                 |cellular_ready_technology,
                 |consumer_commercial,
                 |density_optimized,
                 |device_color,
                 |digital_connector,
                 |display_resolution,
                 |display_size,
                 |display_type,
                 |flash_drive_capacity,
                 |gps_included,
                 |gaming,
                 |graphics_controller,
                 |graphics_controller_brand,
                 |graphics_memory,
                 |hd_form_factor,
                 |hard_drive_disk_size,
                 |height_inches,
                 |included_drives,
                 |egrated_camera,
                 |ernal_memory,
                 |keyboard_style,
                 |mhz,
                 |max_processors,
                 |number_of_processors_included,
                 |op_sys,
                 |pc_family,
                 |pc_form,
                 |pc_memory,
                 |pc_technology,
                 |pc_technology_family,
                 |processor_brand_name,
                 |processor_core,
                 |rack_units,
                 |refurbished,
                 |removable_media_type,
                 |screen_finish,
                 |screen_format,
                 |storage_drive_type,
                 |touchscreen_display,
                 |usb_type_c_connector,
                 |`weight_lbs#`,
                 |wireless_display_erface,
                 |wireless_wan,
                 |wireless_technology,
                 |destination_channel,
                 |channel_level_0,
                 |dollars,
                 |units,
                 |ams_temp_units,
                 |ams_temp_dollars,
                 |ams_asp,
                 |ams_quarter,
                 |ams_vendorfamily,
                 |ams_screen_size_group,
                 |ams_os_group,
                 |ams_smart_buys,
                 |ams_top_sellers,
                 |ams_smartbuy_topseller,
                 |transactional_nontransactional_skus,
                 |ams_aup,
                 |ams_focus,
                 |ams_lenovo_focus,
                 |ams_lenovo_system_type,
                 |ams_lenovo_form_factor,
                 |ams_lenovo_list_price,
                 |ams_lenovo_topseller_transitions,
                 |ams_hp_form_factor,
                 |ams_channel,
                 |ams_y_n,
                 |ams_price_band,
                 |ams_price_band_map,
                 |ams_price_band_100,
                 |ams_source,
                 |channel_level_2_total_retail,
                 |mobile_workstation,
                 |ams_l3m,
                 |ams_l6m,
                 |ams_l12m,
                 |ams_l13m,
                 |ams_cdw_os,
                 |ams_cdw_price,
                 |ams_year_quarter,
                 |ams_year_quarter_fiscal,
                 |ams_qtd_current_prior,
                 |ams_promo_season,
                 |ams_current_prior,
                 |ams_sub_category,
                 |map_price_band_detailed,
                 |map_price_band4,
                 |ams_sub_category_temp,
                 |ams_catgrp,
                 |ams_npd_category,
                 |pen_stylus,
                 |pen_stylus_technology,
                 |headphone_series,
                 |ams_sku_date,
                 |ams_transactional_nontransactional_skus,
                 |ams_newdate,
                 |ams_year
                 |FROM final_fact
    """.stripMargin)

  }


}
