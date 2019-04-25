package com.scienaptic.jobs.core.npd.pc.monthly.transformations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object CATransformations {

  def withExchangeRates(df: DataFrame): DataFrame = {

    val spark = df.sparkSession

    val masterExchangeRates = spark.sql("select time_periods,ca_exchange_rate from ams_datamart_pc.tbl_master_exchange_rates")

    val withExchangeRates= df.join(masterExchangeRates,
      df("time_periods") === masterExchangeRates("time_periods"),"left")

    withExchangeRates
      .withColumnRenamed("ca_exchange_rate","ams_exchange_rate")
      .withColumn("ams_us_dollars",
        when(col("ams_exchange_rate") === 0,0)
          .otherwise(col("ams_temp_dollars")/col("ams_exchange_rate")))

  }



  /*
  This procedure updates AMS_Temp_Units,AMS_Temp_Dollars,AMS_ASP,AMS_AUP,unitsabs,dollarsabs
  */
  def withCAASP(df: DataFrame): DataFrame = {

    val cleanDf = df.withColumn("AMS_Temp_Units",
      when(
        (col("Units")>0) && (col("Dollars")>0),
        col("Units")
      ).otherwise(lit(0).cast(IntegerType)))

    val withTempDollers = cleanDf.withColumn("AMS_Temp_Dollars",
      when((col("Units")>0) && (col("Dollars")>0),col("Dollars")).otherwise(lit(0).cast(IntegerType)))


    val withASPDf =withTempDollers.withColumn("AMS_ASP_CA",
      when(col("AMS_Temp_Units")===0 ,lit(0).cast(IntegerType)).otherwise(col("AMS_Temp_Dollars")/col("AMS_Temp_Units")))
      .withColumn("units_abs",abs(col("units")))
      .withColumn("dollars_abs",abs(col("dollars")))
      .withColumn("AMS_ASP_US",
        when(col("AMS_Temp_Units")===0 ,lit(0).cast(IntegerType)).otherwise(col("ams_us_dollars")/col("AMS_Temp_Units")))
    //val withAUPDf = withASPDf.withColumn("ams_aup",col("ams_asp"))

    withASPDf

  }

  /*
  This procedure updates "AMS_VendorFamily"
  Stored PROC : Proc_Update_Master_Vendor.txt
  */
  def withVendorFamily(df: DataFrame): DataFrame = {

    val spark = df.sparkSession

    val masterBrandDf = spark.sql("select * from ams_datamart_pc.tbl_master_brand")

    val vendorFamilyDf = df.join(masterBrandDf,df("brand") === masterBrandDf("ams_vendorFamily")
      , "left")
      .drop("ams_vendorFamily")
        .withColumnRenamed("ams_brand","ams_vendorFamily")

    vendorFamilyDf
  }


  /*
  This procedure updates AMS_Top_Sellers,AMS_SmartBuy_TopSeller,
  AMS_SKU_DATE,AMS_TRANSACTIONAL-NONTRANSACTIONAL-SKUS

  Stored PROC : Proc_Update_Master_TopSeller_CA
  */

  def withCATopSellers(df: DataFrame): DataFrame = {

    val spark = df.sparkSession

    val tbl_Master_LenovoTopSellers = spark.sql("select sku,top_sellers from ams_datamart_pc.tbl_master_top_sellers_ca");

    val withTopSellers = df.join(tbl_Master_LenovoTopSellers,
      df("model")===tbl_Master_LenovoTopSellers("sku"),"left")
      .withColumn("ams_top_sellers",
          topSellersUDF(col("top_sellers")))
      .withColumn("ams_smartbuy_topseller",
          smartBuyTopSellersUDF(
            col("ams_smart_buys"),
            col("ams_top_sellers")))
      .withColumn("ams_smartbuy_lenovotopseller",
        LenovoSmartBuyTopSellersUDF(
          col("ams_smart_buys"),
          col("ams_top_sellers"),
          col("brand"),
          col("model")))
      .withColumn("ams_transactional-nontransactional-skus",
        transactionalNontransactionalSkusUDF(
          col("ams_smart_buys"),
          col("ams_top_sellers"),
          col("brand"),
          col("model")))

    withTopSellers

  }


  /*
  This procedure updates ams_catgrp,ams_npd_category,ams_sub_category,ams_sub_category_temp
  AMS_SKU_DATE,AMS_TRANSACTIONAL-NONTRANSACTIONAL-SKUS

  Stored PROC : Proc_Update_Master_Category_CA
  */

  def withCACategory(df: DataFrame): DataFrame = {

    val spark = df.sparkSession

    val masterCategoryDf = spark.sql("select * from ams_datamart_pc.tbl_master_category")

    val withCategory = df.join(masterCategoryDf,
      df("subcat")===masterCategoryDf("subcat"),"left")

    val finalCategoryDf = withCategory
      .drop("ams_sub_category")
      .drop("subcat")
      .drop("catgrp")
      .withColumnRenamed("catgory","ams_catgrp")
      .withColumnRenamed("npd_category","ams_npd_category")
      .withColumn("ams_sub_category",
        subCategoryUDF(col("moblwork"),col("subcat")))
      .withColumn("ams_sub_category_temp",
        subCategoryTempUDF(col("moblwork"),col("subcat")))

    finalCategoryDf

  }


  /*
  This procedure updates AMS_CDW_OS,AMS_CDW_PRICE

  Stored PROC : Proc_Update_Master_ParsehUB_CDW
  */

  def withCDW(df: DataFrame): DataFrame = {

    val spark = df.sparkSession

    val masterParsehubCDW = spark.sql("select sku,windows,price from ams_datamart_pc.tbl_master_parsehub_cdw")

    val withCDW= df.join(masterParsehubCDW,
      df("model")===masterParsehubCDW("sku"),"left")

    val finalDf = withCDW
      .drop(masterParsehubCDW("sku"))
      .withColumnRenamed("windows","ams_cdw_os")
      .withColumnRenamed("price","ams_cdw_price")

    finalDf

  }


  /*
  This procedure updates AMS_Focus,AMS_Lenovo_Focus,AMS_Lenovo_System_Type
  ,AMS_Lenovo_Form_Factor,AMS_Lenovo_List_Price

  Stored PROC : Proc_Update_Master_TopSeller_LenovoFocus_MONTHLY
  */

  def withLenovoFocus(df: DataFrame): DataFrame = {

    val spark = df.sparkSession

    val masterLenovoTopSellers = spark
      .sql("select ams_sku_date,focus,system_type,form_factor,pricing_list_price from ams_datamart_pc.tbl_master_lenovotopsellers")

    val withLenovoFocus= df.join(masterLenovoTopSellers,
      df("ams_sku_date")===masterLenovoTopSellers("ams_sku_date"),"left")

    val finalDf = withLenovoFocus
      .withColumnRenamed("focus","ams_focus")
      .withColumnRenamed("system_type","ams_lenovo_system_type")
      .withColumnRenamed("form_factor","ams_lenovo_form_factor")
      .withColumnRenamed("pricing_list_price","ams_lenovo_list_price")
      .withColumn("ams_lenovo_focus",
        lenovoFocusUDF(col("ams_focus")))

    finalDf

  }

  def withOSGroup(df: DataFrame): DataFrame = {

    val spark = df.sparkSession

    val masterOS = spark
      .sql("select ams_os_detail,ams_os_name_chrome_win_mac from ams_datamart_pc.tbl_master_os")

    val withOSGroup= df.join(masterOS,
      df("op_sys")===masterOS("ams_os_detail"),"left")

    val finalDf = withOSGroup
      .withColumnRenamed("ams_os_name_chrome_win_mac","ams_os_group")

    finalDf

  }

  def withCAPriceBand(df: DataFrame): DataFrame = {

    val spark = df.sparkSession

    val masterPriceBand = spark.sql("select price_band,price_band_map,pb_less,pb_high from ams_datamart_pc.tbl_master_priceBand")

    val withPriceBand= df.join(masterPriceBand,
      df("AMS_ASP_CA") >= masterPriceBand("PB_LESS") && df("AMS_ASP_CA") < masterPriceBand("PB_HIGH")
      ,"left")

    withPriceBand
        .drop("pb_less")
        .drop("pb_high")
      .withColumnRenamed("price_band","ams_price_band_ca")
      .withColumnRenamed("price_band_map","ams_price_band_map_ca")


  }



}
