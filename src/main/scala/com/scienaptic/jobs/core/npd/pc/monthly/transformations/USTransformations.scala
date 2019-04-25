package com.scienaptic.jobs.core.npd.pc.monthly.transformations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object USTransformations {

  /*
  This procedure updates AMS_Temp_Units,AMS_Temp_Dollars,AMS_ASP,AMS_AUP,unitsabs,dollarsabs
  */
  def withASP(df: DataFrame): DataFrame = {

    val cleanDf = df.withColumn("AMS_Temp_Units",
      when(
        (col("Units")>0) && (col("Dollars")>0),
        col("Units")
      ).otherwise(lit(0).cast(IntegerType)))

    val withTempDollers = cleanDf.withColumn("AMS_Temp_Dollars",
      when((col("Units")>0) && (col("Dollars")>0),col("Dollars")).otherwise(lit(0).cast(IntegerType)))


    val withASPDf =withTempDollers.withColumn("AMS_ASP",
      when(col("AMS_Temp_Units")===0 ,lit(0).cast(IntegerType)).otherwise(col("AMS_Temp_Dollars")/col("AMS_Temp_Units")))
      .withColumn("units_abs",abs(col("units")))
      .withColumn("dollars_abs",abs(col("dollars")))

    val withAUPDf = withASPDf.withColumn("ams_aup",col("ams_asp"))

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
        .withColumn("ams_vendorFamily",
          when(col("ams_brand").isNull,col("brand"))
            .otherwise(col("ams_brand")))

    vendorFamilyDf
  }

  /*
  This procedure updates AMS_Top_Sellers,AMS_SmartBuy_TopSeller,
  AMS_SKU_DATE,AMS_TRANSACTIONAL-NONTRANSACTIONAL-SKUS

  Stored PROC : Proc_MONTHLY_Update_Master_TopSeller
  */

  def withTopSellers(df: DataFrame): DataFrame = {

    val spark = df.sparkSession

    val Tbl_Master_LenovoTopSellers = spark.sql("select * from ams_datamart_pc.tbl_master_lenovotopsellers limit 10");

    val masterWithSkuDate = Tbl_Master_LenovoTopSellers.withColumn("ams_sku_date_temp",
      skuDateUDF(col("sku"),col("ams_month")))
      .select("top_seller","ams_sku_date_temp")
      .withColumnRenamed("ams_sku_date_temp","ams_sku_date")

    val dfWithSKUDate = df.withColumn("ams_sku_date",
      skuDateUDF(col("model"),col("time_periods")))

    val withTopSellers = dfWithSKUDate.join(masterWithSkuDate,
      dfWithSKUDate("ams_sku_date")===masterWithSkuDate("ams_sku_date"),"left")
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

  Stored PROC : Proc_Update_Master_Category
  */

  def withCategory(df: DataFrame): DataFrame = {

    val spark = df.sparkSession

    val masterCategoryDf = spark.sql("select * from ams_datamart_pc.tbl_master_category")

    val withCategory = df.join(masterCategoryDf,
      df("sub_category")===masterCategoryDf("subcat"),"left")

    val finalCategoryDf = withCategory
      .drop("ams_sub_category")
      .drop("subcat")
      .drop("catgrp")
      .withColumnRenamed("catgory","ams_catgrp")
      .withColumnRenamed("npd_category","ams_npd_category")
      .withColumn("ams_sub_category",
        subCategoryUDF(col("mobile_workstation"),col("sub_category")))
      .withColumn("ams_sub_category_temp",
        subCategoryTempUDF(col("mobile_workstation"),col("sub_category")))

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

  def withPriceBand(df: DataFrame): DataFrame = {

    val spark = df.sparkSession

    val masterPriceBand = spark.sql("select price_band,price_band_map,pb_less,pb_high from ams_datamart_pc.tbl_master_priceBand")

    val withPriceBand= df.join(masterPriceBand,
      df("AMS_ASP") >= masterPriceBand("PB_LESS") && df("AMS_ASP") < masterPriceBand("PB_HIGH")
      ,"left")

    withPriceBand
        .drop("pb_less")
        .drop("pb_high")
      .withColumnRenamed("price_band","ams_price_band")
      .withColumnRenamed("price_band_map","ams_price_band_map")

  }

}
