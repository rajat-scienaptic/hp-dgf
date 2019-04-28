package com.scienaptic.jobs.core.npd.pc.monthly.transformations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object CATransformations {

  def withExchangeRates(df: DataFrame): DataFrame = {

    val spark = df.sparkSession

    val cleanDf = df.withColumn("ams_temp_units",
      when(
        (col("Units")>0) && (col("Dollars")>0),
        col("Units")
      ).otherwise(lit(0).cast(IntegerType)))

    val withTempDollers = cleanDf.withColumn("ams_temp_dollars",
      when((col("Units")>0) && (col("Dollars")>0),col("Dollars")).otherwise(lit(0).cast(IntegerType)))


    val masterExchangeRates = spark.sql("select `time_period(s)` as time_periods,ca_exchange_rate from ams_datamart_pc.tbl_master_exchange_rates")

    val withExchangeRates= withTempDollers.join(masterExchangeRates,
      withTempDollers("time_periods") === masterExchangeRates("time_periods"),"left")

    withExchangeRates
        .drop(masterExchangeRates("time_periods"))
      .withColumnRenamed("ca_exchange_rate","ams_exchange_rate")
      .withColumn("ams_us_dollars",
        when(col("ams_exchange_rate") === 0,0)
          .otherwise(col("ams_temp_dollars")/col("ams_exchange_rate")))

  }


  /*
  This procedure updates AMS_Temp_Units,AMS_Temp_Dollars,AMS_ASP,,unitsabs,dollarsabs
  */
  def withCAASP(df: DataFrame): DataFrame = {

    val withASPDf =df.withColumn("ams_asp_ca",
      when(col("ams_temp_units")===0 ,lit(0).cast(IntegerType)).otherwise(col("ams_temp_dollars")/col("ams_temp_units")))
      .withColumn("units_abs",abs(col("units")))
      .withColumn("dollars_abs",abs(col("dollars")))
      .withColumn("ams_asp_us",
        when(col("ams_temp_units")===0 ,lit(0).cast(IntegerType)).otherwise(col("ams_us_dollars")/col("ams_temp_units")))


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

    val masterCategoryDf = spark.sql("select subcat,catgory,npd_category from ams_datamart_pc.tbl_master_category")

    val withCategory = df.join(masterCategoryDf,
      df("subcat")===masterCategoryDf("subcat"),"left")

    val finalCategoryDf = withCategory
      .drop(masterCategoryDf("subcat"))
      .withColumn("ams_catgrp",masterCategoryDf("catgory"))
      .drop(masterCategoryDf("catgory"))
      .withColumn("ams_npd_category",
        when(col("npd_category").isNull,"Workstation").otherwise(col("npd_category")))
      .drop("npd_category")
      .withColumn("ams_sub_category",
        subCategoryUDF(col("moblwork"),col("subcat")))
      .withColumn("ams_sub_category_temp",
        subCategoryTempUDF(col("moblwork"),col("subcat")))

    finalCategoryDf

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
      df("ams_asp_ca") >= masterPriceBand("PB_LESS") && df("ams_asp_ca") < masterPriceBand("PB_HIGH")
      ,"left")

    withPriceBand
        .drop("pb_less")
        .drop("pb_high")
      .withColumnRenamed("price_band","ams_price_band_ca")
      .withColumnRenamed("price_band_map","ams_price_band_map_ca")


  }



}
