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


    val masterExchangeRates = spark.sql("select cast(`time_period(s)` as date) as time_periods,ca_exchange_rate from ams_datamart_pc.tbl_master_exchange_rates")

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
  This procedure updates ams_asp_ca,ams_asp_us
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
  This procedure updates "ams_vendorfamily"
  Stored PROC : Proc_Update_Master_Vendor.txt
  */

  def withVendorFamily(df: DataFrame): DataFrame = {

    val spark = df.sparkSession

    val masterBrandDf = spark.sql("select * from ams_datamart_pc.tbl_master_brand")

    val vendorFamilyDf = df.join(masterBrandDf,df("brand") === masterBrandDf("ams_vendorfamily")
      , "left")
      .drop("ams_vendorfamily")
      .withColumn("ams_vendorfamily",
        when(col("ams_brand").isNull,col("brand"))
          .otherwise(col("ams_brand")))

    vendorFamilyDf
  }


  /*
  This procedure updates AMS_Top_Sellers,AMS_SmartBuy_TopSeller,
  AMS_SKU_DATE,AMS_TRANSACTIONAL-NONTRANSACTIONAL-SKUS

  Stored PROC : Proc_MONTHLY_Update_Master_TopSeller
  */

  def withCATopSellers(df: DataFrame): DataFrame = {

    val spark = df.sparkSession

    val Tbl_Master_LenovoTopSellers = spark.sql("select sku,topseller,ams_month,focus,system_type,form_factor,pricing_list_price " +
      "from ams_datamart_pc.tbl_master_lenovotopsellers_ca " +
      "group by sku,topseller,ams_month,focus,system_type,form_factor,pricing_list_price");

    val masterWithSkuDate = Tbl_Master_LenovoTopSellers.withColumn("ams_sku_date_temp",
      skuDateUDF(col("sku"),col("ams_month")))
      .drop("sku")
      .drop("ams_month")
      .withColumnRenamed("ams_sku_date_temp","ams_sku_date")

    val dfWithSKUDate = df.withColumn("ams_sku_date",
      skuDateUDF(col("model"),col("time_periods")))

    val withTopSellers = dfWithSKUDate.join(masterWithSkuDate,
      dfWithSKUDate("ams_sku_date")===masterWithSkuDate("ams_sku_date"),"left")
      .drop(masterWithSkuDate("ams_sku_date"))
      .withColumn("ams_top_sellers",
        topSellersUDF(col("topseller")))
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


    val finalDf = withTopSellers
      .withColumnRenamed("focus","ams_focus")
      .withColumnRenamed("system_type","ams_lenovo_system_type")
      .withColumnRenamed("form_factor","ams_lenovo_form_factor")
      .withColumnRenamed("pricing_list_price","ams_lenovo_list_price")
      .withColumn("ams_lenovo_focus",
        lenovoFocusUDF(col("ams_focus")))

    finalDf

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

  def withCAPriceBand(df: DataFrame): DataFrame = {

    val spark = df.sparkSession

    val masterPriceBand = spark.sql("select price_band,price_band_map,pb_less,pb_high from ams_datamart_pc.tbl_master_priceBand")

    val withPriceBand= df.join(masterPriceBand,
      df("ams_asp_ca") >= masterPriceBand("pb_less") && df("ams_asp_ca") < masterPriceBand("pb_high")
      ,"left")

    withPriceBand
        .drop("pb_less")
        .drop("pb_high")
      .withColumnRenamed("price_band","ams_price_band_ca")
      .withColumnRenamed("price_band_map","ams_price_band_map_ca")

  }

  def with_CA_US_PriceBand(df: DataFrame): DataFrame = {

    val spark = df.sparkSession

    val masterPriceBand = spark.sql("select price_band,price_band_map,pb_less,pb_high from ams_datamart_pc.tbl_master_priceBand")

    val withPriceBand= df.join(masterPriceBand,
      df("ams_asp_us") >= masterPriceBand("pb_less") && df("ams_asp_us") < masterPriceBand("pb_high")
      ,"left")

    withPriceBand
      .drop("pb_less")
      .drop("pb_high")
      .withColumnRenamed("price_band","ams_price_band_us")
      .withColumnRenamed("price_band_map","ams_price_band_map_us")

  }

  def withCAPriceBand4(df: DataFrame): DataFrame = {

    val spark = df.sparkSession

    val withTmpCat = df.withColumn("temp_cat",
      when(col("ams_Sub_category_temp") === "Mobile Workstation","MOBILEWORKSTATION").otherwise("COMMERCIAL"))

    val masterPriceBand = spark.sql("select category,price_band,pb_less,pb_high from ams_datamart_pc.tbl_master_map_pb4")

    val withPriceBand= withTmpCat.join(masterPriceBand,
      withTmpCat("ams_asp_ca") >= masterPriceBand("pb_less") && withTmpCat("ams_asp_ca") < masterPriceBand("pb_high") && withTmpCat("temp_cat") === masterPriceBand("category")
      ,"left")

    val final_df = withPriceBand
      .drop("category")
      .drop("temp_cat")
      .drop(masterPriceBand("pb_less"))
      .drop(masterPriceBand("pb_high"))
      .withColumnRenamed("price_band","map_price_band4")

    final_df

  }


  def withCAPriceBandDetailed(df: DataFrame): DataFrame = {

    val spark = df.sparkSession

    val withTempCat =  df.withColumn("temp_cat",
      when(col("ams_sub_category_temp") === "Mobile Workstation","WORKSTATION").otherwise("RETAIL_COMMERCIAL"))

    val masterPriceBand = spark.sql("select category,price_band,pb_less,pb_high from ams_datamart_pc.tbl_master_map_pb_detailed")

    val withPriceBand= withTempCat.join(masterPriceBand,
      withTempCat("ams_asp_ca") >= masterPriceBand("pb_less") && withTempCat("ams_asp_ca") < masterPriceBand("pb_high") && withTempCat("temp_cat") === masterPriceBand("category")
      ,"left")

    val final_df = withPriceBand
      .drop("category")
      .drop("temp_cat")
      .drop(masterPriceBand("pb_less"))
      .drop(masterPriceBand("pb_high"))
      .withColumnRenamed("price_band","map_price_band_detailed")

    final_df

  }

  def with_CA_US_PriceBand4(df: DataFrame): DataFrame = {

    val spark = df.sparkSession

    val withTmpCat = df.withColumn("temp_cat",
      when(col("ams_Sub_category_temp") === "Mobile Workstation","MOBILEWORKSTATION").otherwise("COMMERCIAL"))

    val masterPriceBand = spark.sql("select category,price_band,pb_less,pb_high from ams_datamart_pc.tbl_master_map_pb4")

    val withPriceBand= withTmpCat.join(masterPriceBand,
      withTmpCat("ams_asp_us") >= masterPriceBand("pb_less") && withTmpCat("ams_asp_us") < masterPriceBand("pb_high") && withTmpCat("temp_cat") === masterPriceBand("category")
      ,"left")

    val final_df = withPriceBand
      .drop("category")
      .drop("temp_cat")
      .drop(masterPriceBand("pb_less"))
      .drop(masterPriceBand("pb_high"))
      .withColumnRenamed("price_band","map_price_band4_us")

    final_df

  }


  def with_CA_US_PriceBandDetailed(df: DataFrame): DataFrame = {

    val spark = df.sparkSession

    val withTempCat =  df.withColumn("temp_cat",
      when(col("ams_sub_category_temp") === "Mobile Workstation","WORKSTATION").otherwise("RETAIL_COMMERCIAL"))

    val masterPriceBand = spark.sql("select category,price_band,pb_less,pb_high from ams_datamart_pc.tbl_master_map_pb_detailed")

    val withPriceBand= withTempCat.join(masterPriceBand,
      withTempCat("ams_asp_us") >= masterPriceBand("pb_less") && withTempCat("ams_asp_us") < masterPriceBand("pb_high") && withTempCat("temp_cat") === masterPriceBand("category")
      ,"left")

    val final_df = withPriceBand
      .drop("category")
      .drop("temp_cat")
      .drop(masterPriceBand("pb_less"))
      .drop(masterPriceBand("pb_high"))
      .withColumnRenamed("price_band","map_price_band_detailed_us")

    final_df

  }

}