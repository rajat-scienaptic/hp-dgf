package com.scienaptic.jobs.core.npd.pc.monthly.transformations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object USTransformations {

  /*
  This procedure updates ams_temp_units,ams_temp_dollars,ams_asp,ams_aup,unitsabs,dollarsabs
  */
  def withASP(df: DataFrame): DataFrame = {

    val cleanDf = df.withColumn("ams_temp_units",
      when(
        (col("Units")>0) && (col("Dollars")>0),
        col("Units")
      ).otherwise(lit(0).cast(IntegerType)))

    val withTempDollers = cleanDf.withColumn("ams_temp_dollars",
      when((col("Units")>0) && (col("Dollars")>0),col("Dollars")).otherwise(lit(0).cast(IntegerType)))


    val withASPDf =withTempDollers.withColumn("ams_asp",
      when(col("ams_temp_units")===0 ,lit(0).cast(IntegerType)).otherwise(col("ams_temp_dollars")/col("ams_temp_units")))
      .withColumn("units_abs",abs(col("units")))
      .withColumn("dollars_abs",abs(col("dollars")))

    val withAUPDf = withASPDf.withColumn("ams_aup",col("ams_asp"))

    withAUPDf

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

  def withUSTopSellers(df: DataFrame): DataFrame = {

    val spark = df.sparkSession

    val Tbl_Master_LenovoTopSellers = spark.sql("select sku,top_seller,ams_month,focus,system_type,form_factor,pricing_list_price " +
      "from ams_datamart_pc.tbl_master_lenovotopsellers " +
      "group by sku,top_seller,ams_month,focus,system_type,form_factor,pricing_list_price");

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
        topSellersUDF(col("top_seller")))
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

  Stored PROC : Proc_Update_Master_Category
  */

  def withCategory(df: DataFrame): DataFrame = {

    val spark = df.sparkSession

    val masterCategoryDf = spark.sql("select subcat,catgory,npd_category from ams_datamart_pc.tbl_master_category")

    val withCategory = df
      .filter(!col("sub_category").isin(List("BTO Servers","Bare-Bones Computers","Servers")))
      .join(masterCategoryDf,
      df("sub_category")===masterCategoryDf("subcat"),"left")

    val finalCategoryDf = withCategory
      .drop("subcat")
      .withColumnRenamed("catgory","ams_catgrp")
      .withColumn("ams_npd_category",
        when(col("npd_category").isNull, lit("Workstation"))
          .otherwise(col("npd_category"))
      ).drop("npd_category")
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

  // Not Used

//  def withCDW(df: DataFrame): DataFrame = {
//
//    val spark = df.sparkSession
//
//    val masterParsehubCDW = spark.sql("select sku,windows,price from ams_datamart_pc.tbl_master_parsehub_cdw group by sku,windows,price ")
//
//    val withCDW= df.join(masterParsehubCDW,
//      df("model")===masterParsehubCDW("sku"),"left")
//
//    val finalDf = withCDW
//      .drop(masterParsehubCDW("sku"))
//      .withColumnRenamed("windows","ams_cdw_os")
//      .withColumnRenamed("price","ams_cdw_price")
//
//    finalDf
//
//  }


  /*
  This procedure updates AMS_Focus,AMS_Lenovo_Focus,AMS_Lenovo_System_Type
  ,AMS_Lenovo_Form_Factor,AMS_Lenovo_List_Price

  Stored PROC : Proc_Update_Master_TopSeller_LenovoFocus_MONTHLY
  */

//  def withLenovoFocus(df: DataFrame): DataFrame = {
//
//    val spark = df.sparkSession
//
//    val masterLenovoTopSellers = spark
//      .sql("select ams_sku_date,focus,system_type,form_factor,pricing_list_price from ams_datamart_pc.tbl_master_lenovotopsellers")
//
//    val withLenovoFocus= df.join(masterLenovoTopSellers,
//      df("ams_sku_date")===masterLenovoTopSellers("ams_sku_date"),"left")
//
//    val finalDf = withLenovoFocus
//      .withColumnRenamed("focus","ams_focus")
//      .withColumnRenamed("system_type","ams_lenovo_system_type")
//      .withColumnRenamed("form_factor","ams_lenovo_form_factor")
//      .withColumnRenamed("pricing_list_price","ams_lenovo_list_price")
//      .withColumn("ams_lenovo_focus",
//        lenovoFocusUDF(col("ams_focus")))
//
//    finalDf
//
//  }



  def withPriceBand(df: DataFrame): DataFrame = {

    val spark = df.sparkSession

    val masterPriceBand = spark.sql("select price_band,price_band_map,pb_less,pb_high from ams_datamart_pc.tbl_master_priceBand")

    val withPriceBand= df.join(masterPriceBand,
      df("ams_asp") >= masterPriceBand("PB_LESS") && df("ams_asp") < masterPriceBand("PB_HIGH")
      ,"inner")

    withPriceBand
        .drop("pb_less")
        .drop("pb_high")
      .withColumnRenamed("price_band","ams_price_band")
      .withColumnRenamed("price_band_map","ams_price_band_map")

  }

  def withPriceBand4(df: DataFrame): DataFrame = {

    val spark = df.sparkSession

    val withTempCat = df.withColumn("temp_cat",
      when(col("ams_sub_category_temp") === "Mobile Workstation","MOBILEWORKSTATION").otherwise("COMMERCIAL"))

    val masterPriceBand = spark.sql("select category,price_band,pb_less,pb_high from ams_datamart_pc.tbl_master_map_pb4")

    val withPriceBand= withTempCat.join(masterPriceBand,
      withTempCat("ams_asp") >= masterPriceBand("PB_LESS") && withTempCat("ams_asp") < masterPriceBand("PB_HIGH") && withTempCat("temp_cat") === masterPriceBand("category")
      ,"left")

    val final_df = withPriceBand
        .drop("category")
      .drop("temp_cat")
      .drop(masterPriceBand("PB_LESS"))
      .drop(masterPriceBand("PB_HIGH"))
      .withColumnRenamed("price_band","map_price_band4")

    final_df

  }

  def withPriceBandDetailed(df: DataFrame): DataFrame = {

    val spark = df.sparkSession

    val withTempCat =df.withColumn("temp_cat",
      when(col("ams_sub_category_temp") === "Mobile Workstation","WORKSTATION").otherwise("RETAIL_COMMERCIAL"))

    val masterPriceBand = spark.sql("select category,price_band,pb_less,pb_high from ams_datamart_pc.tbl_master_map_pb_detailed")

    val withPriceBand= withTempCat.join(masterPriceBand,
      withTempCat("ams_asp") >= masterPriceBand("PB_LESS") && withTempCat("ams_asp") < masterPriceBand("PB_HIGH") && withTempCat("temp_cat") === masterPriceBand("category")
      ,"left")

    val final_df = withPriceBand
      .drop("category")
      .drop("temp_cat")
      .drop(masterPriceBand("PB_LESS"))
      .drop(masterPriceBand("PB_HIGH"))
      .withColumnRenamed("price_band","map_price_band_detailed")

    final_df

  }

  //not unique on key
  def withSmartBuyFormFactor(df: DataFrame): DataFrame = {

    val spark = df.sparkSession

    val masterSmartBuys = spark.sql("select sku,form_factor from ams_datamart_pc.tbl_master_smartbuys")

    val withFormFactor= df.join(masterSmartBuys,
      df("model") === masterSmartBuys("sku")
      ,"left")

    val final_df = withFormFactor
      .drop(masterSmartBuys("sku"))
      .withColumnRenamed("form_factor","ams_hp_form_factor")

    final_df

  }

}
