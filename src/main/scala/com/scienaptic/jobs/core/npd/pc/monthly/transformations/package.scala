package com.scienaptic.jobs.core.npd.pc.monthly

import org.apache.spark.sql.functions.udf

package object transformations {

  val currentPrior = (num : Int)  => {

    if(num >= 13 && num <=24){
      "Prior"
    }
    else if( num >= 1 && num <= 12){
      "Current"
    } else {
      "-"
    }

  }

  val qtdCurrentPrior = (num : Int,fiscal : String)  => {

    val qtr = fiscal.substring(4,6);

    if(num >= 13 && num <=24){
      qtr+" Prior"
    }
    else if( num >= 1 && num <= 12){
      qtr+" Current"
    } else {
      "-"
    }

  }

  val subCategory = (mobileWorkstation : String,subCategory : String)  => {

    mobileWorkstation match {
      case "Mobile Workstation" => "Mobile Workstation"
      case _ => subCategory
    }

  }

  val subCategoryTemp = (mobileWorkstation : String,subCategory : String)  => {

    val categories = Set("Workstations","Workstation","BTO Workstations","BTO Workstation")

    if(mobileWorkstation.equals("Mobile Workstation") || categories.contains(subCategory)){
      "Mobile Workstation"
    }
    else {
      "Non Mobile Workstation"
    }

  }

  val smartBuys = (brand : String,model : String)  => {

    if(brand.equals("HP") && ( model.contains("UT") || model.contains("AT"))){
      "Smartbuy"
    }
    else {
      "Non Smart Buy"
    }

  }

  def dateToISO (date : String) = {
    val split = date.split(" ")
    val iso = split(0).replace("-","")
    iso
  }

  val skuDate = (sku : String,ams_month : String)  => {
    if(ams_month != null){
      val iso = dateToISO(ams_month)
      iso
    } else {
      sku
    }
  }

  val topSellers = (ts : String)  => {
    if(ts != null && ts.toLowerCase().equals("yes")){
      "Top Seller"
    }else{
      "Non Top Seller"
    }
  }

  val smartBuyTopSellers = (smartBuys : String,topSellers:String)  => {

    if(smartBuys != null && smartBuys.equals("Smartbuy")){
      "Smartbuy"
    }else if( topSellers != null && topSellers.equals("Top Seller")) {
      "Lenovo Top Seller"
    }else{
      "Non SB/Non LTS"
    }
  }

  val lenovoSmartBuyTopSellers =  (smartBuys : String,topSellers:String,brand : String, model : String)  => {
    if(smartBuys != null && smartBuys.equals("Smartbuy")){
      "Transactional"
    }else if(topSellers != null && topSellers.equals("Top Seller")) {
      "Transactional"
    }else{
      if(brand.equals("Dell") && (model.length() == 5)){
        "Transactional"
      }else{
        "Non Transactional"
      }
    }
  }

  val transactionalNontransactionalSkus = (smartBuys : String,topSellers:String, brand : String, model : String)  => {
    if(smartBuys != null && smartBuys.equals("Smartbuy")){
      "Transactional"
    }else if(topSellers != null && topSellers.equals("Top Seller")) {
      "Transactional"
    }else{
      if(brand.equals("Dell") && (model.length() == 5)){
        "Transactional"
      }else{
        "Non Transactional"
      }
    }
  }

  val lenovoFocus = (focus : String)  => {
    if(focus.equals("Yes") || focus.equals("Yes/STF")){
      "Yes"
    }else if(focus.equals("No")) {
      "No"
    }else{
      "NA"
    }
  }

  def skuDateUDF = udf(skuDate)

  def currentPriorUDF = udf(currentPrior)
  def qtdCurrentPriorUDF = udf(qtdCurrentPrior)
  def subCategoryUDF = udf(subCategory)
  def subCategoryTempUDF = udf(subCategoryTemp)
  def smartBuysUDF = udf(smartBuys)
  def topSellersUDF = udf(topSellers)
  def smartBuyTopSellersUDF = udf(smartBuyTopSellers)
  def LenovoSmartBuyTopSellersUDF = udf(lenovoSmartBuyTopSellers)
  def transactionalNontransactionalSkusUDF = udf(transactionalNontransactionalSkus)
  def lenovoFocusUDF = udf(lenovoFocus)




}
