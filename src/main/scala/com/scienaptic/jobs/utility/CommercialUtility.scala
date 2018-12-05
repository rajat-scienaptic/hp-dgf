package com.scienaptic.jobs.utility

import java.util.Date
import org.apache.spark.sql.functions.udf

object CommercialUtility {
  def createBaseSKUFromProductID(productID: String): String ={
    //TODO: IF Left(Right([Product ID], 4),1)="#" THEN Left([Product ID],Length([Product ID])-4) ELSE [Product ID] ENDIF
    "Sample String"
  }
  val createBaseSKUFromProductIDUDF = udf[String, String](createBaseSKUFromProductID)

  def extractWeekFromDate(dateStr: String): Int = {
    //TODO: DateTimeFormat([Partner Ship Calendar Date],"%w")
    64
  }
  val extractWeekFromDateUDF = udf[String, Int](extractWeekFromDate)

  def addDaystoDateString(dateString: String, days: Int): Date = {
    //TODO: DateTimeAdd([Partner Ship Calendar Date],6-[Temp Date Calc],"days")
    new Date()
  }
  val addDaystoDateStringUDF = udf[String, Int, Date](addDaystoDateString)

  def baseSKUFormula(basesku: String): String= {
    basesku match {
      case "M9L74A" => "M9L75A"
      case "J9V91A" => "J9V90A"
      case "J9V92A" => "J9V90A"
      case _ => basesku
    }
  }
  val baseSKUFormulaUDF = udf[String, String](baseSKUFormula)

  def checkOutsidePromoDate(shipDate: String, endDate: String): String = {
    //TODO: IF [Partner Ship Calendar Date] > DateTimeAdd([End Date],3,"days") THEN "Y" ELSE "N" ENDIF
    //TODO: use 'addDaystoDateString' to add 3 days to endDate
    "Y"
  }
  val checkOutsidePromoDateUDF = udf[String, String, String](checkOutsidePromoDate)

}
