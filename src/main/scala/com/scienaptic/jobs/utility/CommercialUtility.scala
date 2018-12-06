package com.scienaptic.jobs.utility

import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions.udf
import java.time._


object CommercialUtility {
  val createBaseSKUFromProductIDUDF = udf((productID: String) => {
    //TODO: IF Left(Right([Product ID], 4),1)="#" THEN Left([Product ID],Length([Product ID])-4) ELSE [Product ID] ENDIF
    "Sample String"
  })

  val extractWeekFromDateUDF = udf((dateStr: String) => {
    //TODO: DateTimeFormat([Partner Ship Calendar Date],"%w")
    var  simpleDateFormat:SimpleDateFormat = new SimpleDateFormat("dd/mm/yyyy");
    64
  })

  val addDaystoDateStringUDF = udf((dateString: String, days: Int) => {
    //TODO: DateTimeAdd([Partner Ship Calendar Date],6-[Temp Date Calc],"days")
    new Date()
  })

  val baseSKUFormulaUDF = udf((baseSKU) => {
    baseSKU match {
      case "M9L74A" => "M9L75A"
      case "J9V91A" => "J9V90A"
      case "J9V92A" => "J9V90A"
      case _ => baseSKU
    }
  })

  val checkOutsidePromoDateUDF = udf((shipDate: String, endDate: String) => {
    //TODO: IF [Partner Ship Calendar Date] > DateTimeAdd([End Date],3,"days") THEN "Y" ELSE "N" ENDIF
    //TODO: use 'addDaystoDateString' to add 3 days to endDate
    "Y"
  })

  val newEndDateFromMaxWED = udf((maxWED: String, endDate: String) => {
    //TODO: IF DateTimeDiff([Max_Week.End.Date],[End Date],"days") > 14 THEN DateTimeAdd([End Date],14,"days") ELSEIF [Max_Week.End.Date] > [End Date] THEN [Max_Week.End.Date] ELSE [End Date] ENDIF
    //DateTimeDiff - subtracts 2nd from 1st and returns different in 3rd format (here number of days)
    //DateTimeAdd  - Adds 2nd to 1st where 2nd is in 3rd's format (Here adds 14 days to End Date)
  })

  def returnDiffBetweenDates(firstDateString: String, secDateString: String, intFormat: String): Int = {
    val firstDate = convertStringToDate(firstDateString)
    intFormat match {
      case "days" => {

      }
      case "month" => {

      }
    }
    14
  }

  def addIntervalToDate(dateStr: String, interval: Int, intFormat: String): String = {
    "test"
  }

  def convertStringToDate(dateStr: String/*, format: String*/): LocalDate = {
    LocalDate.parse("2012-05-31")
  }

  /* SAMPLE
  def checkOutsidePromoDate(shipDate: String, endDate: String): String = {
  }
  val checkOutsidePromoDateUDF = udf[String, String, String](checkOutsidePromoDate)
  */

}
