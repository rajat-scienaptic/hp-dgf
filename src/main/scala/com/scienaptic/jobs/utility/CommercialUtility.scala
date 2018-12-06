package com.scienaptic.jobs.utility

import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions.udf
import java.time._
import java.time.format.DateTimeFormatter


object CommercialUtility {

  val ddmmyyyyFormat = DateTimeFormatter.ofPattern("dd-MM-yyyy")
  val ddmmyyyySimpleFormat = new SimpleDateFormat("dd-MM-yyyy")

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

  val baseSKUFormulaUDF = udf((baseSKU: String) => {
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

  val dateTimeDiff = udf((startDate: String, endDate: String) => {
    returnDiffBetweenDates(startDate,endDate,"days")
  })




  /* Private functions */
  def returnDiffBetweenDates(firstDateString: String, secDateString: String, intFormat: String): Int = {
    val firstDate = convertStringToLocalDate(firstDateString)
    val secondDate = convertStringToLocalDate(secDateString)
    val per = Period.between(firstDate, secondDate)
    intFormat match {
      case "days" => {
        per.getDays()
      }
      case "month" => {
        per.getMonths()
      }
    }
  }

  def addIntervalToDate(dateStr: String, interval: Int, intFormat: String): String = {
    val dateVal = convertStringToSimpleDate(dateStr)
    val dattime = Date

    "test"
  }

  def convertStringToSimpleDate(dateStr: String/*, format: String*/): Date = {
    ddmmyyyySimpleFormat.parse(dateStr)
  }

  def convertStringToLocalDate(dateStr: String/*, format: String*/): LocalDate = {
    LocalDate.parse(dateStr, ddmmyyyyFormat)
  }

  /* SAMPLE
  def checkOutsidePromoDate(shipDate: String, endDate: String): String = {
  }
  val checkOutsidePromoDateUDF = udf[String, String, String](checkOutsidePromoDate)
  */

}
