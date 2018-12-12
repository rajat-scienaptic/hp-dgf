package com.scienaptic.jobs.utility

import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions.udf
import java.time._
import java.util.concurrent.TimeUnit
import java.time.format.DateTimeFormatter
import java.util.Calendar


object CommercialUtility {

  val ddmmyyyyFormat = DateTimeFormatter.ofPattern("dd-MM-yyyy")
  val yyyyMMddFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val ddmmyyyySimpleFormat = new SimpleDateFormat("dd-MM-yyyy")


  val createBaseSKUFromProductIDUDF = udf((productID: String) => {
    if (productID.takeRight(4).charAt(0) == "#")
      productID.substring(0, productID.length()-4)
    else
      productID
  })

  //Not being used
  val extractWeekFromDateUDF = udf((dateStr: String) => {
    //DateTimeFormat([Partner Ship Calendar Date],"%w")
    var  simpleDateFormat:SimpleDateFormat = new SimpleDateFormat("dd/mm/yyyy")
    val dateObj = simpleDateFormat.parse(dateStr)
    val cal = Calendar.getInstance
    cal.setTime(dateObj)
    cal.get(Calendar.WEEK_OF_YEAR)
  })

  val addDaystoDateStringUDF = udf((dateString: String, days: Int) => {
    //DateTimeAdd([Partner Ship Calendar Date],6-[Temp Date Calc],"days")
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val result = new Date(sdf.parse(dateString).getTime() + TimeUnit.DAYS.toMillis(6-days))
    sdf.format(result)

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

  //Not being used
  val checkOutsidePromoDateUDF = udf((shipDate: String, endDate: String) => {
    //IF [Partner Ship Calendar Date] > DateTimeAdd([End Date],3,"days") THEN "Y" ELSE "N" ENDIF
    "Y"
  })

  //Not being used
  val newEndDateFromMaxWED = udf((maxWED: String, endDate: String) => {
    //IF DateTimeDiff([Max_Week.End.Date],[End Date],"days") > 14 THEN DateTimeAdd([End Date],14,"days") ELSEIF [Max_Week.End.Date] > [End Date] THEN [Max_Week.End.Date] ELSE [End Date] ENDIF
    //DateTimeDiff - subtracts 2nd from 1st and returns different in 3rd format (here number of days)
    //DateTimeAdd  - Adds 2nd to 1st where 2nd is in 3rd's format (Here adds 14 days to End Date)
  })

  //Not being used
  val dateTimeDiff = udf((startDate: String, endDate: String) => {
    //val r = sqlContext.sql("select id,datediff(year,to_date(end), to_date(start)) AS date from c4")
    //datediff(current_date(), $"d")
    returnDiffBetweenDates(startDate,endDate,"days")
  })

  val dateTimeParseWithLastCharacters = udf((dateStr: String, format: String, chars: Int) => {
    convertStringToSimpleDate(dateStr.takeRight(chars), format)
  })

  //Not being used
  val findMaxBetweenTwo = udf((first: String, second: String) => {
    math.max(first.toDouble, second.toDouble)
  })

  //Not being used
  val getEpochNumberFromDateString = udf((dateStr: String) => {
    //df.withColumn("unix_ts" , unix_timestamp($"ts")
    //ts should be timestamp
    //toNumber(dateStr) //convert dateStr to Date object first
  })

  val convertDatetoFormat = udf((dateStr: String, currentFormat: String, targetFormat:String) => {
    val dateObj = convertStringToSimpleDate(dateStr, currentFormat)
    new SimpleDateFormat(targetFormat).format(dateObj)
  })



  /* Private functions */
  private def returnDiffBetweenDates(firstDateString: String, secDateString: String, intFormat: String): Int = {
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

  private def addIntervalToDate(dateStr: String, interval: Int, intFormat: String): String = {
    val dateVal = convertStringToSimpleDate(dateStr)
    val dattime = Date

    "test"
  }

  private def convertStringToSimpleDate(dateStr: String, format: String = "yyyy-MM-dd"): Date = {
    val simpleFormat = new SimpleDateFormat(format)
    simpleFormat.parse(dateStr)
  }

  private def convertStringToLocalDate(dateStr: String): LocalDate = {
    LocalDate.parse(dateStr, ddmmyyyyFormat)
  }

}
