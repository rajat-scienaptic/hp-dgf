package com.scienaptic.jobs.utility

import java.util.Date
import org.apache.spark.sql.functions.udf
import java.time._
import java.util.concurrent.TimeUnit
import java.time.format.DateTimeFormatter
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.GregorianCalendar


object CommercialUtility {

  val ddmmyyyyFormat = DateTimeFormatter.ofPattern("dd-MM-yyyy")
  val yyyyMMddFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val ddmmyyyySimpleFormat = new SimpleDateFormat("dd-MM-yyyy")


  val createBaseSKUFromProductIDUDF = udf((productID: String) => {
    //println(s"ProductID recieved: $productID")
    if (productID.takeRight(4).charAt(0).toString == "#")
      productID.substring(0, productID.length()-4)
    else
      productID
  })

  val extractWeekFromDateUDF = udf((dateStr: String,format: String) => {
    /*import org.joda.time.DateTime
    import org.joda.time.format.DateTimeFormat
    val sourceFormat = DateTimeFormat.forPattern("yyyy-MM-dd")
    val sail_dt = DateTime.parse(dateStr, sourceFormat)*/
    //try {
      val calendar = new GregorianCalendar()
      val dt = new SimpleDateFormat(format).parse(dateStr)
      calendar.setTime(dt)
      val dec = calendar.get(Calendar.DAY_OF_YEAR)/7
      if (calendar.get(Calendar.DAY_OF_YEAR)%7!=0)
        dec+1
      else
        dec
    /*}
    catch {
      case nullPoint: NullPointerException => dateStr
      case e: Exception => dateStr
    }*/
  })

  val addDaystoDateStringUDF = udf((dateString: String, days: Int) => {
    //DateTimeAdd([Partner Ship Calendar Date],6-[Temp Date Calc],"days")
    try {
      val sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
      val result = new Date(sdf.parse(dateString).getTime() + TimeUnit.DAYS.toMillis(6-days))
      sdf.format(result)
    }
    catch {
      case nullPoint: NullPointerException => dateString
      case e: Exception => dateString
    }
    val sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val result = new Date(sdf.parse(dateString).getTime() + TimeUnit.DAYS.toMillis(6-days))
    sdf.format(result)
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
  /*val checkOutsidePromoDateUDF = udf((shipDate: String, endDate: String) => {
    //IF [Partner Ship Calendar Date] > DateTimeAdd([End Date],3,"days") THEN "Y" ELSE "N" ENDIF
    "Y"
  })*/

  //Not being used
  /*val newEndDateFromMaxWED = udf((maxWED: String, endDate: String) => {
    //IF DateTimeDiff([Max_Week.End.Date],[End Date],"days") > 14 THEN DateTimeAdd([End Date],14,"days") ELSEIF [Max_Week.End.Date] > [End Date] THEN [Max_Week.End.Date] ELSE [End Date] ENDIF
    //DateTimeDiff - subtracts 2nd from 1st and returns different in 3rd format (here number of days)
    //DateTimeAdd  - Adds 2nd to 1st where 2nd is in 3rd's format (Here adds 14 days to End Date)
  })*/

  //Not being used
  /*val dateTimeDiff = udf((startDate: String, endDate: String) => {
    //val r = sqlContext.sql("select id,datediff(year,to_date(end), to_date(start)) AS date from c4")
    //datediff(current_date(), $"d")
    returnDiffBetweenDates(startDate,endDate,"days")
  })*/

  val dateTimeParseWithLastCharacters = udf((dateStr: String, chars: Int) => {
    ////println(s"::::::::::::::::::::::::::::::::::::got dateStr: $dateStr with format $format and chars $chars")
    dateStr.takeRight(chars)
    //convertStringToSimpleDate(dateStr.takeRight(chars), format)
  })

  val findMaxBetweenTwo = udf((first: String, second: String) => {
    //println(s":::::::::::::::::::::::::::::::::::: Got first $first and second $second")
    math.max(first.toDouble, second.toDouble)
  })

  //Not being used
  /*val getEpochNumberFromDateString = udf((dateStr: String) => {
    //df.withColumn("unix_ts" , unix_timestamp($"ts")
    //ts should be timestamp
    //toNumber(dateStr) //convert dateStr to Date object first
  })*/

  //Not using
  /*val convertDatetoFormat = udf((dateStr: String, currentFormat: String) => {
    //println(s"Inside convertDatetoFormat : got dateStr:  $dateStr")
    val dateObj = convertStringToSimpleDate(dateStr, currentFormat)
    //new SimpleDateFormat(targetFormat).format(dateObj)
    //TODO: Return timestamp
  })*/



  /* Private functions */
  private def returnDiffBetweenDates(firstDateString: String, secDateString: String, intFormat: String): Int = {
    //println(s":::::::::::::::::::::::::::::::::::: Got firstDateString: $firstDateString and second $secDateString")
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
    val dateVal = convertStringToSimpleDate(dateStr, "yyyy-MM-dd")
    //val dattime = Date
    "test"
  }

  private def convertStringToSimpleDate(dateStr: String, format: String): Date = {
    //println(s"::::::::::Got dateStr $dateStr and format $format")
    val simpleFormat = new SimpleDateFormat(format)
    simpleFormat.parse(dateStr)
  }

  private def convertStringToLocalDate(dateStr: String): LocalDate = {
    LocalDate.parse(dateStr, ddmmyyyyFormat)
  }

}
