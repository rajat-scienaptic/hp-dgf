package com.scienaptic.jobs.utility

import java.util.Date

import org.apache.spark.sql.functions.udf
import java.time._
import java.util.concurrent.TimeUnit
import java.time.format.DateTimeFormatter
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.GregorianCalendar
import scala.util.matching.Regex

import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable


object CommercialUtility {

  val dateFormatterMMddyyyyWithSlash = new SimpleDateFormat("MM/dd/yyyy")
  val dateFormatterMMddyyyyWithHyphen = new SimpleDateFormat("dd-MM-yyyy")
  val ddmmyyyySimpleFormat = new SimpleDateFormat("dd-MM-yyyy")
  val sdfMMddyyyySlash = new SimpleDateFormat("MM/dd/yyyy hh:mm")
  val sdfMMddyyyyHyphen = new SimpleDateFormat("MM-dd-yyyy hh:mm")
  val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")



  def writeDF(df: DataFrame, path: String) = {
    df.coalesce(1).write.option("dateFormat", "yyyy-MM-dd").option("header","true").mode(SaveMode.Overwrite).csv("E:\\Scienaptic\\HP\\Pricing\\R\\SPARK_DEBUG_OUTPUTS\\"+path+".csv")
    //df.write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/Pricing/outputs/INTERMEDIATE/"+path+".csv")
  }

  val createBaseSKUFromProductIDUDF = udf((productID: String) => {
    //println(s"ProductID recieved: $productID")
    if (productID.takeRight(4).charAt(0).toString == "#")
      productID.substring(0, productID.length()-4)
    else
      productID
  })

  val convertFaultyDateFormat = udf((dateStr: String) => {
    try {
      if (dateStr.contains("-")) {
        dateFormatter.format(dateFormatterMMddyyyyWithHyphen.parse(dateStr))
      }
      else {
        dateFormatter.format(dateFormatterMMddyyyyWithSlash.parse(dateStr))
      }
    } catch {
      case _: Exception => dateStr
    }
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
      val dec = calendar.get(Calendar.DAY_OF_YEAR) / 7
      if (calendar.get(Calendar.DAY_OF_YEAR) % 7 != 0)
        dec
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
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val result = new Date(sdf.parse(dateString).getTime() + TimeUnit.DAYS.toMillis(7-days))
      sdf.format(result)
    }
    catch {
      case nullPoint: NullPointerException => dateString
      case e: Exception => dateString
    }
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val result = new Date(sdf.parse(dateString).getTime() + TimeUnit.DAYS.toMillis(7-days))
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

  val createlist = udf((times: Int) => {
    List.range(0,times.toInt,1)
  })

  val concatenateRank = udf((x: mutable.WrappedArray[String]) => {
  //val concatenateRank = udf((x: List[List[Any]]) => {
    try {
      //println("Camee as : "+x.toList)
      val sortedList = x.toList.map(x => (x.split("_")(0).toInt,x.split("_")(1).toInt))
      /*println("------------->   " + sortedList)
      println("---------------> " + sortedList.sortBy(x => x._1))
      println("------------------------> " + sortedList.sortBy(x => x._1).map(x => x._2.toInt))*/
      sortedList.sortBy(x => x._1).map(x => x._2.toInt)
      /*val sortedList = x.map(x=> x.getAs[Int](0).toString+"."+x.getAs[Int](1).toString).sorted
      sortedList.map(x => x.split("\\.")(1).toInt)*/
    } catch {
      case _: Exception => null
    }
  })

  val checkPrevQtsGTBaseline = udf((qts: mutable.WrappedArray[Int], rank: Int, baseline: Int, stab_weeks: Int) => {
    var totalGt = 0
    if (rank <= stab_weeks)
      0
    else {
      /*println("Quantities came: ---> " + qts)
      println("Rank is --------> "+ rank + " baseline ---------> " + baseline + " stab_weeks ----------> "+ stab_weeks)
      println("Start is ----------> " + (rank-stab_weeks))*/
      val start = rank-stab_weeks-1
      for (i <- start until (rank-1)) {
        /*println("Comparing Quantity : " + qts(i) +" and Baseline : " + baseline + " result is ::: " + (qts(i).toInt > baseline.toInt))*/
        if (qts(i) > baseline) {
          totalGt += 1
        }
      }
      if (totalGt >= 1)
        1
      else
        0
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
  /*private def returnDiffBetweenDates(firstDateString: String, secDateString: String, intFormat: String): Int = {
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
  }*/

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

  /*private def convertStringToLocalDate(dateStr: String): LocalDate = {
    LocalDate.parse(dateStr, ddmmyyyyFormat)
  }*/

}
