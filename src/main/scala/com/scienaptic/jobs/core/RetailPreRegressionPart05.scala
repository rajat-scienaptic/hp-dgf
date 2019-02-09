package com.scienaptic.jobs.core

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.bean.{RetailHoliday, RetailHolidayTranspose, UnionOperation}
import com.scienaptic.jobs.core.RetailPreRegressionPart01.{checkPrevDistInvGTBaseline, concatenateRankWithDist, stability_range}
import com.scienaptic.jobs.utility.CommercialUtility.createlist
import com.scienaptic.jobs.utility.Utils.renameColumns
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

import scala.collection.mutable

object RetailPreRegressionPart05 {

  val Cat_switch = 1
  val dat2000_01_01 = to_date(unix_timestamp(lit("2000-01-01"), "yyyy-MM-dd").cast("timestamp"))
  val dat9999_12_31 = to_date(unix_timestamp(lit("9999-12-31"), "yyyy-MM-dd").cast("timestamp"))
  val dateFormatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
  val dateFormatterMMddyyyyWithSlash = new SimpleDateFormat("MM/dd/yyyy")
  val dateFormatterMMddyyyyWithHyphen = new SimpleDateFormat("dd-MM-yyyy")
  val maximumRegressionDate = "2018-12-29"
  val minimumRegressionDate = "2014-01-01"
  val monthDateFormat = new SimpleDateFormat("MMM", Locale.ENGLISH)

  val stability_weeks = 4
  val stability_range = 0.7
  val intro_weeks = 8
  val min_baseline = 5
  val baselineThreshold = if (min_baseline / 2 > 0) min_baseline / 2 else 0

  val indexerForAdLocation = new StringIndexer().setInputCol("Ad_Location").setOutputCol("Ad_Location_fact")
  val pipelineForForAdLocation = new Pipeline().setStages(Array(indexerForAdLocation))

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

  val pmax = udf((col1: Double, col2: Double, col3: Double) => math.max(col1, math.max(col2, col3)))
  val pmax2 = udf((col1: Double, col2: Double) => math.max(col1, col2))
  val pmin = udf((col1: Double, col2: Double, col3: Double) => math.min(col1, math.min(col2, col3)))
  val pmin2 = udf((col1: Double, col2: Double) => math.min(col1, col2))

  val getMonthNumberFromString = udf((month: String) => {
    val date: Date = monthDateFormat.parse(month)
    val cal: Calendar = Calendar.getInstance
    cal.setTime(date)
    cal.get(Calendar.MONTH)
  })

  val concatenateRankWithDist = udf((x: mutable.WrappedArray[String]) => {
    //val concatenateRank = udf((x: List[List[Any]]) => {
    try {
      //      val sortedList = x.map(x => x.getAs[Int](0).toString + "." + x.getAs[Double](1).toString).sorted
      val sortedList = x.toList.map(x => (x.split("_")(0).toInt, x.split("_")(1).toDouble))
      sortedList.sortBy(x => x._1).map(x => x._2.toDouble)
    } catch {
      case _: Exception => null
    }
  })

  val checkPrevDistInvGTBaseline = udf((distributions: mutable.WrappedArray[Double], rank: Int, distribution: Double) => {
    var totalGt = 0
    if (rank <= stability_weeks)
      0
    else {
      val start = rank - stability_weeks - 1
      for (i <- start until rank - 1) {

        // checks if every distribution's abs value is less than the stability range
        if (math.abs(distributions(i) - distribution) <= stability_range) {
          totalGt += 1
        }
      }
      if (totalGt >= 1)
        1
      else
        0
    }
  })
  def execute(executionContext: ExecutionContext): Unit = {
    val spark: SparkSession = executionContext.spark
    import spark.implicits._

    var retailEOL  = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/retailTemp/RetailFeatEngg/retail-BOL-PART04.csv")
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("EOL_Date", to_date(unix_timestamp(col("EOL_Date"), "yyyy-MM-dd").cast("timestamp"))).cache()

    val christmasDF = Seq(("2014-12-27", 1), ("2015-12-26", 1), ("2016-12-31", 1), ("2017-12-30", 1), ("2018-12-29", 1), ("2019-12-28", 1)).toDF("Week_End_Date", "USChristmasDay")
    val columbusDF = Seq(("2014-10-18", 1), ("2015-10-17", 1), ("2016-10-15", 1), ("2017-10-14", 1), ("2018-10-13", 1), ("2019-10-19", 1)).toDF("Week_End_Date", "USColumbusDay")
    val independenceDF = Seq(("2014-07-05", 1), ("2015-07-04", 1), ("2016-07-09", 1), ("2017-07-08", 1), ("2018-07-07", 1), ("2019-07-06", 1)).toDF("Week_End_Date", "USIndependenceDay")
    val laborDF = Seq(("2014-09-06", 1), ("2015-09-12", 1), ("2016-09-10", 1), ("2017-09-09", 1), ("2018-09-08", 1), ("2019-09-07", 1)).toDF("Week_End_Date", "USLaborDay")
    val linconsBdyDF = Seq(("2014-02-15", 1), ("2015-02-14", 1), ("2016-02-13", 1), ("2017-02-18", 1), ("2018-02-17", 1), ("2019-02-16", 1)).toDF("Week_End_Date", "USLincolnsBirthday")
    val memorialDF = Seq(("2014-05-31", 1), ("2015-05-30", 1), ("2016-06-04", 1), ("2017-06-03", 1), ("2018-06-02", 1), ("2019-06-01", 1)).toDF("Week_End_Date", "USMemorialDay")
    val MLKingsDF = Seq(("2014-01-25", 1), ("2015-01-24", 1), ("2016-01-23", 1), ("2017-01-21", 1), ("2018-01-20", 1), ("2019-01-26", 1)).toDF("Week_End_Date", "USMLKingsBirthday")
    val newYearDF = Seq(("2014-01-04", 1), ("2015-01-03", 1), ("2016-01-02", 1), ("2017-01-07", 1), ("2018-01-06", 1), ("2019-01-05", 1)).toDF("Week_End_Date", "USNewYearsDay")
    val presidentsDayDF = Seq(("2014-02-22", 1), ("2015-02-21", 1), ("2016-02-20", 1), ("2017-02-25", 1), ("2018-02-24", 1), ("2019-02-23", 1)).toDF("Week_End_Date", "USPresidentsDay")
    val veteransDayDF = Seq(("2014-11-15", 1), ("2015-11-14", 1), ("2016-11-12", 1), ("2017-11-11", 1), ("2018-11-17", 1), ("2019-11-16", 1)).toDF("Week_End_Date", "USVeteransDay")
    val washingtonBdyDF = Seq(("2014-02-22", 1), ("2015-02-28", 1), ("2016-02-27", 1), ("2017-02-25", 1), ("2018-02-24", 1), ("2019-02-23", 1)).toDF("Week_End_Date", "USWashingtonsBirthday")
    val thanksgngDF = Seq(("2014-11-29", 1), ("2015-11-28", 1), ("2016-11-26", 1), ("2017-11-25", 1), ("2018-11-24", 1), ("2019-11-30", 1)).toDF("Week_End_Date", "USThanksgivingDay")

    val usCyberMonday = thanksgngDF.withColumn("Week_End_Date", date_add(col("Week_End_Date").cast("timestamp"), 7))
      .withColumnRenamed("USThanksgivingDay", "USCyberMonday")
    retailEOL = retailEOL.withColumn("Week_End_Date", to_date(col("Week_End_Date")))
      .join(christmasDF, Seq("Week_End_Date"), "left")
      .join(columbusDF, Seq("Week_End_Date"), "left")
      .join(independenceDF, Seq("Week_End_Date"), "left")
      .join(laborDF, Seq("Week_End_Date"), "left")
      .join(linconsBdyDF, Seq("Week_End_Date"), "left")
      .join(memorialDF, Seq("Week_End_Date"), "left")
      .join(MLKingsDF, Seq("Week_End_Date"), "left")
      .join(newYearDF, Seq("Week_End_Date"), "left")
      .join(presidentsDayDF, Seq("Week_End_Date"), "left")
      .join(veteransDayDF, Seq("Week_End_Date"), "left")
      .join(washingtonBdyDF, Seq("Week_End_Date"), "left")
      .join(thanksgngDF, Seq("Week_End_Date"), "left")
      .join(usCyberMonday, Seq("Week_End_Date"), "left")

    List("USChristmasDay", "USColumbusDay", "USIndependenceDay", "USLaborDay", "USLincolnsBirthday", "USMemorialDay", "USMLKingsBirthday", "USNewYearsDay", "USPresidentsDay", "USThanksgivingDay", "USVeteransDay", "USWashingtonsBirthday", "USCyberMonday")
      .foreach(x => {
        retailEOL = retailEOL.withColumn(x, when(col(x).isNull, 0).otherwise(col(x)))
      })
    /*
     TODO : consider the above dataframe , value of each DF is 1 and pmax below would be 1 as assumption
    retail$hol.dummy <- pmax(retail$USChristmasDay, retail$USColumbusDay, retail$USLaborDay, retail$USLincolnsBirthday, retail$USMemorialDay, retail$USMLKingsBirthday,
      retail$USVeteransDay, retail$USWashingtonsBirthday, retail$USCyberMonday, retail$USIndependenceDay, retail$USNewYearsDay,
      retail$USPresidentsDay, retail$USThanksgivingDay)*/

    val holidaysListNATreatment = Seq("USChristmasDay", "USThanksgivingDay", "USMemorialDay", "USPresidentsDay", "USLaborDay")

    var reatilWithHolidaysDF = retailEOL
      .select("Week_End_Date", "USLaborDay", "USMemorialDay", "USPresidentsDay", "USThanksgivingDay", "USChristmasDay") //   retail_hol <- retail[,colnames(retail) %in% holidays]


    // TODO : retail_hol <- gather(retail_hol, holidays, holiday.dummy, USLaborDay:USChristmasDay) // also this variable is omitted line 886
    // gather starts
    implicit val retailHolidayEncoder = Encoders.product[RetailHoliday]
    implicit val retailHolidayTranspose = Encoders.product[RetailHolidayTranspose]

    val retailHolidayDataSet = reatilWithHolidaysDF.as[RetailHoliday]
    val names = retailHolidayDataSet.schema.fieldNames

    val transposedData = retailHolidayDataSet.flatMap(row => Array(RetailHolidayTranspose(row.Week_End_Date, row.USMemorialDay, row.USPresidentsDay, row.USThanksgivingDay, names(1), row.USLaborDay),
      RetailHolidayTranspose(row.Week_End_Date, row.USMemorialDay, row.USPresidentsDay, row.USThanksgivingDay, names(5), row.USChristmasDay)
    ))
    reatilWithHolidaysDF = transposedData.toDF()
    // gather ends

    reatilWithHolidaysDF = reatilWithHolidaysDF
      .withColumn("holiday_dummy", col("holiday_dummy").cast(IntegerType))
      .filter(col("holiday_dummy") === 1).distinct().drop("holiday_dummy")
      // TODO done: check the format of Week_Ento_dated_Date
      .withColumn("lag_week", date_sub(to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp"), "yyyy-MM-dd"), 7))
      .withColumn("holiday_dummy", lit(1))
    // TODO : verify -> spread(retail_hol, holidays, Week.End.Date) : 896

    var spreadHolidays = reatilWithHolidaysDF
      .groupBy("Week_End_Date")
      .pivot("holidays")
      .agg(first(col("holiday_dummy")))
    //      .withColumn("USChristmasDay", when(col("USChristmasDay") === 1, col("Week_End_Date").cast(StringType)).otherwise(col("USChristmasDay")))
    //      .withColumn("USLaborDay", when(col("USLaborDay") === 1, col("Week_End_Date").cast(StringType)).otherwise(col("USLaborDay")))

    reatilWithHolidaysDF = spreadHolidays
      .join(reatilWithHolidaysDF, Seq("Week_End_Date"), "right")
      .drop("holiday_dummy", "holidays", "Week_End_Date")
      .withColumnRenamed("lag_week", "Week_End_Date")

    holidaysListNATreatment.foreach(holiday => {
      reatilWithHolidaysDF = reatilWithHolidaysDF.withColumnRenamed(holiday, "Lag_" + holiday)
    })

    retailEOL = retailEOL
      .join(reatilWithHolidaysDF, Seq("Week_End_Date"), "left")

    //    retailEOL.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("D:\\files\\temp\\retail-Feb06-r-914.csv")

    holidaysListNATreatment.foreach(holiday => {
      retailEOL = retailEOL.withColumn("Lag_" + holiday, when(col("Lag_" + holiday).isNull || col("Lag_" + holiday) === "", 0).otherwise(lit(1)))
    })

    retailEOL = retailEOL
      .withColumn("Amazon_Prime_Day", when(col("Week_End_Date") === "2018-07-21", 1).otherwise(0))

    // write
    retailEOL.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("/etherData/retailTemp/RetailFeatEngg/retail-Holidays-PART05.csv")
  }
}
