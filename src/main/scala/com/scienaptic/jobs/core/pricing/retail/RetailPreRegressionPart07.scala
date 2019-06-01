package com.scienaptic.jobs.core.pricing.retail

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.bean.{RetailHoliday, RetailHolidayTranspose}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}

import scala.collection.mutable

object RetailPreRegressionPart07 {

  def execute(executionContext: ExecutionContext): Unit = {
    val spark: SparkSession = executionContext.spark
    import spark.implicits._

    var retailEOL  = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("E:\\Scienaptic\\HP\\Pricing\\Data\\CR1\\May31_Run\\spark_out_retail\\retail-BOL-PART06.csv")
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

    val holidaysListNATreatment = Seq("USChristmasDay", "USThanksgivingDay", "USMemorialDay", "USPresidentsDay", "USLaborDay")

    var reatilWithHolidaysDF = retailEOL
      .select("Week_End_Date", "USLaborDay", "USMemorialDay", "USPresidentsDay", "USThanksgivingDay", "USChristmasDay")

    implicit val retailHolidayEncoder = Encoders.product[RetailHoliday]
    implicit val retailHolidayTranspose = Encoders.product[RetailHolidayTranspose]

    val retailHolidayDataSet = reatilWithHolidaysDF.as[RetailHoliday]
    val names = retailHolidayDataSet.schema.fieldNames

    val transposedData = retailHolidayDataSet.flatMap(row => Array(RetailHolidayTranspose(row.Week_End_Date, row.USMemorialDay, row.USPresidentsDay, row.USThanksgivingDay, names(1), row.USLaborDay),
      RetailHolidayTranspose(row.Week_End_Date, row.USMemorialDay, row.USPresidentsDay, row.USThanksgivingDay, names(5), row.USChristmasDay)
    ))
    reatilWithHolidaysDF = transposedData.toDF()

    reatilWithHolidaysDF = reatilWithHolidaysDF
      .withColumn("holiday_dummy", col("holiday_dummy").cast(IntegerType))
      .filter(col("holiday_dummy") === 1).distinct().drop("holiday_dummy")
      .withColumn("lag_week", date_sub(to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp"), "yyyy-MM-dd"), 7))
      .withColumn("holiday_dummy", lit(1))

    val spreadHolidays = reatilWithHolidaysDF
      .groupBy("Week_End_Date")
      .pivot("holidays")
      .agg(first(col("holiday_dummy")))

    reatilWithHolidaysDF = spreadHolidays
      .join(reatilWithHolidaysDF, Seq("Week_End_Date"), "right")
      .drop("holiday_dummy", "holidays", "Week_End_Date")
      .withColumnRenamed("lag_week", "Week_End_Date")

    holidaysListNATreatment.foreach(holiday => {   reatilWithHolidaysDF = reatilWithHolidaysDF.withColumnRenamed(holiday, "Lag_" + holiday)   })

    retailEOL = retailEOL.join(reatilWithHolidaysDF, Seq("Week_End_Date"), "left")

    holidaysListNATreatment.foreach(holiday => {
      retailEOL = retailEOL.withColumn("Lag_" + holiday, when(col("Lag_" + holiday).isNull || col("Lag_" + holiday) === "", 0).otherwise(lit(1)))
    })

    retailEOL = retailEOL.withColumn("Amazon_Prime_Day", when(col("Week_End_Date") === "2018-07-21", 1).otherwise(0))

    retailEOL.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("E:\\Scienaptic\\HP\\Pricing\\Data\\CR1\\May31_Run\\spark_out_retail\\retail-Holidays-PART07.csv")
  }
}
