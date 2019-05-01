package com.scienaptic.jobs.core.npd.pc.weekly.transformations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object USWeeklyTransformations {

  def withWeeksToDisplay(df :DataFrame) : DataFrame ={
    val spark = df.sparkSession

    val master_Month = spark.sql("select * from Tbl_Master_Month");

    val withTempMonth = df.withColumn("temp_month",substring(col("time_periods"),15,3))

    val joinedDf = withTempMonth.join(master_Month,
      lower(withTempMonth("temp_month"))===lower(master_Month("month_name")),"inner")

    val withNewDate = joinedDf
      .withColumn("ams_newdate",
      concat(col("month_number"),
        lit("/"),
        substring(col("time_periods"),19,2),
        lit("/"),
        substring(col("time_periods"),22,4)
      ))
      .withColumn("ams_qtr",
      concat(substring(col("time_periods"),22,4),
        col("calendar_quarter")
      ))
      .withColumn("ams_qtrweek",
        concat(substring(col("time_periods"),22,4),
          weekofyear(col("ams_newdate"))
        )
      )



    joinedDf
  }

}
