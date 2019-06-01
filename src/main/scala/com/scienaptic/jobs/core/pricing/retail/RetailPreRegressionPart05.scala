package com.scienaptic.jobs.core.pricing.retail

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.CommercialUtility.createlist
import com.scienaptic.jobs.utility.Utils.renameColumns
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object RetailPreRegressionPart05 {

  val stability_weeks = 4
  val stability_range = 0.7
  val intro_weeks = 8
  val min_baseline = 5
  val baselineThreshold = if (min_baseline / 2 > 0) min_baseline / 2 else 0

  val concatenateRankWithDist = udf((x: mutable.WrappedArray[String]) => {
    try {
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

    var retailUnionRetailOtherAccountsDF  = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/home/avik/Scienaptic/HP/data/May31_Run/spark_out_retail/retail-r-retailUnionRetailOtherAccountsDF-part04.csv")
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))

    /* CR1 - Variables removed from R code
    .withColumn("log_POS_Qty", log(col("POS_Qty")))
    .withColumn("log_POS_Qty", when(col("log_POS_Qty").isNull, 0).otherwise(col("log_POS_Qty")))
    .withColumn("log_POS_Qty", log(lit(1) - col("POS_Qty")))*/

    var retailEOL = retailUnionRetailOtherAccountsDF.select("SKU", "ES_date", "GA_date").dropDuplicates()
      .filter(col("ES_date").isNotNull || col("GA_date").isNotNull)
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date_wday", lit(7) - dayofweek(col("ES_date")).cast("int")) //As dayofweek returns in range 1-7 we want 0-6
      .withColumn("GA_date_wday", lit(7) - dayofweek(col("GA_date")).cast("int"))
      .withColumn("GA_date", expr("date_add(GA_date, GA_date_wday)"))
      .withColumn("ES_date", expr("date_add(ES_date, ES_date_wday)"))
      .drop("GA_date_wday", "ES_date_wday")


    val windForSKUAndAccount = Window.partitionBy("SKU&Account").orderBy(/*"SKU", "Account",*/ "Week_End_Date")
    var EOLcriterion = retailUnionRetailOtherAccountsDF
      .groupBy("SKU", "Account", "Week_End_Date")
      .agg(max("Distribution_Inv").as("Distribution_Inv"))
      .sort("SKU", "Account", "Week_End_Date")
      .withColumn("SKU&Account", concat(col("SKU"), col("Account")))

    val EOLcriterion1 = EOLcriterion
      .groupBy("SKU", "Account")
      .agg(max(col("Distribution_Inv")).as("Distribution_Inv2"))

    EOLcriterion = EOLcriterion.join(EOLcriterion1, Seq("SKU", "Account"), "left")

    EOLcriterion = EOLcriterion.orderBy("SKU", "Account", "Week_End_Date")
      .withColumn("rank", row_number().over(windForSKUAndAccount))
      .withColumn("dist_threshold", col("Distribution_Inv2") * (lit(1) - lit(stability_range)))

    var EOLWithCriterion1 = EOLcriterion
      .groupBy("SKU&Account")
      .agg((collect_list(concat_ws("_", col("rank"), col("Distribution_Inv")).cast("string"))).as("DistInvArray"))

    EOLWithCriterion1 = EOLWithCriterion1
      .withColumn("DistInvArray", when(col("DistInvArray").isNull, null).otherwise(concatenateRankWithDist(col("DistInvArray"))))
    EOLcriterion = EOLcriterion.join(EOLWithCriterion1, Seq("SKU&Account"), "left")
      .withColumn("EOL_criterion", when((col("rank") <= stability_weeks) || (col("Distribution_Inv") < col("dist_threshold")), 0)
        .otherwise(checkPrevDistInvGTBaseline(col("DistInvArray"), col("rank"), col("Distribution_Inv"))))
      .drop("rank", "DistInvArray", "SKU&Account", "Distribution_Inv2", "dist_threshold", "uuid")

    val EOLCriterionLast = EOLcriterion.where(col("EOL_criterion") === 1)
      .groupBy("SKU", "Account")
      .agg(max("Week_End_Date").as("last_date"))

    val EOLCriterionMax = retailUnionRetailOtherAccountsDF
      .groupBy("SKU", "Account")
      .agg(max("Week_End_Date").as("max_date"))

    val EOLCriterionMin = retailUnionRetailOtherAccountsDF
      .groupBy("SKU", "Account")
      .agg(min("Week_End_Date").as("min_date"))

    val EOLMaxJoinLastDF = EOLCriterionMax.join(EOLCriterionLast, Seq("SKU", "Account"), "left")

    val EOLMaxLastJoinMinDF = EOLMaxJoinLastDF.join(EOLCriterionMin, Seq("SKU", "Account"), "left")

    val maxMaxDate = EOLMaxLastJoinMinDF.agg(max("max_date")).head().getDate(0)

    var EOLNATreatmentDF = EOLMaxLastJoinMinDF
      .withColumn("last_date", when(col("last_date").isNull, col("min_date")).otherwise(col("last_date")))
      .drop("min_date")
      .filter(col("max_date") =!= col("last_date") || col("max_date") =!= maxMaxDate)
      .withColumn("diff_weeks", ((datediff(to_date(col("max_date")), to_date(col("last_date")))) / 7) + 1)

    EOLNATreatmentDF = EOLNATreatmentDF.withColumn("diff_weeks", when(col("diff_weeks").isNull || col("diff_weeks") <= 0, 0).otherwise(col("diff_weeks")))
      .withColumn("diff_weeks", col("diff_weeks").cast("int"))
      .withColumn("repList", createlist(col("diff_weeks"))).withColumn("add", explode(col("repList"))).drop("repList")
      .withColumn("add", col("add") * lit(7))
      .withColumn("Week_End_Date", expr("date_add(last_date, add)"))
      .drop("max_date", "last_date", "diff_weeks", "add")
      .withColumn("EOL_criterion", lit(1))


    retailEOL = retailUnionRetailOtherAccountsDF.withColumn("Week_End_Date", to_date(col("Week_End_Date")))
      .join(EOLNATreatmentDF, Seq("SKU", "Account", "Week_End_Date"), "left")
      .withColumn("EOL_criterion", when(col("EOL_criterion").isNull, 0).otherwise(col("EOL_criterion")))
      .withColumn("EOL_criterion_old", col("EOL_criterion")) // Variable omitted

    var retailEOLDates = renameColumns(executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/home/avik/Scienaptic/HP/data/May31_Run/inputs/EOL_Dates_Retail.csv")).cache()
    retailEOLDates.columns.toList.foreach(x => {
      retailEOLDates = retailEOLDates.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    retailEOLDates = retailEOLDates.cache()
      .withColumn("EOL_Date", to_date(unix_timestamp(col("EOL_Date"), "MM/dd/yyyy").cast("timestamp")))

    retailEOL = retailEOL
      .withColumn("EOL_criterion_old", col("EOL_criterion"))
      .join(retailEOLDates, Seq("Account", "SKU"), "left")
      .withColumn("Week_End_Date", to_date(col("Week_End_date")))
      .withColumn("EOL_Date", when(col("EOL_Date").isNull, null).otherwise(to_date(col("EOL_Date").cast("timestamp"))))
      .withColumn("EOL_criterion", when(col("Week_End_Date") >= col("EOL_Date"), 1).otherwise(lit(0)))
      .withColumn("EOL_criterion", when(col("EOL_Date").isNull, null).otherwise(col("EOL_criterion")))
      .withColumn("EOL_criterion", when(col("EOL_criterion").isNull, col("EOL_criterion_old")).otherwise(col("EOL_criterion"))) // omitted variable

    var EOLCriterion2 = retailEOL
      .withColumn("EOL_criterion", when(col("WeeK_End_Date") >= col("ES_date"), 1).otherwise(col("EOL_criterion")))
      .withColumn("EOL_criterion", when(col("ES_date").isNull, null).otherwise(col("EOL_criterion")))
      .withColumn("EOL_criterion", when(col("Account") === "Sam's Club", 0).otherwise(col("EOL_criterion")))
      .withColumn("EOL_criterion", when(col("Online") === 1 && col("POS_Qty") > 0, 0).otherwise(col("EOL_criterion")))

     retailEOL = EOLCriterion2

     retailEOL.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("/home/avik/Scienaptic/HP/data/May31_Run/spark_out_retail/retail-EOL-half-PART05.csv")
  }
}
