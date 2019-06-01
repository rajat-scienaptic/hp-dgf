package com.scienaptic.jobs.core.pricing.retail

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.CommercialUtility.createlist
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object RetailPreRegressionPart06 {

  val stability_weeks = 4
  val stability_range = 0.7
  val intro_weeks = 8

  def execute(executionContext: ExecutionContext): Unit = {
    val spark: SparkSession = executionContext.spark


    var retailEOL  = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/home/avik/Scienaptic/HP/data/May31_Run/spark_out_retail/retail-EOL-half-PART05.csv")
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("EOL_Date", to_date(unix_timestamp(col("EOL_Date"), "yyyy-MM-dd").cast("timestamp"))).cache()

    var BOL = retailEOL.select("SKU", "ES_date", "GA_date")
      .dropDuplicates()
      .where((col("ES_date").isNotNull) || (col("GA_date").isNotNull))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date_wday", lit(7) - dayofweek(col("ES_date")).cast("int"))
      .withColumn("GA_date_wday", lit(7) - dayofweek(col("GA_date")).cast("int"))
      .withColumn("GA_date", to_date(expr("date_add(GA_date, GA_date_wday)")))
      .withColumn("ES_date", to_date(expr("date_add(ES_date, ES_date_wday)")))
      .drop("GA_date_wday", "ES_date_wday")

    val windForSKUnAccount = Window.partitionBy("SKU&Account").orderBy("Week_End_Date")
    var BOLCriterion = retailEOL
      .groupBy("SKU", "Account", "Week_End_Date")
      .agg(max("Distribution_Inv").as("Distribution_Inv")) //TODO: Changed from sum to max
      .sort("SKU", "Account", "Week_End_Date")
      .withColumn("SKU&Account", concat(col("SKU"), col("Account")))
      //      .withColumn("uuid", lit(generateUUID()))
      .withColumn("rank", row_number().over(windForSKUnAccount))
      .withColumn("BOL_criterion", when(col("rank") < intro_weeks, 0).otherwise(1)) //BOL_criterion$BOL_criterion <- ave(BOL_criterion$Qty, paste0(BOL_criterion$SKU, BOL_criterion$Reseller.Cluster), FUN = BOL_criterion_v3)
      .drop("rank", "Distribution_Inv", "uuid")

    BOLCriterion = BOLCriterion.join(BOL.select("SKU", "GA_date"), Seq("SKU"), "left")
    val minWEDDate = to_date(lit(BOLCriterion.agg(min("Week_End_Date")).head().getDate(0)))

    BOLCriterion = BOLCriterion.withColumn("GA_date", when(col("GA_date").isNull, minWEDDate).otherwise(col("GA_date")))
      .where(col("Week_End_Date") >= col("GA_date") && col("GA_date").isNotNull)

    val BOLCriterionFirst = BOLCriterion.where(col("BOL_criterion") === 1)
      .groupBy("SKU", "Account")
      .agg(min("Week_End_Date").as("first_date"))

    val BOLCriterionMax = retailEOL
      .groupBy("SKU", "Account")
      .agg(max("Week_End_Date").as("max_date"))

    val BOLCriterionMin = retailEOL
      .groupBy("SKU", "Account")
      .agg(min("Week_End_Date").as("min_date"))

    BOLCriterion = BOLCriterionMax.withColumn("Account", col("Account")) // with column is on purpose as join cannot find Account from max dataframe
      .join(BOLCriterionFirst, Seq("SKU", "Account"), "left")
      .join(BOLCriterionMin, Seq("SKU", "Account"), "left")

    BOLCriterion = BOLCriterion.withColumn("first_date", when(col("first_date").isNull, col("max_date")).otherwise(col("first_date")))
      .drop("max_date")
    val minMinDateBOL = BOLCriterion.agg(min("min_date")).head().getDate(0)
    BOLCriterion = BOLCriterion
      .where(!((col("min_date") === col("first_date")) && (col("first_date") === minMinDateBOL)))
      .withColumn("diff_weeks", ((datediff(to_date(col("first_date")), to_date(col("min_date")))) / 7) + 1)

    /*
     do not un comment
    BOL_criterion <- BOL_criterion[rep(row.names(BOL_criterion), BOL_criterion$diff_weeks),]
    * BOL_criterion$add <- t(as.data.frame(strsplit(row.names(BOL_criterion), "\\.")))[,2]
    BOL_criterion$add <- ifelse(grepl("\\.",row.names(BOL_criterion))==FALSE,0,as.numeric(BOL_criterion$add))
    BOL_criterion$add <- BOL_criterion$add*7
    BOL_criterion$Week.End.Date <- BOL_criterion$min_date+BOL_criterion$add
    */
    BOLCriterion = BOLCriterion.withColumn("diff_weeks", when(col("diff_weeks").isNull || col("diff_weeks") <= 0, 0).otherwise(col("diff_weeks")))
      .withColumn("diff_weeks", col("diff_weeks").cast("int"))
      .withColumn("repList", createlist(col("diff_weeks"))).withColumn("add", explode(col("repList"))).drop("repList")
      .withColumn("add", col("add") * lit(7))
      .withColumn("Week_End_Date", expr("date_add(min_date, add)")) //CHECK: check if min_date is in timestamp format!

    BOLCriterion = BOLCriterion.drop("min_date", "fist_date", "diff_weeks", "add")
      .withColumn("BOL_criterion", lit(1))

    retailEOL = retailEOL.join(BOLCriterion, Seq("SKU", "Account", "Week_End_Date"), "left")
      .withColumn("BOL_criterion", when(col("BOL_criterion").isNull, 0).otherwise(col("BOL_criterion")))
      .withColumn("BOL_criterion", when(col("EOL_criterion").isNull, null).otherwise(col("BOL_criterion")))
      .withColumn("BOL_criterion", when(col("EOL_criterion") === 1, 0).otherwise(col("BOL_criterion")))
      .withColumn("BOL_criterion", when(datediff(col("Week_End_Date"), col("GA_date")) < (7 * 6), 1).otherwise(col("BOL_criterion")))
      .withColumn("BOL_criterion", when(col("GA_date").isNull, 0).otherwise(col("BOL_criterion")))
      .withColumn("BOL_criterion", when(col("Account").isin("Amazon-Proper") && col("SKU") === "M2U85A" && (col("Week_End_Date") === "2018-04-14" || col("Week_End_Date") === "2018-04-21"), 0).otherwise(col("BOL_criterion"))) // multiple date condition merged into 1
      .withColumn("EOL_criterion", when(col("EOL_criterion").isNull, 0).otherwise(col("EOL_criterion")))
      .withColumn("BOL_criterion", when(col("BOL_criterion").isNull, 0).otherwise(col("BOL_criterion")))
      .withColumn("ASP_IR", when(col("EOL_criterion") === 1, when(col("Other_IR") =!= 0, col("Other_IR")).otherwise(col("ASP_IR"))).otherwise(col("ASP_IR")))
      .withColumn("Other_IR", when(col("EOL_criterion") === 1, when(col("Other_IR") =!= 0, 0).otherwise(col("Other_IR"))).otherwise(col("Other_IR")))
      .withColumn("ASP_Flag", when(col("ASP_IR") > 0, 1).otherwise(lit(0)))
      .withColumn("Other_IR_Flag", when(col("Other_IR") > 0, 1).otherwise(lit(0)))

        retailEOL.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("/home/avik/Scienaptic/HP/data/May31_Run/spark_out_retail/retail-BOL-PART06.csv")
  }
}
