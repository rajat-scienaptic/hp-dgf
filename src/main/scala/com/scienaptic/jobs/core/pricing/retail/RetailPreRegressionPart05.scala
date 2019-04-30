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

  val Cat_switch = 1
  val dat2000_01_01 = to_date(unix_timestamp(lit("2000-01-01"), "yyyy-MM-dd").cast("timestamp"))
  val dat9999_12_31 = to_date(unix_timestamp(lit("9999-12-31"), "yyyy-MM-dd").cast("timestamp"))
  val dateFormatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
  val dateFormatterMMddyyyyWithSlash = new SimpleDateFormat("MM/dd/yyyy")
  val dateFormatterMMddyyyyWithHyphen = new SimpleDateFormat("dd-MM-yyyy")
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

    var retailUnionRetailOtherAccountsDF  = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/retailTemp/RetailFeatEngg/retail-r-retailUnionRetailOtherAccountsDF-part04.csv")
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))

    /*do not uncomment
     following are omitted variables
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


    val windForSKUAndAccount = Window.partitionBy("SKU&Account" /*, "uuid"*/).orderBy(/*"SKU", "Account",*/ "Week_End_Date")
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

    //    EOLcriterion.where(col("SKU") === "1AS85A" && col("Account") === "Best Buy").show(50,false)
    val EOLCriterionLast = EOLcriterion.where(col("EOL_criterion") === 1)
      .groupBy("SKU", "Account")
      .agg(max("Week_End_Date").as("last_date"))
    //      .join(EOLcriterion.where(col("EOL_criterion") === 1), Seq("SKU_Name", "Account"), "right")

    val EOLCriterionMax = retailUnionRetailOtherAccountsDF
      .groupBy("SKU", "Account")
      .agg(max("Week_End_Date").as("max_date"))
    //      .join(retailEOL, Seq("SKU", "Account"), "right")

    val EOLCriterionMin = retailUnionRetailOtherAccountsDF
      .groupBy("SKU", "Account")
      .agg(min("Week_End_Date").as("min_date"))
    //      .join(retailEOL, Seq("SKU", "Account"), "right")

    val EOLMaxJoinLastDF = EOLCriterionMax
      .join(EOLCriterionLast, Seq("SKU", "Account"), "left")

    val EOLMaxLastJoinMinDF = EOLMaxJoinLastDF
      .join(EOLCriterionMin, Seq("SKU", "Account"), "left")

    // comment below 2 lines
    //    EOLMaxLastJoinMinDF.write.option("header", true).mode(SaveMode.Overwrite).csv("D:\\files\\temp\\EOLMaxLastJoinMinDF")
    //    var EOLMaxLastJoinMinDF =  executionContext.spark.read.option("header", true).option("inferSchema", true).csv("D:\\files\\temp\\EOLMaxLastJoinMinDF")

    val maxMaxDate = EOLMaxLastJoinMinDF.agg(max("max_date")).head().getDate(0)

    var EOLNATreatmentDF = EOLMaxLastJoinMinDF
      .withColumn("last_date", when(col("last_date").isNull, col("min_date")).otherwise(col("last_date")))
      .drop("min_date")
      .filter(col("max_date") =!= col("last_date") || col("max_date") =!= maxMaxDate)
      .withColumn("diff_weeks", ((datediff(to_date(col("max_date")), to_date(col("last_date")))) / 7) + 1)

    //    EOL_criterion <- EOL_criterion[rep(row.names(EOL_criterion), EOL_criterion$diff_weeks),]
    //      EOL_criterion$add <- t(as.data.frame(strsplit(row.names(EOL_criterion), "\\.")))[,2]# t = transpose
    //      EOL_criterion$add <- ifelse(grepl("\\.",row.names(EOL_criterion))==FALSE,0,as.numeric(EOL_criterion$add))
    //      EOL_criterion$add <- EOL_criterion$add*7
    //      EOL_criterion$Week.End.Date <- EOL_criterion$last_date+EOL_criterion$add

    EOLNATreatmentDF = EOLNATreatmentDF.withColumn("diff_weeks", when(col("diff_weeks").isNull || col("diff_weeks") <= 0, 0).otherwise(col("diff_weeks")))
      .withColumn("diff_weeks", col("diff_weeks").cast("int"))
      .withColumn("repList", createlist(col("diff_weeks"))).withColumn("add", explode(col("repList"))).drop("repList")
      .withColumn("add", col("add") * lit(7))
      .withColumn("Week_End_Date", expr("date_add(last_date, add)")) //CHECK: check if min_date is in timestamp format!
      .drop("max_date", "last_date", "diff_weeks", "add")
      .withColumn("EOL_criterion", lit(1))


    retailEOL = retailUnionRetailOtherAccountsDF.withColumn("Week_End_Date", to_date(col("Week_End_Date"))) // cast needed as join WED has datetype format
      .join(EOLNATreatmentDF, Seq("SKU", "Account", "Week_End_Date"), "left")
      .withColumn("EOL_criterion", when(col("EOL_criterion").isNull, 0).otherwise(col("EOL_criterion")))
      .withColumn("EOL_criterion_old", col("EOL_criterion")) // Variable omitted

    // comment ends here


    // comment below 2 lines
    //    retailEOL.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("D:\\files\\temp\\retail-Feb07-r-670.csv")

    var retailEOLDates = renameColumns(executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/managedSources/Calendar/EOL_Dates/EOL_Dates_Retail.csv")).cache()
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

     retailEOL.write.option("header", true).mode(SaveMode.Overwrite).csv("/etherData/retailTemp/RetailFeatEngg/retail-EOL-half-PART05.csv")
  }
}
