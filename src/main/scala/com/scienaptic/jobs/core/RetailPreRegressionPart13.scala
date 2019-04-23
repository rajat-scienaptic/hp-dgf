package com.scienaptic.jobs.core

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.core.RetailPreRegressionPart01.Cat_switch
import com.scienaptic.jobs.utility.CommercialUtility.extractWeekFromDateUDF
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}

import scala.collection.mutable

object RetailPreRegressionPart13 {

  val Cat_switch = 1
  val dat2000_01_01 = to_date(unix_timestamp(lit("2000-01-01"), "yyyy-MM-dd").cast("timestamp"))
  val dat9999_12_31 = to_date(unix_timestamp(lit("9999-12-31"), "yyyy-MM-dd").cast("timestamp"))
  val dateFormatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
  val dateFormatterMMddyyyyWithSlash = new SimpleDateFormat("MM/dd/yyyy")
  val dateFormatterMMddyyyyWithHyphen = new SimpleDateFormat("dd-MM-yyyy")
  val maximumRegressionDate = "2019-03-30"
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

    var retailWithCompCannDF = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/retailTemp/RetailFeatEngg/retail-Seasonality-Hardware-PART12.csv")
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("EOL_Date", to_date(unix_timestamp(col("EOL_Date"), "yyyy-MM-dd").cast("timestamp"))).cache()

    /* TODO : load R file
    load(file = "Scaling Ratio by Discount Depth.RData")
    supplies_GM_scaling_factor <- data.frame("supplies_GM_scaling_factor" = model$coefficients[grepl("Promo.Pct",names(model$coefficients))])
     remove retailWithCompCannDF and use Data frame from R file
     */
//    var supplies_GM_scaling_factor = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/etherData/managedSources/Depth/DepthDataConverted.csv").cache()
//      .withColumn("Account", when(col("Account").contains("Best Buy"), "Best Buy")
//        .when(col("Account").contains("Office Depot-Max"), "Office Depot-Max")
//        .when(col("Account").contains("Staples"), "Staples")
//        .when(col("Account").contains("Costco"), "Costco").otherwise(null)
//      )

    /*
    Removed : rownames() in R
      #   supplies_GM_scaling_factor <- data.frame("supplies_GM_scaling_factor" = model$coefficients[grepl("Promo.Pct",names(model$coefficients))])

      #supplies_GM_scaling_factor$Account = if(supplies_GM_scaling_factor ="Best Buy","Best Buy",
      #ifelse(supplies_GM_scaling_factor="Office Depot-Max" then "Office Depot-Max" else if(supplies_GM_scaling_factor="Staples" then"Staples" else if(supplies_GM_scaling_factor= "Costco") then"Costco" else NA))))
        supplies_GM_scaling_factor$Account <- ifelse(grepl("Best Buy",rownames(supplies_GM_scaling_factor))==TRUE,"Best Buy",ifelse(grepl("Office Depot-Max",rownames(supplies_GM_scaling_factor))==TRUE,"Office Depot-Max", ifelse(grepl("Staples",rownames(supplies_GM_scaling_factor))==TRUE,"Staples", ifelse(grepl("Costco",rownames(supplies_GM_scaling_factor))==TRUE,"Costco",NA))))

      # renaming
      if(Cat_switch==1) supplies_GM_scaling_factor$L1.Category <- ifelse(grepl("Home and Home Office",rownames(supplies_GM_scaling_factor))==TRUE,"Home and Home Office",ifelse(grepl("Office - Personal",rownames(supplies_GM_scaling_factor))==TRUE,"Office - Personal",ifelse(grepl("Office - Small Workteam",rownames(supplies_GM_scaling_factor))==TRUE,"Office - Small Workteam",NA)))

      #
      if(Cat_switch==2){
        L2_Cat <- unique(as.character(retail$L2.Category))
        supplies_GM_scaling_factor$L2.Category <- NA

        #
        for(i in 1:length(L2_Cat)){
          supplies_GM_scaling_factor$L2.Category <- ifelse(grepl(L2_Cat[i],rownames(supplies_GM_scaling_factor))==TRUE, L2_Cat[i], supplies_GM_scaling_factor$L2.Category)

        }
      }



    Cat_switch match {
      case 1 => {
        supplies_GM_scaling_factor = supplies_GM_scaling_factor
          .withColumn("L1_Category", when(col("Account").contains("Home and Home Office"), "Home and Home Office")
            .when(col("Account").contains("Office - Personal"), "Office - Personal")
            .when(col("Account").contains("Office - Small Workteam"), "Office - Small Workteam")
            .otherwise(null)
          )
      }
      case 2 => {
        val L2_Cat = retailWithCompCannDF.select("Account").distinct()

        supplies_GM_scaling_factor = supplies_GM_scaling_factor
          .withColumn("L2_Category", null)

        L2_Cat foreach (l2Category => {
          supplies_GM_scaling_factor = supplies_GM_scaling_factor
            .withColumn("L2_Category", when(col("Account").contains(l2Category), l2Category)
              .otherwise(col("L2_Category"))
            )
        })

      }

    }
    */
    val avgDiscountSKUAccountDF = retailWithCompCannDF
      .groupBy("SKU_Name", "Account")
      .agg((sum(col("POS_Qty") * col("Total_IR")) / sum(col("POS_Qty") * col("Street_Price"))).as("avg_discount_SKU_Account"))

    retailWithCompCannDF = retailWithCompCannDF
      .join(avgDiscountSKUAccountDF, Seq("SKU_Name", "Account"), "left")
      .withColumn("avg_discount_SKU_Account", when(col("avg_discount_SKU_Account").isNull, 0).otherwise(col("avg_discount_SKU_Account")))
      .na.fill(0, Seq("avg_discount_SKU_Account"))
      .withColumn("Supplies_GM_unscaled", col("Supplies_GM"))
      .withColumn("Supplies_GM", col("Supplies_GM_unscaled") * (lit(1) + ((col("Promo_Pct") - col("avg_discount_SKU_Account")) * col("supplies_GM_scaling_factor"))))
      .withColumn("Supplies_GM_no_promo", col("Supplies_GM_unscaled") * (lit(1) + ((lit(0) - col("avg_discount_SKU_Account")) * col("supplies_GM_scaling_factor"))))
      .withColumn("Supplies_Rev_unscaled", col("Supplies_Rev"))
      .withColumn("Supplies_Rev", col("Supplies_Rev_unscaled") * (lit(1) + ((col("Promo_Pct") - col("avg_discount_SKU_Account")) * col("supplies_GM_scaling_factor"))))
      .withColumn("Supplies_Rev_no_promo", col("Supplies_Rev_unscaled") * (lit(1) + ((lit(0) - col("avg_discount_SKU_Account")) * col("supplies_GM_scaling_factor"))))
      .withColumn("L1_cannibalization_log", log(lit(1) - col("L1_cannibalization")))
      .withColumn("L2_cannibalization_log", log(lit(1) - col("L2_cannibalization")))
      .withColumn("L1_competition_log", log(lit(1) - col("L1_competition")))
      .withColumn("L2_competition_log", log(lit(1) - col("L2_competition")))
      .withColumn("L1_cannibalization_log", when(col("L1_cannibalization_log").isNull, 0).otherwise(col("L1_cannibalization_log")))
      .withColumn("L2_cannibalization_log", when(col("L2_cannibalization_log").isNull, 0).otherwise(col("L2_cannibalization_log")))
      .withColumn("L1_competition_log", when(col("L1_competition_log").isNull, 0).otherwise(col("L1_competition_log")))
      .withColumn("L2_competition_log", when(col("L2_competition_log").isNull, 0).otherwise(col("L2_competition_log")))
      .drop("SKU_category")

    retailWithCompCannDF.write.mode(SaveMode.Overwrite).option("header", true).csv("/etherData/retailTemp/RetailFeatEngg/retail-SuppliesGM-PART13.csv")


  }
}
