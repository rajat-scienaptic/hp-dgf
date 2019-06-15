package com.scienaptic.jobs.core.pricing.retail

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import com.scienaptic.jobs.ExecutionContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object RetailPreRegressionPart13 {

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

    retailWithCompCannDF = retailWithCompCannDF
      .na.fill(0, Seq("Promo_Pct"))
      .withColumn("Supplies_GM_unscaled", col("Supplies_GM"))
      .withColumn("Supplies_GM", col("Supplies_GM_unscaled") * (lit(1) + ((col("Promo_Pct") - col("avg_discount_SKU_Account")) * col("supplies_GM_scaling_factor"))))
      .withColumn("Supplies_GM_no_promo", col("Supplies_GM_unscaled") * (lit(1) + ((lit(0) - col("avg_discount_SKU_Account")) * col("supplies_GM_scaling_factor"))))
    retailWithCompCannDF = retailWithCompCannDF
      .withColumn("Supplies_Rev_unscaled", col("Supplies_Rev"))
      .withColumn("Supplies_Rev", col("Supplies_Rev_unscaled") * (lit(1) + ((col("Promo_Pct") - col("avg_discount_SKU_Account")) * col("supplies_GM_scaling_factor"))))
      .withColumn("Supplies_Rev_no_promo", col("Supplies_Rev_unscaled") * (lit(1) + ((lit(0) - col("avg_discount_SKU_Account")) * col("supplies_GM_scaling_factor"))))
      // CR1 - No longer variables required as commented in R code
      /*.withColumn("L1_cannibalization_log", log(lit(1) - col("L1_cannibalization")))
      .withColumn("L2_cannibalization_log", log(lit(1) - col("L2_cannibalization")))
      .withColumn("L1_competition_log", log(lit(1) - col("L1_competition")))
      .withColumn("L2_competition_log", log(lit(1) - col("L2_competition")))
      .withColumn("L1_cannibalization_log", when(col("L1_cannibalization_log").isNull, 0).otherwise(col("L1_cannibalization_log")))
      .withColumn("L2_cannibalization_log", when(col("L2_cannibalization_log").isNull, 0).otherwise(col("L2_cannibalization_log")))
      .withColumn("L1_competition_log", when(col("L1_competition_log").isNull, 0).otherwise(col("L1_competition_log")))
      .withColumn("L2_competition_log", when(col("L2_competition_log").isNull, 0).otherwise(col("L2_competition_log")))*/
      .drop("SKU_category")

    retailWithCompCannDF.write.mode(SaveMode.Overwrite).option("header", true).csv("/etherData/retailTemp/RetailFeatEngg/retail-SuppliesGM-PART13.csv")


  }
}
