package com.scienaptic.jobs.core.pricing.retail

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import com.scienaptic.jobs.ExecutionContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object RetailPreRegressionPart20 {

  def execute(executionContext: ExecutionContext): Unit = {
    val spark: SparkSession = executionContext.spark

    var retailWithCompCann3DF  = executionContext.spark.read.option("header", true).option("inferSchema", true).csv("/home/avik/Scienaptic/HP/data/May31_Run/spark_out_retail/retail-CateCannOfflineOnline-PART19.csv")
      .withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA_date", to_date(unix_timestamp(col("GA_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES_date", to_date(unix_timestamp(col("ES_date"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("EOL_Date", to_date(unix_timestamp(col("EOL_Date"), "yyyy-MM-dd").cast("timestamp"))).cache()

    retailWithCompCann3DF = retailWithCompCann3DF
      .withColumn("cann_group", lit("None"))
      .withColumn("cann_group", when(col("SKU_Name").contains("M20") || col("SKU_Name").contains("M40"), "M201_M203/M402").otherwise(col("cann_group")))
      .withColumn("cann_group", when(col("SKU_Name").contains("M22") || col("SKU_Name").contains("M42"), "M225_M227/M426").otherwise(col("cann_group")))
      .withColumn("cann_group", when(col("SKU_Name").contains("M25") || col("SKU_Name").contains("M45"), "M252_M254/M452").otherwise(col("cann_group")))
      .withColumn("cann_group", when(col("SKU_Name").contains("M27") || col("SKU_Name").contains("M28") || col("SKU_Name").contains("M47"), "M277_M281/M477").otherwise(col("cann_group")))
      .withColumn("cann_group", when(col("SKU_Name").contains("8710") || col("SKU_Name").contains("8720"), "Weber").otherwise(col("cann_group")))
      .withColumn("cann_group", when(col("SKU_Name").contains("6968") || col("SKU_Name").contains("6978"), "Muscatel").otherwise(col("cann_group")))
      .withColumn("cann_group", when(col("SKU_Name").contains("6255") || col("SKU_Name").contains("7155") || col("SKU_Name").contains("7855"), "Palermo").otherwise(col("cann_group")))
      .withColumn("cann_receiver", lit("None"))
      .withColumn("cann_receiver", when(col("SKU_Name").contains("M40"), "M402").otherwise(col("cann_receiver")))
      .withColumn("cann_receiver", when(col("SKU_Name").contains("M42"), "M426").otherwise(col("cann_receiver")))
      .withColumn("cann_receiver", when(col("SKU_Name").contains("M45"), "M452").otherwise(col("cann_receiver")))
      .withColumn("cann_receiver", when(col("SKU_Name").contains("M47"), "M477").otherwise(col("cann_receiver")))

    retailWithCompCann3DF = retailWithCompCann3DF
      //.withColumn("is201", when(col("SKU_Name").contains("M201") or col("SKU_Name").contains("M203"), 1).otherwise(0))
      .withColumn("is225", when(col("SKU_Name").contains("M225") or col("SKU_Name").contains("M227"), 1).otherwise(0))
      //.withColumn("is252", when(col("SKU_Name").contains("M252") or col("SKU_Name").contains("M254"), 1).otherwise(0))
      //.withColumn("is277", when(col("SKU_Name").contains("M277") or col("SKU_Name").contains("M281"), 1).otherwise(0))
      .withColumn("isM40", when(col("SKU_Name").contains("M40"), 1).otherwise(0))
      .withColumn("isM42", when(col("SKU_Name").contains("M42"), 1).otherwise(0))
      .withColumn("isM45", when(col("SKU_Name").contains("M45"), 1).otherwise(0))
      .withColumn("isM47", when(col("SKU_Name").contains("M47"), 1).otherwise(0))
      .withColumn("isWeber", when(col("cann_group") === "Weber", 1).otherwise(0))
      .withColumn("isMuscatel", when(col("cann_group") === "Muscatel", 1).otherwise(0))
      .withColumn("isPalermo", when(col("cann_group") === "Palermo", 1).otherwise(0))
      .withColumn("isMuscatelForWeber", when(col("cann_group") === "Muscatel", 1).otherwise(0))
      .withColumn("isWeberForMuscatel", when(col("cann_group") === "Weber", 1).otherwise(0))
      .withColumn("isPalermoForMuscatel", when(col("cann_group") === "Palermo", 1).otherwise(0))
      .withColumn("isPalermoForMuscatel", when(col("cann_group") === "Muscatel", 1).otherwise(0))

    //val retailWeek1 = retailWithCompCann3DF.where(col("isM40") === 1).groupBy("Week_End_Date").agg(mean(col("NP_IR") + col("ASP_IR")).alias("Direct_Cann_201"))
      //.withColumn("is201", lit(1))
    val retailWeek2 = retailWithCompCann3DF.where(col("isM42") === 1).groupBy("Week_End_Date").agg(mean(col("NP_IR") + col("ASP_IR")).alias("Direct_Cann_225"))
      .withColumn("is225", lit(1))
    //val retailWeek3 = retailWithCompCann3DF.where(col("isM45") === 1).groupBy("Week_End_Date").agg(mean(col("NP_IR") + col("ASP_IR")).alias("Direct_Cann_252"))
      //.withColumn("is252", lit(1))
    //val retailWeek4 = retailWithCompCann3DF.where(col("isM47") === 1).groupBy("Week_End_Date").agg(mean(col("NP_IR") + col("ASP_IR")).alias("Direct_Cann_277"))
      //.withColumn("is277", lit(1))

    val retailWeek5 = retailWithCompCann3DF.where(col("isMuscatelForWeber") === 1).groupBy("Week_End_Date").agg(mean(col("NP_IR") + col("ASP_IR")).alias("Direct_Cann_Weber"))
      .withColumn("isWeber", lit(1))
    val retailWeek6 = retailWithCompCann3DF.where(col("isWeberForMuscatel") === 1).groupBy("Week_End_Date").agg(mean(col("NP_IR") + col("ASP_IR")).alias("Direct_Cann_Muscatel_Weber"))
      .withColumn("isMuscatel", lit(1))
    val retailWeek7 = retailWithCompCann3DF.where(col("isPalermoForMuscatel") === 1).groupBy("Week_End_Date").agg(mean(col("NP_IR") + col("ASP_IR")).alias("Direct_Cann_Muscatel_Palermo"))
      .withColumn("isMuscatel", lit(1))
    val retailWeek8 = retailWithCompCann3DF.where(col("isPalermoForMuscatel") === 1).groupBy("Week_End_Date").agg(mean(col("NP_IR") + col("ASP_IR")).alias("Direct_Cann_Palermo"))
      .withColumn("isPalermo", lit(1))

    retailWithCompCann3DF = retailWithCompCann3DF//.join(retailWeek1, Seq("is201", "Week_End_Date"), "left")
      .join(retailWeek2, Seq("is225", "Week_End_Date"), "left")
      //.join(retailWeek3, Seq("is252", "Week_End_Date"), "left")
      //.join(retailWeek4, Seq("is277", "Week_End_Date"), "left")
      .join(retailWeek5, Seq("isWeber", "Week_End_Date"), "left")
      .join(retailWeek6, Seq("isMuscatel", "Week_End_Date"), "left")
      .join(retailWeek7, Seq("isMuscatel", "Week_End_Date"), "left")
      .join(retailWeek8, Seq("isPalermo", "Week_End_Date"), "left")
      //.withColumn("Direct_Cann_201", when(col("Direct_Cann_201").isNull, 0).otherwise(col("Direct_Cann_201")))
      .withColumn("Direct_Cann_225", when(col("Direct_Cann_225").isNull, 0).otherwise(col("Direct_Cann_225")))
      //.withColumn("Direct_Cann_252", when(col("Direct_Cann_252").isNull, 0).otherwise(col("Direct_Cann_252")))
      //.withColumn("Direct_Cann_277", when(col("Direct_Cann_277").isNull, 0).otherwise(col("Direct_Cann_277")))
      .withColumn("Direct_Cann_Weber", when(col("Direct_Cann_Weber").isNull, 0).otherwise(col("Direct_Cann_Weber")))
      .withColumn("Direct_Cann_Muscatel_Weber", when(col("Direct_Cann_Muscatel_Weber").isNull, 0).otherwise(col("Direct_Cann_Muscatel_Weber")))
      .withColumn("Direct_Cann_Palermo", when(col("Direct_Cann_Palermo").isNull, 0).otherwise(col("Direct_Cann_Palermo")))
      .withColumn("LBB", when(col("Account") === "Amazon-Proper", col("LBB")).otherwise(lit(0)))
      .withColumn("LBB", when(col("LBB").isNull, 0).otherwise(col("LBB")))
      .withColumnRenamed("Direct_Cann_Muscatel_Palermo", "Direct_Cann_Muscatel_Palermo2")
      .drop("Direct_Cann_Muscatel_Palermo")

    //Direct_Cann_201
    var tempMeanIR = retailWithCompCann3DF.select("SKU_Name","Week_End_Date","NP_IR","ASP_IR").where(col("SKU_Name").contains("M40"))
    tempMeanIR = tempMeanIR
      .groupBy("Week_End_Date")
      .agg(mean(col("NP_IR")+col("ASP_IR")).as("Direct_Cann_201"),
        sum(when(col("NP_IR").isNull || col("ASP_IR").isNull, 1).otherwise(0)).as("Direct_Cann_NCount"))
      .withColumn("Direct_Cann_201", when(col("Direct_Cann_NCount") > 0, null).otherwise(col("Direct_Cann_201")))
        .drop("Direct_Cann_NCount")
    retailWithCompCann3DF = retailWithCompCann3DF.join(tempMeanIR, Seq("Week_End_Date"), "left")
        .withColumn("Direct_Cann_201", when(col("SKU_Name").contains("M201") || col("SKU_Name").contains("M203"), col("Direct_Cann_201")).otherwise(0))

    //Direct_Cann_252
    tempMeanIR = retailWithCompCann3DF.select("SKU_Name","Week_End_Date","NP_IR","ASP_IR")
      .where(col("SKU_Name").contains("M45"))
    tempMeanIR = tempMeanIR
      .groupBy("Week_End_Date")
      .agg(mean(col("NP_IR")+col("ASP_IR")).as("Direct_Cann_252"),
          sum(when(col("NP_IR").isNull || col("ASP_IR").isNull, 1).otherwise(0)).as("Direct_Cann_NCount"))
      .withColumn("Direct_Cann_252", when(col("Direct_Cann_NCount") > 0, null).otherwise(col("Direct_Cann_252")))
        .drop("Direct_Cann_NCount")
    retailWithCompCann3DF = retailWithCompCann3DF.join(tempMeanIR, Seq("Week_End_Date"), "left")
      .withColumn("Direct_Cann_252", when(col("SKU_Name").contains("M252") || col("SKU_Name").contains("M254"), col("Direct_Cann_252")).otherwise(0))

    //Direct_Cann_277
    tempMeanIR = retailWithCompCann3DF.select("SKU_Name","Week_End_Date","NP_IR","ASP_IR")
      .where(col("SKU_Name").contains("M47"))
    tempMeanIR = tempMeanIR
      .groupBy("Week_End_Date").agg(mean(col("NP_IR")+col("ASP_IR")).as("Direct_Cann_277"),
          sum(when(col("NP_IR").isNull || col("ASP_IR").isNull, 1).otherwise(0)).as("Direct_Cann_NCount"))
      .withColumn("Direct_Cann_277", when(col("Direct_Cann_NCount") > 0, null).otherwise(col("Direct_Cann_277")))
    retailWithCompCann3DF = retailWithCompCann3DF.join(tempMeanIR, Seq("Week_End_Date"), "left")
      .withColumn("Direct_Cann_277", when(col("SKU_Name").contains("M277") || col("SKU_Name").contains("M281"), col("Direct_Cann_277")).otherwise(0))

    //Muscatel Palermo change
    tempMeanIR = retailWithCompCann3DF.where(col("cann_group")==="Palermo")
      .select("cann_group","Week_End_Date","NP_IR","ASP_IR")
    tempMeanIR = tempMeanIR.groupBy("Week_End_Date")
      .agg(mean(col("NP_IR")+col("ASP_IR")).as("Direct_Cann_Muscatel_Palermo"))

    retailWithCompCann3DF = retailWithCompCann3DF.join(tempMeanIR, Seq("Week_End_Date"), "left")
    retailWithCompCann3DF = retailWithCompCann3DF.withColumn("Direct_Cann_Muscatel_Palermo", when(col("cann_group")==="Muscatel",col("Direct_Cann_Muscatel_Palermo")).otherwise(0))
      .na.fill(0, Seq("Direct_Cann_Muscatel_Palermo","Direct_Cann_201","Direct_Cann_225","Direct_Cann_252","Direct_Cann_277","Direct_Cann_Weber","Direct_Cann_Muscatel_Weber","Direct_Cann_Palermo"))

    retailWithCompCann3DF.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("/home/avik/Scienaptic/HP/data/May31_Run/spark_out_retail/retail-DirectCann-PART20.csv")

  }
}
