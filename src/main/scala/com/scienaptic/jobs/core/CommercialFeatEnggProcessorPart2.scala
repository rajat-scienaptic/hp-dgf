package com.scienaptic.jobs.core

import java.text.SimpleDateFormat

import com.scienaptic.jobs.ExecutionContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import com.scienaptic.jobs.bean.UnionOperation.doUnion
import java.util.UUID
import org.apache.spark.storage.StorageLevel

import com.scienaptic.jobs.core.CommercialFeatEnggProcessor.stability_weeks
import org.apache.spark.sql.functions.rank
import com.scienaptic.jobs.utility.Utils.renameColumns
import com.scienaptic.jobs.utility.CommercialUtility.{addDaystoDateStringUDF, checkPrevQtsGTBaseline, concatenateRank, createlist, extractWeekFromDateUDF}
import org.apache.spark.sql.expressions.Window

object CommercialFeatEnggProcessorPart2 {
  val Cat_switch=1
  val min_baseline = 2
  val stability_weeks = 4
  val intro_weeks = 6

  val dat2000_01_01 = to_date(unix_timestamp(lit("2000-01-01"),"yyyy-MM-dd").cast("timestamp"))
  val dat9999_12_31 = to_date(unix_timestamp(lit("9999-12-31"),"yyyy-MM-dd").cast("timestamp"))

/*  val indexerForSpecialPrograms = new StringIndexer().setInputCol("Special_Programs").setOutputCol("Special_Programs_fact").setHandleInvalid("keep")
  val pipelineForSpecialPrograms = new Pipeline().setStages(Array(indexerForSpecialPrograms))

  val indexerForResellerCluster = new StringIndexer().setInputCol("Reseller_Cluster").setOutputCol("Reseller_Cluster_fact").setHandleInvalid("keep")
  val pipelineForResellerCluster= new Pipeline().setStages(Array(indexerForResellerCluster))
*/
  def execute(executionContext: ExecutionContext): Unit = {

    val sparkConf = new SparkConf().setAppName("Test")
    val spark = SparkSession.builder
      .master("yarn-client")
      .appName("Commercial-R-2")
      .config(sparkConf)
      .getOrCreate

    val baselineThreshold = if (min_baseline/2 > 0) min_baseline/2 else 0

    import spark.implicits._

    var commercialWithCompCannDF = spark.read.option("header","true").option("inferSchema","true").csv("/etherData/commercialTemp/CommercialFeatEngg/commercialWithCompCannDF.csv")
      .persist(StorageLevel.MEMORY_AND_DISK)

    val wind = Window.partitionBy("SKU_Name","Reseller_Cluster").orderBy("Qty")
    commercialWithCompCannDF.createOrReplaceTempView("commercial")

    val percentil75DF = spark.sql("select SKU_Name, Reseller_Cluster, Reseller_Cluster_LEVELS, PERCENTILE(Qty, 0.75) OVER (PARTITION BY SKU_Name, Reseller_Cluster, Reseller_Cluster_LEVELS) as percentile_0_75 from commercial")
      .dropDuplicates("SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS","percentile_0_75")
    //writeDF(percentil75DF,"percentil75DF")
    commercialWithCompCannDF = commercialWithCompCannDF.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      .join(percentil75DF.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("SKU_Name", "Reseller_Cluster","Reseller_Cluster_LEVELS"), "left")
    commercialWithCompCannDF.createOrReplaceTempView("commercial")
    val percentile25DF = spark.sql("select SKU_Name, Reseller_Cluster, Reseller_Cluster_LEVELS, PERCENTILE(Qty, 0.25) OVER (PARTITION BY SKU_Name, Reseller_Cluster, Reseller_Cluster_LEVELS) as percentile_0_25 from commercial")
      .dropDuplicates("SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS","percentile_0_25")
    //writeDF(percentile25DF,"percentile25DF")
    commercialWithCompCannDF = commercialWithCompCannDF.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      .join(percentile25DF.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("SKU_Name", "Reseller_Cluster","Reseller_Cluster_LEVELS"), "left")
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_WITH_75_25_QUANTILE")
    commercialWithCompCannDF = commercialWithCompCannDF.withColumn("IQR", col("percentile_0_75")-col("percentile_0_25"))
      .withColumn("outlier", when(col("Qty")>col("percentile_0_75"), (col("Qty")-col("percentile_0_75"))/col("IQR")).otherwise(when(col("Qty")<col("percentile_0_25"), (col("Qty")-col("percentile_0_25"))/col("IQR")).otherwise(lit(0))))
      .withColumn("spike", when(abs(col("outlier"))<=8, 0).otherwise(1))
      .withColumn("spike", when((col("SKU_Name")==="OJ Pro 8610") && (col("Reseller_Cluster")==="Other - Option B") && (col("Week_End_Date")==="2014-11-01"),1).otherwise(col("spike")))
      .withColumn("spike", when((col("SKU").isin("F6W14A")) && (col("Week_End_Date")==="2017-07-10") && (col("Reseller_Cluster").isin("Other - Option B")), 1).otherwise(col("spike")))
      .withColumn("spike2", when((col("spike")===1) && (col("IR")>0), 0).otherwise(col("spike")))
      .drop("percentile_0_75", "percentile_0_25","IQR")
      .withColumn("Qty", col("Qty").cast("int")).repartition(1000).cache()
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_Spike")*/

    commercialWithCompCannDF = commercialWithCompCannDF.withColumn("Qty",col("Qty").cast("int"))
    val commercialWithHolidayAndQtyFilter = commercialWithCompCannDF.withColumn("Qty",col("Qty").cast("int"))
      .where((col("Promo_Flag")===0) && (col("USThanksgivingDay")===0) && (col("USCyberMonday")===0) && (col("spike")===0))
      .where(col("Qty")>0).cache()
    //writeDF(commercialWithHolidayAndQtyFilter,"commercialWithHolidayAndQtyFilter_FILTER_FOR_NPBL")
    var npbl = commercialWithHolidayAndQtyFilter
      .groupBy("Reseller_Cluster","Reseller_Cluster_LEVELS","SKU_Name")
      .agg(mean("Qty").as("no_promo_avg"),
        stddev("Qty").as("no_promo_sd"),
        min("Qty").as("no_promo_min"),
        max("Qty").as("no_promo_max"))
    //writeDF(npbl,"npbl_BEFORE_MEDIAN")
    commercialWithHolidayAndQtyFilter.createOrReplaceTempView("npbl")
    val npblTemp = spark.sql("select SKU_Name, Reseller_Cluster, Reseller_Cluster_LEVELS, PERCENTILE(Qty, 0.50) OVER (PARTITION BY SKU_Name, Reseller_Cluster, Reseller_Cluster_LEVELS) as no_promo_med from npbl")
      .dropDuplicates("SKU_Name", "Reseller_Cluster", "Reseller_Cluster_LEVELS","no_promo_med")
    //writeDF(npblTemp,"npblTemp")
    npbl = npbl.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      .join(npblTemp.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("Reseller_Cluster","SKU_Name","Reseller_Cluster_LEVELS"), "inner")
    //writeDF(npbl,"npbl")

    commercialWithCompCannDF = commercialWithCompCannDF.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      .join(npbl.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster")).select("SKU_Name", "Reseller_Cluster","Reseller_Cluster_LEVELS","no_promo_avg", "no_promo_med"), Seq("SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS"), "left")
      .withColumn("no_promo_avg", when(col("no_promo_avg").isNull, 0).otherwise(col("no_promo_avg")))
      .withColumn("no_promo_med", when(col("no_promo_med").isNull, 0).otherwise(col("no_promo_med")))
      .withColumn("low_baseline", when(((col("no_promo_avg")>=min_baseline) && (col("no_promo_med")>=baselineThreshold)) || ((col("no_promo_med")>=min_baseline) && (col("no_promo_avg")>=baselineThreshold)),0).otherwise(1))
      .withColumn("low_volume", when(col("Qty")>0,0).otherwise(1))
      .withColumn("raw_bl_avg", col("no_promo_avg")*(col("seasonality_npd")+lit(1)))
      .withColumn("raw_bl_med", col("no_promo_med")*(col("seasonality_npd")+lit(1)))
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_AFTER_NPBL")*/

    val windForSKUAndReseller = Window.partitionBy("SKU&Reseller")
      .orderBy(/*"SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS",*/"Week_End_Date")

    var EOLcriterion = commercialWithCompCannDF
      .groupBy("SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS","Week_End_Date")
      .agg(sum("Qty").as("Qty"), sum("no_promo_med").as("no_promo_med"))
      .sort("SKU_Name","Reseller_Cluster_LEVELS","Week_End_Date")
      .withColumn("Qty&no_promo_med", concat_ws(";",col("Qty"), col("no_promo_med")))
      .withColumn("SKU&Reseller", concat(col("SKU_Name"), col("Reseller_Cluster_LEVELS"))).cache()
    //writeDF(EOLcriterion,"EOLcriterion_FIRST")

    EOLcriterion = EOLcriterion.orderBy("SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS","Week_End_Date")
      .withColumn("rank", row_number().over(windForSKUAndReseller))
    //writeDF(EOLcriterion,"EOLcriterion_WITH_RANK")

    var EOLWithCriterion1 = EOLcriterion
      .groupBy("SKU&Reseller").agg((collect_list(concat_ws("_",col("rank"),col("Qty")).cast("string"))).as("QtyArray"))

    EOLWithCriterion1 = EOLWithCriterion1
      .withColumn("QtyArray", when(col("QtyArray").isNull, null).otherwise(concatenateRank(col("QtyArray"))))
    ////writeDF(EOLWithCriterion1,"EOLWithCriterion1_WITH_QTYARRAY")

    EOLcriterion = EOLcriterion.join(EOLWithCriterion1, Seq("SKU&Reseller"), "left")
      .withColumn("EOL_criterion", when(col("rank")<=stability_weeks || col("Qty")<col("no_promo_med"), 0).otherwise(checkPrevQtsGTBaseline(col("QtyArray"), col("rank"), col("no_promo_med"), lit(stability_weeks))))
      .drop("rank","QtyArray","SKU&Reseller","Qty&no_promo_med")
    //writeDF(EOLcriterion,"EOLcriterion_BEFORE_LAST_MIN_MAX")

    val EOLCriterionLast = EOLcriterion.where(col("EOL_criterion")===lit(1))
      .groupBy("SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS")
      .agg(max("Week_End_Date").as("last_date"))
    //writeDF(EOLCriterionLast,"EOLCriterionLast")

    val EOLCriterionMax = commercialWithCompCannDF
      .groupBy("SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS")
      .agg(max("Week_End_Date").as("max_date"))
    //writeDF(EOLCriterionMax,"EOLCriterionMax")

    EOLcriterion = EOLCriterionMax.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      .withColumn("Reseller_Cluster_LEVELS",col("Reseller_Cluster_LEVELS"))
      .join(EOLCriterionLast.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster")).withColumn("Reseller_Cluster_LEVELS",col("Reseller_Cluster_LEVELS")), Seq("SKU_Name","Reseller_Cluster_LEVELS","Reseller_Cluster"), "left")
      .where(col("last_date").isNotNull)
    //writeDF(EOLcriterion,"EOLcriterion_BEFORE_MAXMAXDAte")

    val maxMaxDate = EOLcriterion.agg(max("max_date")).head().getDate(0)
    EOLcriterion = EOLcriterion
      .where((col("max_date")=!=col("last_date")) || (col("last_date")=!=maxMaxDate))
      .drop("max_date")
    //writeDF(EOLcriterion,"EOLcriterion_AFTER_MAXMAX")

    commercialWithCompCannDF = commercialWithCompCannDF.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      .join(EOLcriterion.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS"), "left")
      .withColumn("EOL", when(col("last_date").isNull, 0).otherwise(when(col("Week_End_Date")>col("last_date"),1).otherwise(0)))
      .drop("last_date").cache()
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_BEFORE_SKU_COMP_IN_EOL")
    commercialWithCompCannDF = commercialWithCompCannDF
      .withColumn("EOL", when(col("SKU").isin("G3Q47A","M9L75A","F8B04A","B5L24A","L2719A","D3Q19A","F2A70A","CF377A","L2747A","F0V69A","G3Q35A","C5F93A","CZ271A","CF379A","B5L25A","D3Q15A","B5L26A","L2741A","CF378A","L2749A","CF394A"),0).otherwise(col("EOL")))
      .withColumn("EOL", when((col("SKU")==="C5F94A") && (col("Season")=!="STS'17"), 0).otherwise(col("EOL"))).repartition(500)
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_WITH_EOL")*/

    var BOL = commercialWithCompCannDF.select("SKU","ES date_LEVELS","GA date_LEVELS")
      .withColumnRenamed("ES date_LEVELS","ES date").withColumnRenamed("GA date_LEVELS","GA date").dropDuplicates().cache()
    //writeDF(BOL,"BOL_WITH_DROP_DUPLICATES")

    BOL = BOL.where((col("ES date").isNotNull) || (col("GA date").isNotNull))
    //writeDF(BOL,"BOL_WITH_ES_GA_DATE_CHECK")

    BOL = BOL
      .withColumn("ES date_wday", dayofweek(col("ES date")).cast("int"))  //As dayofweek returns in range 1-7 we want 0-6
      .withColumn("GA date_wday", dayofweek(col("GA date")).cast("int"))
      .withColumn("ES date_wday_sub", lit(7)-col("ES date_wday"))
      .withColumn("GA date_wday_sub", lit(7)-col("GA date_wday"))
      .withColumn("ES date_wday_sub", when(col("ES date_wday_sub").isNull,0).otherwise(col("ES date_wday_sub")))
      .withColumn("GA date_wday_sub", when(col("GA date_wday_sub").isNull,0).otherwise(col("GA date_wday_sub")))
    //writeDF(BOL,"BOL_WITH_WDAYs")

    BOL = BOL
      .withColumnRenamed("GA date","GA_date").withColumnRenamed("ES date","ES_date")
      .withColumnRenamed("GA date_wday_sub","GA_date_wday_sub").withColumnRenamed("ES date_wday_sub","ES_date_wday_sub")
      .withColumn("GA_date", expr("date_add(GA_date,GA_date_wday_sub)"))
      .withColumn("ES_date", expr("date_add(ES_date,ES_date_wday_sub)"))
      .withColumnRenamed("GA_date","GA date").withColumnRenamed("ES_date","ES date")
      .withColumnRenamed("GA_date_wday_sub","GA date_wday_sub").withColumnRenamed("ES_date_wday_sub","ES date_wday_sub")
      .drop("GA date_wday","ES date_wday","GA date_wday_sub","ES date_wday_sub")
    //writeDF(BOL,"BOL_WITH_WDAY_MODIFIED")

    val windForSKUnReseller = Window.partitionBy("SKU$Reseller").orderBy(/*"SKU","Reseller_Cluster","Reseller_Cluster_LEVELS",*/"Week_End_Date")
    var BOLCriterion = commercialWithCompCannDF
      .groupBy("SKU","Reseller_Cluster","Reseller_Cluster_LEVELS","Week_End_Date")
      .agg(sum("Qty").as("Qty"))
      .sort("SKU","Reseller_Cluster","Reseller_Cluster_LEVELS","Week_End_Date")
      .withColumn("SKU$Reseller", concat(col("SKU"),col("Reseller_Cluster_LEVELS")))
      .withColumn("rank", row_number().over(windForSKUnReseller))
      .withColumn("BOL_criterion", when(col("rank")<intro_weeks, 0).otherwise(1))
      .drop("rank","Qty").cache()
    //writeDF(BOLCriterion,"BOLCriterion_Before_MinWedDATE")

    BOLCriterion = BOLCriterion
      .join(BOL.select("SKU","GA date"), Seq("SKU"), "left")
    val minWEDDate = to_date(unix_timestamp(lit(BOLCriterion.agg(min("Week_End_Date")).head().getDate(0)),"yyyy-MM-dd").cast("timestamp"))
    BOLCriterion = BOLCriterion.withColumn("GA date", when(col("GA date").isNull, minWEDDate).otherwise(col("GA date")))
      .where(col("Week_End_Date")>=col("GA date"))
    //writeDF(BOLCriterion,"BOLCriterion_WITH_WEEK_END_DATE_FILTER_AGAINST_GA")
    val BOLCriterionFirst = BOLCriterion.where(col("BOL_criterion")===1)
      .groupBy("SKU","Reseller_Cluster","Reseller_Cluster_LEVELS")
      .agg(min("Week_End_Date").as("first_date"))
    //writeDF(BOLCriterionFirst,"BOLCriterionFirst")
    val BOLCriterionMax = commercialWithCompCannDF
      .groupBy("SKU","Reseller_Cluster","Reseller_Cluster_LEVELS")
      .agg(max("Week_End_Date").as("max_date"))
    //writeDF(BOLCriterionMax,"BOLCriterionMax")
    val BOLCriterionMin = commercialWithCompCannDF//.where(col("BOL_criterion")===1)
      .groupBy("SKU","Reseller_Cluster","Reseller_Cluster_LEVELS")
      .agg(min("Week_End_Date").as("min_date"))
    //writeDF(BOLCriterionMin,"BOLCriterionMin")

    BOLCriterion =  BOLCriterionMax.withColumn("SKU",col("SKU")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      .join(BOLCriterionFirst.withColumn("SKU",col("SKU")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("SKU","Reseller_Cluster","Reseller_Cluster_LEVELS"), "left")
      .join(BOLCriterionMin.withColumn("SKU",col("SKU")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("SKU","Reseller_Cluster","Reseller_Cluster_LEVELS"), "left")
        .persist(StorageLevel.MEMORY_AND_DISK)
    //writeDF(BOLCriterion,"BOLCriterion_AFTER_FIRST_MIN_MAX_JOIN")*/

    BOLCriterion = BOLCriterion.withColumn("first_date", when(col("first_date").isNull, col("max_date")).otherwise(col("first_date")))
      .drop("max_date")
    //writeDF(BOLCriterion,"BOLCriterion_WITH_FIRST_DATE")
    val minMinDateBOL = BOLCriterion.agg(min("min_date")).head().getDate(0)
    BOLCriterion = BOLCriterion
      .where(!((col("min_date")===col("first_date")) && (col("first_date")===minMinDateBOL)))
      .withColumn("diff_weeks", ((datediff(to_date(col("first_date")),to_date(col("min_date"))))/7)+1)

    BOLCriterion = BOLCriterion.withColumn("diff_weeks", when(col("diff_weeks").isNull || col("diff_weeks")<=0, 0).otherwise(col("diff_weeks")))
    BOLCriterion = BOLCriterion.withColumn("diff_weeks", col("diff_weeks").cast("int"))
    //writeDF(BOLCriterion,"BOLCriterion_WITH_DIFF_WEEKS")*/

    BOLCriterion = BOLCriterion
      .withColumn("repList", createlist(col("diff_weeks").cast("int")))
      .withColumn("add", explode(col("repList"))).drop("repList")
      .withColumn("add", col("add")/*+lit(1)*/)
    //writeDF(BOLCriterion,"BOLCriterion_AFTER_EXPLODE")
    BOLCriterion = BOLCriterion
      .withColumn("add", col("add")*lit(7))
      .withColumn("Week_End_Date", expr("date_add(min_date,add)"))
    //.withColumn("Week_End_Date", addDaystoDateStringUDF(col("min_date"), col("add")))   //CHECK: check if min_date is in timestamp format!

    BOLCriterion = BOLCriterion.drop("min_date","fist_date","diff_weeks","add")
      .withColumn("BOL_criterion", lit(1))
    //writeDF(BOLCriterion, "BOLCriterion_FINAL_DF")

    commercialWithCompCannDF = commercialWithCompCannDF.drop("GA date").withColumn("GA date",col("GA date_LEVELS"))//TODO Remove this
      .withColumn("SKU",col("SKU")).withColumn("Reseller_Cluster",col("Reseller_Cluster")).withColumn("Week_End_Date",col("Week_End_Date"))
      .join(BOLCriterion.withColumn("SKU",col("SKU")).withColumn("Reseller_Cluster",col("Reseller_Cluster")).withColumn("Week_End_Date",col("Week_End_Date")), Seq("SKU","Reseller_Cluster","Reseller_Cluster_LEVELS","Week_End_Date"), "left")
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_WITH_BOL_JOINED_BEFORE_NULL_IMPUTATION")*/

    commercialWithCompCannDF = commercialWithCompCannDF
      .withColumn("BOL", when(col("EOL")===1,0).otherwise(col("BOL_criterion")))
      .withColumn("BOL", when(datediff(col("Week_End_Date"),col("GA date_LEVELS"))<(7*6),1).otherwise(col("BOL")))
      .withColumn("BOL", when(col("GA date_LEVELS").isNull, 0).otherwise(col("BOL")))
      .withColumn("BOL", when(col("BOL").isNull, 0).otherwise(col("BOL"))).cache()
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_Join_BOLCRITERIA")

    val commercialEOLSpikeFilter = commercialWithCompCannDF.where((col("EOL")===0) && (col("spike")===0))
    var opposite = commercialEOLSpikeFilter
      .groupBy("SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS")
      .agg(count("SKU_Name").as("n"),
        mean("Qty").as("Qty_total"))

    var opposite_Promo_flag = commercialEOLSpikeFilter.where(col("Promo_Flag")===1)
      .groupBy("SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS")
      .agg(mean("Qty").as("Qty_promo"))

    var opposite_Promo_flag_ZERO = commercialEOLSpikeFilter.where(col("Promo_Flag")===0)
      .groupBy("SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS")
      .agg(mean("Qty").as("Qty_no_promo"))

    opposite = opposite.join(opposite_Promo_flag, Seq("SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS"), "left")
      .join(opposite_Promo_flag_ZERO, Seq("SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS"), "left").cache()

    //writeDF(opposite,"opposite_BEFORE_MERGE")
    opposite = opposite
      .withColumn("opposite", when((col("Qty_no_promo")>col("Qty_promo")) || (col("Qty_no_promo")<0), 1).otherwise(0))
      .withColumn("opposite", when(col("opposite").isNull, 0).otherwise(col("opposite")))
      .withColumn("no_promo_sales", when(col("Qty_promo").isNull, 1).otherwise(0))
    //writeDF(opposite,"opposite")
    commercialWithCompCannDF = commercialWithCompCannDF.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      .join(opposite.select("SKU_Name", "Reseller_Cluster","Reseller_Cluster_LEVELS", "opposite","no_promo_sales").withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("SKU_Name","Reseller_Cluster","Reseller_Cluster_LEVELS"), "left")
      .withColumn("NP_Flag", col("Promo_Flag"))
      .withColumn("NP_IR", col("IR"))
      .withColumn("high_disc_Flag", when(col("Promo_Pct")<=0.55, 0).otherwise(1))
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_WITH_OPPOSITE")

    val commercialPromoMean = commercialWithCompCannDF
      .groupBy("Reseller_Cluster_LEVELS","Reseller_Cluster","SKU_Name","Season")
      .agg(mean(col("Promo_Flag")).as("PromoFlagAvg"))
    //writeDF(commercialPromoMean,"commercialPromoMean_WITH_PROMO_FLAG_Avg")*/

    commercialWithCompCannDF = commercialWithCompCannDF.join(commercialPromoMean, Seq("Reseller_Cluster","Reseller_Cluster_LEVELS","SKU_Name","Season"), "left")
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_JOINED_PROMO_MEAN")
    commercialWithCompCannDF = commercialWithCompCannDF
      .withColumn("always_promo_Flag", when(col("PromoFlagAvg")===1, 1).otherwise(0)).drop("PromoFlagAvg")
      .withColumn("EOL", when(col("Reseller_Cluster_LEVELS")==="CDW",
        when(col("SKU_Name")==="LJ Pro M402dn", 0).otherwise(col("EOL"))).otherwise(col("EOL")))
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_WITH_LAST_EOL_MODIFICATION")
    commercialWithCompCannDF.persist(StorageLevel.MEMORY_AND_DISK)
    commercialWithCompCannDF = commercialWithCompCannDF
      .withColumn("cann_group", lit("None"))
      .withColumn("cann_group", when(col("SKU_Name").contains("M20") || col("SKU_Name").contains("M40"),"M201_M203/M402").otherwise(col("cann_group")))
      .withColumn("cann_group", when(col("SKU_Name").contains("M22") || col("SKU_Name").contains("M42"),"M225_M227/M426").otherwise(col("cann_group")))
      .withColumn("cann_group", when(col("SKU_Name").contains("M25") || col("SKU_Name").contains("M45"),"M252_M254/M452").otherwise(col("cann_group")))
      .withColumn("cann_group", when(col("SKU_Name").contains("M27") || col("SKU_Name").contains("M28"),"M277_M281/M477").otherwise(col("cann_group")))
      .withColumn("cann_group", when(col("SKU_Name").contains("8710") || col("SKU_Name").contains("8720") || col("SKU_Name").contains("6968") || col("SKU_Name").contains("6978"),"Weber/Muscatel").otherwise(col("cann_group")))
      .withColumn("cann_group", when(col("SKU_Name").contains("6255") || col("SKU_Name").contains("7155") || col("SKU_Name").contains("7855") || col("SKU_Name").contains("6988") || col("SKU_Name").contains("6978"),"Palermo/Muscatel").otherwise(col("cann_group")))
      .withColumn("cann_receiver", lit("None"))
      .withColumn("cann_receiver", when(col("SKU_Name").contains("M40"), "M402").otherwise(col("cann_receiver")))
      .withColumn("cann_receiver", when(col("SKU_Name").contains("M42"), "M426").otherwise(col("cann_receiver")))
      .withColumn("cann_receiver", when(col("SKU_Name").contains("M45"), "M452").otherwise(col("cann_receiver")))
      .withColumn("cann_receiver", when(col("SKU_Name").contains("M47"), "M477").otherwise(col("cann_receiver")))
      .withColumn("cann_receiver", when(col("SKU_Name").contains("8710") || col("SKU_Name").contains("8720"), "Weber").otherwise(col("cann_receiver")))
      .withColumn("cann_receiver", when(col("SKU_Name").contains("6968") || col("SKU_Name").contains("6978"), "Muscatel").otherwise(col("cann_receiver")))
      .withColumn("cann_receiver", when(col("SKU_Name").contains("6255") || col("SKU_Name").contains("7155") || col("SKU_Name").contains("7855"), "Palermo").otherwise(col("cann_receiver")))
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_AFTER_CANN_RECEIVER_CALC")

    commercialWithCompCannDF=commercialWithCompCannDF
      .withColumn("is201",when(col("SKU_Name").contains("M201") or col("SKU_Name").contains("M203"),1).otherwise(0))
      .withColumn("is225",when(col("SKU_Name").contains("M225") or col("SKU_Name").contains("M227"),1).otherwise(0))
      .withColumn("is252",when(col("SKU_Name").contains("M252") or col("SKU_Name").contains("M254"),1).otherwise(0))
      .withColumn("is277",when(col("SKU_Name").contains("M277") or col("SKU_Name").contains("M281"),1).otherwise(0))
      .withColumn("isM40",when(col("SKU_Name").contains("M40"),1).otherwise(0))
      .withColumn("isM42",when(col("SKU_Name").contains("M42"),1).otherwise(0))
      .withColumn("isM45",when(col("SKU_Name").contains("M45"),1).otherwise(0))
      .withColumn("isM47",when(col("SKU_Name").contains("M47"),1).otherwise(0))

    val commWeek1=commercialWithCompCannDF.where(col("isM40")===1).groupBy("Week_End_Date").agg(mean(col("IR")).alias("Direct_Cann_201"))
      .withColumn("is201",lit(1))
    val commWeek2=commercialWithCompCannDF.where(col("isM42")===1).groupBy("Week_End_Date").agg(mean(col("IR")).alias("Direct_Cann_225"))
      .withColumn("is225",lit(1))
    val commWeek3=commercialWithCompCannDF.where(col("isM45")===1).groupBy("Week_End_Date").agg(mean(col("IR")).alias("Direct_Cann_252"))
      .withColumn("is252",lit(1))
    val commWeek4=commercialWithCompCannDF.where(col("isM47")===1).groupBy("Week_End_Date").agg(mean(col("IR")).alias("Direct_Cann_277"))
      .withColumn("is277",lit(1))
    commercialWithCompCannDF=commercialWithCompCannDF.join(commWeek1, Seq("is201", "Week_End_Date"), "left")
      .join(commWeek2, Seq("is225", "Week_End_Date"), "left")
      .join(commWeek3, Seq("is252", "Week_End_Date"), "left")
      .join(commWeek4, Seq("is277", "Week_End_Date"), "left")
      .withColumn("Direct_Cann_201", when(col("Direct_Cann_201").isNull,0).otherwise(col("Direct_Cann_201")))
      .withColumn("Direct_Cann_225", when(col("Direct_Cann_201").isNull,0).otherwise(col("Direct_Cann_225")))
      .withColumn("Direct_Cann_252", when(col("Direct_Cann_201").isNull,0).otherwise(col("Direct_Cann_252")))
      .withColumn("Direct_Cann_277", when(col("Direct_Cann_201").isNull,0).otherwise(col("Direct_Cann_277")))
    //writeDF(commercialWithCompCannDF, "commercialWithCompCannDF_WITH_DIRECT_CANN")

    commercialWithCompCannDF = commercialWithCompCannDF.drop("is225","is201","is252","is277","isM40","isM42","isM45","isM47")
      .withColumn("Hardware_GM", when(col("Category_1")==="Value" && col("Week_End_Date")>="2016-07-01", col("Hardware_GM")+lit(68)).otherwise(col("Hardware_GM")))
      .withColumn("Hardware_GM", when(col("Category_1")==="Value" && col("Week_End_Date")>="2017-05-01", col("Hardware_GM")+lit(8)).otherwise(col("Hardware_GM")))
      .withColumn("Hardware_GM", when(col("Category Custom")==="A4 SMB" && col("Week_End_Date")>="2017-11-01", col("Hardware_GM")-lit(7.51)).otherwise(col("Hardware_GM")))
      .withColumn("Hardware_GM", when(col("Category Custom").isin("A4 Value","A3 Value") && col("Week_End_Date")>="2017-11-01", col("Hardware_GM")+lit(33.28)).otherwise(col("Hardware_GM")))
      .withColumn("Supplies_GM", when(col("L1_Category")==="Scanners",0).otherwise(col("Supplies_GM")))
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_DIRECT_CANN_20")*/

    commercialWithCompCannDF = commercialWithCompCannDF
      .withColumn("exclude", when(!col("PL_LEVELS").isin("3Y"),when(col("Reseller_Cluster_LEVELS").isin("Other - Option C", "eTailerOther - Option C", "Stockpiler & Deal Chaser", "eTailerStockpiler & Deal Chaser") || col("low_volume")===1 || col("low_baseline")===1 || col("spike")===1 || col("opposite")===1 || col("EOL")===1 || col("BOL")===1 || col("no_promo_sales")===1,1).otherwise(0)).otherwise(0))
      .withColumn("exclude", when(col("PL_LEVELS").isin("3Y"),when(col("Reseller_Cluster_LEVELS").isin("Other - Option C", "eTailerOther - Option C", "Stockpiler & Deal Chaser", "eTailerStockpiler & Deal Chaser") || col("low_volume")===1 || /*col("low_baseline")===1 ||*/ col("spike")===1 || col("opposite")===1 || col("EOL")===1 || col("BOL")===1 || col("no_promo_sales")===1,1).otherwise(0)).otherwise(col("exclude")))
      .withColumn("exclude", when(col("SKU_Name").contains("Sprocket"), 1).otherwise(col("exclude")))
      .withColumn("AE_NP_IR", col("NP_IR"))
      .withColumn("AE_ASP_IR", lit(0))
      .withColumn("AE_Other_IR", lit(0))
      .withColumn("Street_PriceWhoChange_log", when(col("Changed_Street_Price")===0, 0).otherwise(log(col("Street Price")*col("Changed_Street_Price"))))
      .withColumn("SKUWhoChange", when(col("Changed_Street_Price")===0, 0).otherwise(col("SKU")))
      .withColumn("PriceChange_HPS_OPS", when(col("Changed_Street_Price")===0, 0).otherwise(col("HPS_OPS_LEVELS")))
    //writeDF(commercialWithCompCannDF,"commercialWithCompCannDF_EXCLUDE_SKUWHOCHANGE")

    val maxWED = commercialWithCompCannDF.agg(max("Week_End_Date")).head().getDate(0)
    val maxWEDSeason = commercialWithCompCannDF.where(col("Week_End_Date")===maxWED).sort(col("Week_End_Date").desc).select("Season").head().getString(0)
    val latestSeasonCommercial = commercialWithCompCannDF.where(col("Season")===maxWEDSeason)

    val windForSeason = Window.orderBy(col("Week_End_Date").desc)
    val uniqueSeason = commercialWithCompCannDF.withColumn("rank", row_number().over(windForSeason))
      .where(col("rank")===2).select("Season").head().getString(0)

    val latestSeason = latestSeasonCommercial.select("Week_End_Date").distinct().count()
    if (latestSeason<13) {
      commercialWithCompCannDF = commercialWithCompCannDF.withColumn("Season_most_recent", when(col("Season")===maxWEDSeason,uniqueSeason).otherwise(col("Season")))
    }else {
      commercialWithCompCannDF = commercialWithCompCannDF.withColumn("Season_most_recent", col("Season"))
    }
    commercialWithCompCannDF = commercialWithCompCannDF.select("SKU_Name","Reseller_Cluster_LEVELS","SKU","Week_End_Date","L1_Category","L2_Category","Season","Street Price","IPSLES_LEVELS","HPS_OPS_LEVELS","Series","Category","Category Subgroup","Category_1_LEVELS","Category_2_LEVELS","Category_3_LEVELS","Category Custom","Line_LEVELS","PL_LEVELS","PLC Status","GA date_LEVELS","ES date_LEVELS","Inv_Qty","Special_Programs","Qty","IR","Big_Deal_Qty","Non_Big_Deal_Qty","Brand","Consol SKU","Full Name","VPA","Promo_Flag","Promo_Pct","Discount_Depth_Category","log_Qty","price","Inv_Qty_log","USChristmasDay","USColumbusDay","USIndependenceDay","USLaborDay","USLincolnsBirthday","USMemorialDay","USMLKingsBirthday","USNewYearsDay","USPresidentsDay","USVeteransDay","USWashingtonsBirthday","USThanksgivingDay","USCyberMonday","L1_competition_Brother","L1_competition_Canon","L1_competition_Epson","L1_competition_Lexmark","L1_competition_Samsung","L2_competition_Brother","L2_competition_Canon","L2_competition_Epson","L2_competition_Lexmark","L2_competition_Samsung","L1_competition","L2_competition","L1_competition_HP_ssmodel","L2_competition_HP_ssmodel","L1_cannibalization","L2_cannibalization","Sale_Price","seasonality_npd","seasonality_npd2","Hardware_GM","Supplies_GM","Hardware_Rev","Supplies_Rev","Changed_Street_Price","Valid_Start_Date","Valid_End_Date","Hardware_GM_type","Hardware_Rev_type","Supplies_GM_type","Supplies_Rev_type","avg_discount_SKU_Account","supplies_GM_scaling_factor","Supplies_GM_unscaled","Supplies_GM_no_promo","Supplies_Rev_unscaled","Supplies_Rev_no_promo","L1_cannibalization_log","L2_cannibalization_log","L1_competition_log","L2_competition_log","Big_Deal","Big_Deal_Qty_log","outlier","spike","spike2","no_promo_avg","no_promo_med","low_baseline","low_volume","raw_bl_avg","raw_bl_med","EOL","BOL","opposite","no_promo_sales","NP_Flag","NP_IR","high_disc_Flag","always_promo_Flag","cann_group","cann_receiver","Direct_Cann_201","Direct_Cann_225","Direct_Cann_252","Direct_Cann_277","exclude","AE_NP_IR","AE_ASP_IR","AE_Other_IR","Street_PriceWhoChange_log","SKUWhoChange","PriceChange_HPS_OPS","Season_most_recent")
      .withColumnRenamed("Reseller_Cluster_LEVELS","Reseller_Cluster")
      .withColumnRenamed("Street Price","Street_Price")
      .withColumnRenamed("IPSLES_LEVELS","IPSLES")
      .withColumnRenamed("HPS_OPS_LEVELS","HPS_OPS")
      .withColumnRenamed("Category Subgroup","Category_Subgroup")
      .withColumnRenamed("Category_1_LEVELS","Category_1")
      .withColumnRenamed("Category_2_LEVELS","Category_2")
      .withColumnRenamed("Category_3_LEVELS","Category_3")
      .withColumnRenamed("Category Custom","Category_Custom")
      .withColumnRenamed("Line_LEVELS","Line")
      .withColumnRenamed("PLC Status","PLC_Status")
      .withColumnRenamed("GA date_LEVELS","GA_date")
      .withColumnRenamed("ES date_LEVELS","ES_date")
      .withColumnRenamed("Consol SKU","Consol_SKU")
      .withColumnRenamed("Full Name","Full_Name")

    val format = new SimpleDateFormat("d-M-y_h-m-s")
    import java.util.Calendar
    //commercialWithCompCannDF.write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/Pricing/Output/Preregression_Commercial/regression_data_commercial_Jan27.csv")
    /*+format.format(Calendar.getInstance().getTime().toString)+*/
    commercialWithCompCannDF.write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/Pricing/Output/Preregression_Commercial/regression_data_commercial.csv")

  }
}