package com.scienaptic.jobs.core.pricing.commercial

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.CommercialUtility.createlist
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

object CommercialFeatEnggProcessor8 {
  val Cat_switch=1
  val min_baseline = 2
  val stability_weeks = 4
  val intro_weeks = 6

  val dat2000_01_01 = to_date(unix_timestamp(lit("2000-01-01"),"yyyy-MM-dd").cast("timestamp"))
  val dat9999_12_31 = to_date(unix_timestamp(lit("9999-12-31"),"yyyy-MM-dd").cast("timestamp"))

  def execute(executionContext: ExecutionContext): Unit = {

    val sparkConf = new SparkConf().setAppName("Test")
    val spark = SparkSession.builder
      .master("yarn-client")
      //.master("local[*]")
      .appName("Commercial-R-8")
      .config(sparkConf)
      .getOrCreate

    var commercial = spark.read.option("header","true").option("inferSchema","true").csv("E:\\Scienaptic\\HP\\Pricing\\Data\\CR1\\May31_Run\\spark_output\\commercialBeforeBOL.csv")
      .withColumn("ES date", to_date(col("ES date")))
      .withColumn("Week_End_Date", to_date(col("Week_End_Date")))
      .withColumn("Valid_Start_Date", to_date(col("Valid_Start_Date")))
      .withColumn("Valid_End_Date", to_date(col("Valid_End_Date")))
      .withColumn("GA date", to_date(unix_timestamp(col("GA date"),"yyyy-MM-dd").cast("timestamp")))
      .persist(StorageLevel.MEMORY_AND_DISK).cache()
    
    var BOL = commercial.select("SKU","ES date","GA date")
      .dropDuplicates().cache()

    BOL = BOL.where((col("ES date").isNotNull) || (col("GA date").isNotNull))

    BOL = BOL
      .withColumn("ES date_wday", dayofweek(col("ES date")).cast("int"))  //As dayofweek returns in range 1-7 we want 0-6
      .withColumn("GA date_wday", dayofweek(col("GA date")).cast("int"))
      .withColumn("ES date_wday_sub", lit(7)-col("ES date_wday"))
      .withColumn("GA date_wday_sub", lit(7)-col("GA date_wday"))
      .withColumn("ES date_wday_sub", when(col("ES date_wday_sub").isNull,0).otherwise(col("ES date_wday_sub")))
      .withColumn("GA date_wday_sub", when(col("GA date_wday_sub").isNull,0).otherwise(col("GA date_wday_sub")))

    BOL = BOL
      .withColumnRenamed("GA date","GA_date").withColumnRenamed("ES date","ES_date")
      .withColumnRenamed("GA date_wday_sub","GA_date_wday_sub").withColumnRenamed("ES date_wday_sub","ES_date_wday_sub")
      .withColumn("GA_date", expr("date_add(GA_date,GA_date_wday_sub)"))
      .withColumn("ES_date", expr("date_add(ES_date,ES_date_wday_sub)"))
      .withColumnRenamed("GA_date","GA date").withColumnRenamed("ES_date","ES date")
      .withColumnRenamed("GA_date_wday_sub","GA date_wday_sub").withColumnRenamed("ES_date_wday_sub","ES date_wday_sub")
      .drop("GA date_wday","ES date_wday","GA date_wday_sub","ES date_wday_sub")

    val windForSKUnReseller = Window.partitionBy("SKU$Reseller").orderBy(/*"SKU","Reseller_Cluster","Reseller_Cluster_LEVELS",*/"Week_End_Date")
    //TODO: Too heavy from here till max, min, last date joins
    var BOLCriterion = commercial
      .groupBy("SKU","Reseller_Cluster","Week_End_Date")
      .agg(sum("Qty").as("Qty"))
      .sort("SKU","Reseller_Cluster","Week_End_Date")
      .withColumn("SKU$Reseller", concat(col("SKU"),col("Reseller_Cluster")))
      .withColumn("rank", row_number().over(windForSKUnReseller))
      .withColumn("BOL_criterion", when(col("rank")<intro_weeks, 0).otherwise(1))
      .drop("rank","Qty").cache()

    BOLCriterion = BOLCriterion
      .join(BOL.select("SKU","GA date"), Seq("SKU"), "left")
    val minWEDDate = to_date(unix_timestamp(lit(BOLCriterion.agg(min("Week_End_Date")).head().getDate(0)),"yyyy-MM-dd").cast("timestamp"))
    BOLCriterion = BOLCriterion.withColumn("GA date", when(col("GA date").isNull, minWEDDate).otherwise(col("GA date")))
      .where(col("Week_End_Date")>=col("GA date"))

    val BOLCriterionFirst = BOLCriterion.where(col("BOL_criterion")===1)
      .groupBy("SKU","Reseller_Cluster")
      .agg(min("Week_End_Date").as("first_date"))

    val BOLCriterionMax = commercial
      .groupBy("SKU","Reseller_Cluster")
      .agg(max("Week_End_Date").as("max_date"))

    val BOLCriterionMin = commercial//.where(col("BOL_criterion")===1)
      .groupBy("SKU","Reseller_Cluster")
      .agg(min("Week_End_Date").as("min_date"))

    BOLCriterion =  BOLCriterionMax.withColumn("SKU",col("SKU")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      .join(BOLCriterionFirst.withColumn("SKU",col("SKU")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("SKU","Reseller_Cluster"), "left")
      .join(BOLCriterionMin.withColumn("SKU",col("SKU")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("SKU","Reseller_Cluster"), "left")
        .persist(StorageLevel.MEMORY_AND_DISK)

    BOLCriterion = BOLCriterion.withColumn("first_date", when(col("first_date").isNull, col("max_date")).otherwise(col("first_date")))
      .drop("max_date")

    val minMinDateBOL = BOLCriterion.agg(min("min_date")).head().getDate(0)
    BOLCriterion = BOLCriterion
      .where(!((col("min_date")===col("first_date")) && (col("first_date")===minMinDateBOL)))
      .withColumn("diff_weeks", ((datediff(to_date(col("first_date")),to_date(col("min_date"))))/7)+1)

    BOLCriterion = BOLCriterion.withColumn("diff_weeks", when(col("diff_weeks").isNull || col("diff_weeks")<=0, 0).otherwise(col("diff_weeks")))
    BOLCriterion = BOLCriterion.withColumn("diff_weeks", col("diff_weeks").cast("int"))

    BOLCriterion = BOLCriterion
      .withColumn("repList", createlist(col("diff_weeks").cast("int")))
      .withColumn("add", explode(col("repList"))).drop("repList")
      .withColumn("add", col("add"))

    BOLCriterion = BOLCriterion
      .withColumn("add", col("add")*lit(7))
      .withColumn("Week_End_Date", expr("date_add(min_date,add)"))

    BOLCriterion = BOLCriterion.drop("min_date","fist_date","diff_weeks","add")
      .withColumn("BOL_criterion", lit(1))

    commercial = commercial
      .withColumn("SKU",col("SKU")).withColumn("Reseller_Cluster",col("Reseller_Cluster")).withColumn("Week_End_Date",col("Week_End_Date"))
      .join(BOLCriterion.withColumn("SKU",col("SKU")).withColumn("Reseller_Cluster",col("Reseller_Cluster")).withColumn("Week_End_Date",col("Week_End_Date")), Seq("SKU","Reseller_Cluster","Week_End_Date"), "left")

    commercial = commercial
      .withColumn("BOL", when(col("EOL")===1,0).otherwise(col("BOL_criterion")))
      .withColumn("BOL", when(datediff(col("Week_End_Date"),col("GA date"))<(7*6),1).otherwise(col("BOL")))
      .withColumn("BOL", when(col("GA date").isNull, 0).otherwise(col("BOL"))) //Important: Dont comment this!
      .withColumn("BOL", when(col("BOL").isNull, 0).otherwise(col("BOL"))).cache()

    commercial.write.option("header","true").mode(SaveMode.Overwrite).csv("E:\\Scienaptic\\HP\\Pricing\\Data\\CR1\\May31_Run\\spark_output\\commercialBeforeOpposite.csv")
  }

}