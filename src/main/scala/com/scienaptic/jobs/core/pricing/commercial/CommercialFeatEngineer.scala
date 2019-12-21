package com.scienaptic.jobs.core.pricing.commercial

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.bean.UnionOperation.doUnion
import com.scienaptic.jobs.utility.Utils.renameColumns
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

object CommercialFeatEnggProcessor {
  val Cat_switch=1
  val min_baseline = 2
  val stability_weeks = 4
  val intro_weeks = 6

  val dat2000_01_01 = to_date(unix_timestamp(lit("2000-01-01"),"yyyy-MM-dd").cast("timestamp"))
  val dat9999_12_31 = to_date(unix_timestamp(lit("9999-12-31"),"yyyy-MM-dd").cast("timestamp"))

  def execute(executionContext: ExecutionContext): Unit = {

    val sparkConf = new SparkConf().setAppName("Test")
    val spark = SparkSession.builder
      //.master("local[*]")
      .master("yarn-client")
      .appName("Commercial-R")
      .config(sparkConf)
      .getOrCreate

    //val baselineThreshold = if (min_baseline/2 > 0) min_baseline/2 else 0

    import spark.implicits._
    val maxRegressionDate = renameColumns(spark.read.option("header",true).option("inferSchema",true).csv("/etherData/managedSources/NPD/NPD_weekly.csv"))
      .select("Week_End_Date").withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"),"MM/dd/yyyy").cast("timestamp")))
      .withColumn("Week_End_Date", date_sub(col("Week_End_Date"), 7)).agg(max("Week_End_Date")).head().getDate(0)

    val currentTS = executionContext.spark.read.json("/etherData/state/currentTS.json").select("ts").head().getString(0)
    //val commercialDF = spark.read.option("header","true").option("inferSchema","true").csv("E:\\Scienaptic\\HP\\Pricing\\Data\\CR1\\May31_Run\\outputs\\posqty_output_commercial.csv")
    var commercialDF = spark.read.option("header","true").option("inferSchema","true").csv("/etherData/Pricing/Outputs/POS_Commercial/posqty_commercial_output_"+currentTS+".csv")
      //.repartition(500).persist(StorageLevel.MEMORY_AND_DISK)
    var commercial = renameColumns(commercialDF)
    commercial.columns.toList.foreach(x => {
      commercial = commercial.withColumn(x, when(col(x).cast("string") === "NA" || col(x).cast("string") === "", null).otherwise(col(x)))
    })
    commercial = commercial
      //.withColumn("Week_End_Date", to_date(unix_timestamp(col("Week_End_Date"),"yyyy-MM-dd").cast("timestamp")))
      //TODO For production keep this
      .withColumn("Week_End_Date", to_date(col("Week_End_Date")))
      .where(col("Week_End_Date") >= lit("2014-01-01"))
      //AVIK Change: Making max pre-regression dynamic based on NPD
      .where(col("Week_End_Date") <= lit(maxRegressionDate))
      //.withColumn("ES date",to_date(unix_timestamp(col("ES date"),"yyyy-MM-dd").cast("timestamp")))
      //.withColumn("GA date",to_date(unix_timestamp(col("GA date"),"yyyy-MM-dd").cast("timestamp")))
      //TODO: For production keep this
      .withColumn("ES date",to_date(col("ES date")))
      .withColumn("GA date",to_date(col("GA date")))

    //val ifs2DF = spark.read.option("header","true").option("inferSchema","true").csv("/etherData/managedSources/IFS2/IFS2_most_recent.csv")
    val ifs2DF = spark.read.option("header","true").option("inferSchema","true").csv("/etherData/managedSources/IFS2/IFS2_most_recent.csv")
    var ifs2 = renameColumns(ifs2DF)
    ifs2.columns.toList.foreach(x => {
      ifs2 = ifs2.withColumn(x, when(col(x) === "NA" || col(x) === "", null).otherwise(col(x)))
    })
    ifs2 = ifs2
      .withColumn("Valid_Start_Date", to_date(unix_timestamp(col("Valid_Start_Date"),"MM/dd/yyyy").cast("timestamp")))
      .withColumn("Valid_End_Date", to_date(unix_timestamp(col("Valid_End_Date"),"MM/dd/yyyy").cast("timestamp")))
    ifs2.write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/commercialTemp/CommercialFeatEngg/ifs2.csv")

    commercial = commercial
      .withColumnRenamed("Reseller Cluster","Reseller_Cluster")
      .withColumnRenamed("Street Price","Street_Price_Org")
      .join(ifs2.dropDuplicates("SKU","Street_Price").select("SKU","Changed_Street_Price","Street_Price", "Valid_Start_Date", "Valid_End_Date"), Seq("SKU"), "left")
      .withColumn("Valid_Start_Date", when(col("Valid_Start_Date").isNull,dat2000_01_01).otherwise(col("Valid_Start_Date")))
      .withColumn("Valid_End_Date", when(col("Valid_End_Date").isNull,dat9999_12_31).otherwise(col("Valid_End_Date")))
      .withColumn("Street_Price", when(col("Street_Price").isNull, col("Street_Price_Org")).otherwise(col("Street_Price")))
      .where((col("Week_End_Date") >= col("Valid_Start_Date")) && (col("Week_End_Date") <= col("Valid_End_Date")))    // CR1 - Filter relaxed.
      .drop("Street_Price_Org","Changed_Street_Price","Valid_Start_Date","Valid_End_Date")
      .persist(StorageLevel.MEMORY_AND_DISK)

    
    commercial = commercial.withColumn("Qty",col("Non_Big_Deal_Qty"))
      .withColumn("Special_Programs",lit("None"))
    var stockpiler_dealchaser = commercial
      .groupBy("Reseller_Cluster")
      .agg(sum("Qty").alias("Qty_total"))
    val stockpiler_dealchaserPromo = commercial.where(col("IR")>0)
      .groupBy("Reseller_Cluster")
      .agg(sum("Qty").as("Qty_promo"))
    stockpiler_dealchaser = stockpiler_dealchaser.join(stockpiler_dealchaserPromo, Seq("Reseller_Cluster"), "left")
      .withColumn("Qty_promo", when(col("Qty_promo").isNull, 0).otherwise(col("Qty_promo")))

    stockpiler_dealchaser = stockpiler_dealchaser
      .withColumn("proportion_on_promo", col("Qty_promo")/col("Qty_total"))
      .withColumn("proportion_on_promo", when(col("proportion_on_promo").isNull, 0).otherwise(col("proportion_on_promo")))
      .where(col("proportion_on_promo")>0.95)

    //TODO: Broadcast this value
    val stockpilerResellerList = stockpiler_dealchaser.select("Reseller_Cluster").distinct().collect().map(_ (0).asInstanceOf[String]).toSet
    commercial = commercial
      .withColumn("Reseller_Cluster", when(col("Reseller_Cluster").isin(stockpilerResellerList.toSeq: _*), lit("Stockpiler & Deal Chaser")).otherwise(col("Reseller_Cluster")))
      .withColumn("Reseller_Cluster", when(col("eTailer")===lit(1), concat(lit("eTailer"),col("Reseller_Cluster"))).otherwise(col("Reseller_Cluster")))
        .drop("eTailer")

    val commercialResFacNotPL = commercial.where(!col("PL").isin("E4","E0","ED"))
      .withColumn("Brand",lit("HP"))
        .withColumn("GA date",to_date(col("GA date")))
        .withColumn("ES date",to_date(col("ES date"))).cache()

    var commercialHPDF = commercialResFacNotPL
      .groupBy("SKU","SKU_Name","Reseller_Cluster","Week_End_Date","Season","Street_Price","IPSLES","HPS/OPS","Series","Category","Category Subgroup","Category_1","Category_2","Category_3","Category Custom","Line","PL","L1_Category","L2_Category","PLC Status","GA date","ES date","Inv_Qty","Special_Programs")
      .agg(sum("Qty").as("Qty"), max("IR").as("IR"), sum("Big_Deal_Qty").as("Big_Deal_Qty"), sum("Non_Big_Deal_Qty").as("Non_Big_Deal_Qty"))
      .withColumnRenamed("HPS/OPS","HPS_OPS")


    val commercialResFacPL = commercial.where(col("PL").isin("E4","E0","ED"))

    val commercialSSDF = commercialResFacPL
      .groupBy("SKU","SKU_Name","Reseller_Cluster","Week_End_Date","Season","Special_Programs")
      .agg(sum("Qty").as("Qty"), sum("Inv_Qty").as("Inv_Qty"), max("IR").as("IR"), sum("Big_Deal_Qty").as("Big_Deal_Qty"), sum("Non_Big_Deal_Qty").as("Non_Big_Deal_Qty"))



    val auxTableDF = spark.read.option("header","true").option("inferSchema","true").csv("/etherData/managedSources/AUX/Aux_sku_hierarchy.csv")
    var auxTable = renameColumns(auxTableDF).cache()
    auxTable.columns.toList.foreach(x => {
      auxTable = auxTable.withColumn(x, when(col(x).cast("string") === "NA" || col(x).cast("string") === "", null).otherwise(col(x)))
    })
    auxTable = auxTable
      .withColumn("GA date",to_date(col("GA date")))
      .withColumn("ES date",to_date(col("ES date")))
    auxTable.persist(StorageLevel.MEMORY_AND_DISK)
    val commercialSSJoinSKUDF = commercialSSDF.join(auxTable, Seq("SKU"), "left")
      .withColumnRenamed("HPS/OPS","HPS_OPS")
      .withColumn("L1_Category",col("L1: Use Case"))
      .withColumn("L2_Category",col("L2: Key functionality"))
      .drop("L1: Use Case","L2: Key functionality","Abbreviated_Name","Platform_Name","Mono/Color","Added","L3: IDC Category","L0: Format","Need_Big_Data","Need_IFS2?","Top_SKU","NPD_Model","NPD_Product_Company","Sales_Product_Name")

    var commercialSSFactorsDF = commercialSSJoinSKUDF
        .drop("L3: IDC Category","L0: Format","Need Big Data","Need IFS2?","Top SKU","NPD Model","NPD_Product_Company","Sales_Product_Name")
    commercialHPDF = commercialHPDF.withColumn("Brand",lit("HP"))
    commercialHPDF.cache()

    commercial = doUnion(commercialSSFactorsDF, commercialHPDF.withColumnRenamed("Street_Price","Street Price")).get
      .withColumn("ES date", to_date(unix_timestamp(col("ES date"),"yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA date", to_date(unix_timestamp(col("GA date"),"yyyy-MM-dd").cast("timestamp")))
      .withColumn("Street Price", (regexp_replace(col("Street Price"),lit("\\$"),lit(""))))
      .withColumn("Street Price", (regexp_replace(col("Street Price"),lit(","),lit(""))).cast("double"))
      .where(col("Street Price") =!= lit(0))

      commercial = commercial
      .withColumn("VPA", when(col("Reseller_Cluster").isin("CDW","PC Connection","Zones Inc","Insight Direct","PCM","GovConnection"), 1).otherwise(0))
      .withColumn("Promo_Flag", when(col("IR")>0,1).otherwise(0))
      .withColumn("Promo_Pct", when(col("Street Price").isNull,null).otherwise(col("IR")/col("Street Price").cast("double")))
      .withColumn("Discount_Depth_Category", when(col("Promo_Pct")===0, "No Discount").when(col("Promo_Pct")<=0.2, "Very Low").when(col("Promo_Pct")<=0.3, "Low").when(col("Promo_Pct")===0.4, "Moderate").when(col("Promo_Pct")<="0.5", "Heavy").otherwise(lit("Very Heavy")))
      .withColumn("log_Qty", log(col("Qty")))
      .withColumn("log_Qty", when(col("log_Qty").isNull, 0).otherwise(col("log_Qty")))
      .withColumn("price", log(lit(1)-col("Promo_Pct")))
      .withColumn("price", when(col("price").isNull, 0).otherwise(col("price")))
      .withColumn("Inv_Qty_log", log(col("Inv_Qty")))
      .withColumn("Inv_Qty_log", when(col("Inv_Qty_log").isNull, 0).otherwise(col("Inv_Qty_log")))
      .na.fill(0, Seq("log_Qty","price","Inv_Qty_log"))

    /*  CR1 - Fixed Cost feature addition - Start  */
    /*Avik Aprl13 Change: 20190320 fixed cost udpated-update Scienaptic - Start*/
    val IFS2FC = ifs2.where(col("Account")==="Commercial").groupBy("Account","SKU")
      .agg(mean(col("Fixed_Cost")).as("Fixed_Cost")).drop("Account").distinct()
    commercial = commercial.join(IFS2FC, Seq("SKU"), "left")
    /*Avik Aprl13 Change: 20190320 fixed cost udpated-update Scienaptic - End*/
    /*  CR1 - Fixed Cost feature addition - End  */

    val christmasDF = Seq(("2014-12-27",1),("2015-12-26",1),("2016-12-31",1),("2017-12-30",1),("2018-12-29",1),("2019-12-28",1)).toDF("Week_End_Date","USChristmasDay")
    val columbusDF = Seq(("2014-10-18",1),("2015-10-17",1),("2016-10-15",1),("2017-10-14",1),("2018-10-13",1),("2019-10-19",1)).toDF("Week_End_Date","USColumbusDay")
    val independenceDF = Seq(("2014-07-05",1),("2015-07-04",1),("2016-07-09",1),("2017-07-08",1),("2018-07-07",1),("2019-07-06",1)).toDF("Week_End_Date","USIndependenceDay")
    val laborDF = Seq(("2014-09-06",1),("2015-09-12",1),("2016-09-10",1),("2017-09-09",1),("2018-09-08",1),("2019-09-07",1)).toDF("Week_End_Date","USLaborDay")
    val linconsBdyDF = Seq(("2014-02-15",1),("2015-02-14",1),("2016-02-13",1),("2017-02-18",1),("2018-02-17",1),("2019-02-16",1)).toDF("Week_End_Date","USLincolnsBirthday")
    val memorialDF = Seq(("2014-05-31",1),("2015-05-30",1),("2016-06-04",1),("2017-06-03",1),("2018-06-02",1),("2019-06-01",1)).toDF("Week_End_Date","USMemorialDay")
    val MLKingsDF = Seq(("2014-01-25",1),("2015-01-24",1),("2016-01-23",1),("2017-01-21",1),("2018-01-20",1),("2019-01-26",1)).toDF("Week_End_Date","USMLKingsBirthday")
    val newYearDF = Seq(("2014-01-04",1),("2015-01-03",1),("2016-01-02",1),("2017-01-07",1),("2018-01-06",1),("2019-01-05",1)).toDF("Week_End_Date","USNewYearsDay")
    val presidentsDayDF = Seq(("2014-02-22",1),("2015-02-21",1),("2016-02-20",1),("2017-02-25",1),("2018-02-24",1),("2019-02-23",1)).toDF("Week_End_Date","USPresidentsDay")
    val veteransDayDF = Seq(("2014-11-15",1),("2015-11-14",1),("2016-11-12",1),("2017-11-11",1),("2018-11-17",1),("2019-11-16",1)).toDF("Week_End_Date","USVeteransDay")
    val washingtonBdyDF = Seq(("2014-02-22",1),("2015-02-28",1),("2016-02-27",1),("2017-02-25",1),("2018-02-24",1),("2019-02-23",1)).toDF("Week_End_Date","USWashingtonsBirthday")
    val thanksgngDF = Seq(("2014-11-29",1),("2015-11-28",1),("2016-11-26",1),("2017-11-25",1),("2018-11-24",1),("2019-11-30",1)).toDF("Week_End_Date","USThanksgivingDay")
    val usCyberMonday = thanksgngDF.withColumn("Week_End_Date", date_add(col("Week_End_Date").cast("timestamp"), 7)).withColumnRenamed("USThanksgivingDay","USCyberMonday")
    commercial = commercial
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
      List("USChristmasDay","USColumbusDay","USIndependenceDay","USLaborDay","USLincolnsBirthday","USMemorialDay","USMLKingsBirthday","USNewYearsDay","USPresidentsDay","USThanksgivingDay","USVeteransDay","USWashingtonsBirthday","USCyberMonday")
      .foreach(x=> {
          commercial = commercial.withColumn(x, when(col(x).isNull, 0).otherwise(col(x)))
      })
      commercial.persist(StorageLevel.MEMORY_AND_DISK)
    commercial.write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/commercialTemp/CommercialFeatEngg/commercialBeforeNPD.csv")
    commercial.write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/commercialTemp/CommercialFeatEngg/commercialWithCompCannDF.csv")
    //commercial.write.option("header","true").mode(SaveMode.Overwrite).csv("E:\\Scienaptic\\HP\\Pricing\\Data\\CR1\\May31_Run\\spark_output\\commercialBeforeNPD.csv")
    //commercial.write.option("header","true").mode(SaveMode.Overwrite).csv("E:\\Scienaptic\\HP\\Pricing\\Data\\CR1\\May31_Run\\spark_output\\commercialWithCompCannDF.csv")

  }
}