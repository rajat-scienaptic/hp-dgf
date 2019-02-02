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

    var commercialWithCompCannDF = spark.read.option("header","true").option("inferSchema","true").csv("/etherData/comercialTemp/CommercialFeatEngg/commercialWithCompCannDF.csv")


    val wind = Window.partitionBy("SKU_Name","Reseller_Cluster").orderBy("Qty")
    //df2 = sqlContext.sql("select agent_id, percentile_approx(payment_amount,0.95) as approxQuantile from df group by agent_id")
    commercialWithCompCannDF.createOrReplaceTempView("commercial")

    val percentil75DF = spark.sql("select SKU_Name, Reseller_Cluster, percentile_approx(Qty,0.75) as percentile_0_75 from commercial group by SKU_Name, Reseller_Cluster")
    commercialWithCompCannDF = commercialWithCompCannDF.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
    .join(percentil75DF.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("SKU_Name", "Reseller_Cluster"), "left")
    commercialWithCompCannDF.createOrReplaceTempView("commercial")
    val percentile25DF = spark.sql("select SKU_Name, Reseller_Cluster, percentile_approx(Qty,0.25) as percentile_0_25 from commercial group by SKU_Name, Reseller_Cluster")
    commercialWithCompCannDF = commercialWithCompCannDF.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
    .join(percentile25DF.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("SKU_Name", "Reseller_Cluster"), "left")

    commercialWithCompCannDF = commercialWithCompCannDF.withColumn("IQR", col("percentile_0_75")-col("percentile_0_25"))
        .withColumn("outlier", when(col("Qty")>col("percentile_0_75"), (col("Qty")-col("percentile_0_75"))/col("IQR")).otherwise(when(col("Qty")<col("percentile_0_25"), (col("Qty")-col("percentile_0_25"))/col("IQR")).otherwise(lit(0))))
        .withColumn("spike", when(abs(col("outlier"))<=8, 0).otherwise(1))
        .withColumn("spike", when((col("SKU_Name")==="OJ Pro 8610") && (col("Reseller_Cluster")==="Other - Option B") && (col("Week_End_Date")==="2014-11-01"),1).otherwise(col("spike")))
        .withColumn("spike", when((col("SKU").isin("F6W14A")) && (col("Week_End_Date")==="10/7/2017") && (col("Reseller_Cluster").isin("Other - Option B")), 1).otherwise(col("spike")))
        .withColumn("spike2", when((col("spike")===1) && (col("IR")>0), 0).otherwise(col("spike")))
        .drop("percentile_0_75", "percentile_0_25","IQR")
        .withColumn("Qty", col("Qty").cast("int"))
    commercialWithCompCannDF.write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/comercialTemp/CommercialFeatEngg/commercialWithCompCannDFAFTERIQR.csv")
    
    val commercialWithHolidayAndQtyFilter = commercialWithCompCannDF.where((col("Promo_Flag")===0) && (col("USThanksgivingDay")===0) && (col("USCyberMonday")===0) && (col("spike")===0))
      .where(col("Qty")>0)
    var npbl = commercialWithHolidayAndQtyFilter
      .groupBy("Reseller_Cluster","SKU_Name")
      .agg(mean("Qty").as("no_promo_avg"), stddev("Qty").as("no_promo_sd"), min("Qty").as("no_promo_min"), max("Qty").as("no_promo_max"))
    npbl = commercialWithHolidayAndQtyFilter.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
    .join(npbl.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("Reseller_Cluster","SKU_Name"), "left")
    npbl.createOrReplaceTempView("npbl")
    val npblTemp = spark.sql("select Reseller_Cluster, SKU_Name, percentile_approx(Qty, 0.5) as no_promo_med from npbl group by Reseller_Cluster,SKU_Name")
    npbl = npbl.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
    .join(npblTemp.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("Reseller_Cluster","SKU_Name"), "left")

    commercialWithCompCannDF = commercialWithCompCannDF.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
    .join(npbl.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster")).select("SKU_Name", "Reseller_Cluster","no_promo_avg", "no_promo_med"), Seq("SKU_Name","Reseller_Cluster"), "left")
        .withColumn("no_promo_avg", when(col("no_promo_avg").isNull, 0).otherwise(col("no_promo_avg")))
        .withColumn("no_promo_med", when(col("no_promo_med").isNull, 0).otherwise(col("no_promo_med")))
        .withColumn("low_baseline", when(((col("no_promo_avg")>=min_baseline) && (col("no_promo_med")>baselineThreshold)) || ((col("no_promo_med")>=min_baseline) && (col("no_promo_avg")>=baselineThreshold)),0).otherwise(1))
        .withColumn("low_volume", when(col("Qty")>0,0).otherwise(1))
        .withColumn("raw_bl_avg", col("no_promo_avg")*(col("seasonality_npd")+lit(1)))
        .withColumn("raw_bl_med", col("no_promo_med")*(col("seasonality_npd")+lit(1)))



    /* EOL_criterion_commercial <- function (x) {
       #Argument comes as A:B
      Qty <- as.numeric(do.call(rbind, strsplit(x, split=";"))[,1])   #Extracts A
      baseline <- as.numeric(do.call(rbind, strsplit(x, split=";"))[,2])    #Extracts B
      temp <- NULL
      d <- NULL
      for (i in length(Qty):1){
        if (Qty[i]<baseline[i] | i<=stability_weeks){
          temp[i] <- 0
        }else{
          for (j in 1:stability_weeks) {
            d[j] <- Qty[i-j]>baseline[i]}
          if (length(d[d==TRUE])>=1){
            temp[i] <- 1
          }else{
            temp[i] <-0
          }
        }
      }
      temp
    }
    * */

    val windForSKUAndReseller = Window.partitionBy("SKU&Reseller").orderBy("SKU_Name","Reseller_Cluster","Week_End_Date")

    var EOLcriterion = commercialWithCompCannDF
      .groupBy("SKU_Name","Reseller_Cluster","Week_End_Date")
      .agg(sum("Qty").as("Qty"), sum("no_promo_med").as("no_promo_med"))
      //.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      //.withColumn("Week_End_Date",col("Week_End_Date"))
      .sort("SKU_Name","Reseller_Cluster","Week_End_Date")
      .withColumn("Qty&no_promo_med", concat_ws(";",col("Qty"), col("no_promo_med")))
      .withColumn("SKU&Reseller", concat(col("SKU_Name"), col("Reseller_Cluster")))

      //.join(commercialWithCompCannDF.withColumnRenamed("Qty","Qty_BKP").withColumnRenamed("no_promo_med","no_promo_med_BKP").withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      //  .withColumn("Week_End_Date",col("Week_End_Date"))
      //  , Seq("SKU_Name","Reseller_Cluster","Week_End_Date"), "right")

      //EOL_criterion$EOL_criterion <- ave(Qty&no_promo_med, SKU&Reseller, EOL_criterion_commercial)

    //var EOLcriterion = spark.read.option("header","true").option("inferSchema","true").csv("C:\\Users\\avika\\Downloads\\outputs\\EOLcriterion.csv")
    //var commercialWithCompCannDF = spark.read.option("header","true").option("inferSchema","true").csv("C:\\Users\\avika\\Downloads\\outputs\\commercialWithCompCannDF.csv")

    EOLcriterion = EOLcriterion
        .withColumn("rank", dense_rank().over(windForSKUAndReseller))
        .withColumn("EOL_criterion", when(col("rank")<=stability_weeks || col("Qty")<col("no_promo_med"), 0).otherwise(1))

    var EOLWithCriterion1 = EOLcriterion.orderBy("SKU_Name","Reseller_Cluster","Week_End_Date")
      .groupBy("SKU&Reseller").agg(collect_list(struct(col("rank"),col("Qty"))).as("QtyArray"))
    EOLWithCriterion1 = EOLWithCriterion1
      .withColumn("QtyArray", when(col("QtyArray").isNull, null).otherwise(concatenateRank(col("QtyArray"))))

    EOLcriterion = EOLcriterion.join(EOLWithCriterion1, Seq("SKU&Reseller"), "left")
      .withColumn("EOL_criterion", checkPrevQtsGTBaseline(col("QtyArray"), col("rank"), col("no_promo_med")))
        .drop("rank","QtyArray","SKU&Reseller","Qty&no_promo_med")

    //TODO: DONE TILL HERE!!!
    val EOLCriterionLast = EOLcriterion.where(col("EOL_criterion")===1)
      .groupBy("SKU_Name","Reseller_Cluster")
      .agg(max("Week_End_Date").as("last_date"))
      //.join(EOLcriterion.where(col("EOL_criterion")===1), Seq("SKU_Name","Reseller_Cluster"), "right")

    val EOLCriterionMax = commercialWithCompCannDF
      .groupBy("SKU_Name","Reseller_Cluster")
      .agg(max("Week_End_Date").as("max_date"))
      //.join(commercialWithCompCannDF, Seq("SKU_Name","Reseller_Cluster"), "right")

    EOLcriterion = EOLCriterionMax.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      .join(EOLCriterionLast.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("SKU_Name","Reseller_Cluster"), "left")
        .where(col("last_date").isNotNull)

    val maxMaxDate = EOLcriterion.agg(max("max_date")).head().getTimestamp(0)
    EOLcriterion = EOLcriterion
        .where(!((col("max_date")===col("last_date")) && (col("last_date")===maxMaxDate)))
        .drop("max_date")

    //commercialWithCompCannDF.printSchema()
    //EOLcriterion.printSchema()

    EOLcriterion.write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/comercialTemp/EOLcriterionAFTER_MAX_MIN.csv")
    commercialWithCompCannDF.write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/comercialTemp/CommercialFeatEngg/commercialWithCompCannDFAFTER_EOL_CALC.csv")

    //var EOLcriterion = spark.read.option("header","true").option("inferSchema","true").csv("C:\\Users\\avika\\Downloads\\outputs\\EOLcriterionNew.csv")
    //var commercialWithCompCannDF = spark.read.option("header","true").option("inferSchema","true").csv("C:\\Users\\avika\\Downloads\\outputs\\commercialWithCompCannDFNew.csv")

    commercialWithCompCannDF = commercialWithCompCannDF.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      .join(EOLcriterion.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("SKU_Name","Reseller_Cluster"), "left")
        .withColumn("EOL", when(col("last_date").isNull, 0).otherwise(when(col("Week_End_Date")>col("last_date"),1).otherwise(0)))
        .drop("last_date")
        .withColumn("EOL", when(col("SKU").isin("G3Q47A","M9L75A","F8B04A","B5L24A","L2719A","D3Q19A","F2A70A","CF377A","L2747A","F0V69A","G3Q35A","C5F93A","CZ271A","CF379A","B5L25A","D3Q15A","B5L26A","L2741A","CF378A","L2749A","CF394A"),0).otherwise(col("EOL")))
        .withColumn("EOL", when((col("SKU")==="C5F94A") && (col("Season")=!="STS'17"), 0).otherwise(col("EOL")))

    var BOL = commercialWithCompCannDF.select("SKU","ES date_LEVELS","GA date_LEVELS")  //TODO: Remove '_LEVELS' and renaming below
      .withColumnRenamed("ES date_LEVELS","ES date").withColumnRenamed("GA date_LEVELS","GA date")
      .dropDuplicates()
      .where((col("ES date").isNotNull) || (col("GA date").isNotNull))
      .withColumn("ES date", to_date(unix_timestamp(col("ES date"),"yyyy-MM-dd").cast("timestamp")))
      .withColumn("GA date", to_date(unix_timestamp(col("GA date"),"yyyy-MM-dd").cast("timestamp")))
      .withColumn("ES date_wday", dayofweek(col("ES date")).cast("int")-1)  //As dayofweek returns in range 1-7 we want 0-6
      .withColumn("GA date_wday", dayofweek(col("GA date")).cast("int")-1)
      .withColumn("GA date", addDaystoDateStringUDF(col("GA date").cast("timestamp").cast("string"), lit(7)-col("GA date_wday")))
      .withColumn("ES date", addDaystoDateStringUDF(col("ES date").cast("timestamp").cast("string"), lit(7)-col("ES date_wday")))
      .drop("GA date_wday","ES date_wday")

    /*
    * BOL_criterion_v3 <- function (x) {
      temp <- NULL
      for (i in 1:length(x)){
        if (i < intro_weeks){
          temp[i] <- 0
        }else{
          temp[i] <- 1
        }
      }
      temp
    }
    * */
    val windForSKUnReseller = Window.partitionBy("SKU$Reseller").orderBy("SKU","Reseller_Cluster","Week_End_Date")
    var BOLCriterion = commercialWithCompCannDF
      .groupBy("SKU","Reseller_Cluster","Week_End_Date")
      .agg(sum("Qty").as("Qty"))
      .sort("SKU","Reseller_Cluster","Week_End_Date")
      .withColumn("SKU$Reseller", concat(col("SKU"),col("Reseller_Cluster")))
      .withColumn("rank", dense_rank().over(windForSKUnReseller))
      .withColumn("BOL_criterion", when(col("rank")<intro_weeks, 0).otherwise(1)) /*BOL_criterion$BOL_criterion <- ave(BOL_criterion$Qty, paste0(BOL_criterion$SKU, BOL_criterion$Reseller.Cluster), FUN = BOL_criterion_v3)*/
      .drop("rank")
      .drop("Qty")

    BOLCriterion = BOLCriterion.join(BOL.select("SKU","GA date"), Seq("SKU"), "left")
    val minWEDDate = to_date(unix_timestamp(lit(BOLCriterion.agg(min("Week_End_Date")).head().getTimestamp(0)),"yyyy-MM-dd").cast("timestamp"))

    BOLCriterion = BOLCriterion.withColumn("GA date", when(col("GA date").isNull, minWEDDate).otherwise(col("GA date")))
        .where(col("Week_End_Date")>=col("GA date"))

    val BOLCriterionFirst = BOLCriterion.where(col("BOL_criterion")===1)
      .groupBy("SKU","Reseller_Cluster")
      .agg(min("Week_End_Date").as("first_date"))

    val BOLCriterionMax = commercialWithCompCannDF
      .groupBy("SKU","Reseller_Cluster")
      .agg(min("Week_End_Date").as("max_date"))

    val BOLCriterionMin = BOLCriterion.where(col("BOL_criterion")===1)
      .groupBy("SKU","Reseller_Cluster")
      .agg(min("Week_End_Date").as("min_date"))

    BOLCriterion =  BOLCriterionMax.withColumn("SKU",col("SKU")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
      .join(BOLCriterionFirst.withColumn("SKU",col("SKU")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("SKU","Reseller_Cluster"), "left")
        .join(BOLCriterionMin.withColumn("SKU",col("SKU")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("SKU","Reseller_Cluster"), "left")

    BOLCriterion = BOLCriterion.withColumn("first_date", when(col("first_date").isNull, col("max_date")).otherwise(col("first_date")))
        .drop("max_date")
    val minMinDateBOL = BOLCriterion.agg(min("min_date")).head().getTimestamp(0)
    //BOLCriterion.printSchema()
    BOLCriterion = BOLCriterion
        .where(!((col("min_date")===col("first_date")) && (col("first_date")===minMinDateBOL)))
        .withColumn("diff_weeks", ((datediff(to_date(col("first_date")),to_date(col("min_date"))))/7)+1)
        //.withColumn("diff_weeks", ((col("first_date")-col("min_date"))/7)+1)

    /*BOL_criterion <- BOL_criterion[rep(row.names(BOL_criterion), BOL_criterion$diff_weeks),]
    * BOL_criterion$add <- t(as.data.frame(strsplit(row.names(BOL_criterion), "\\.")))[,2]
      BOL_criterion$add <- ifelse(grepl("\\.",row.names(BOL_criterion))==FALSE,0,as.numeric(BOL_criterion$add))
      BOL_criterion$add <- BOL_criterion$add*7
      BOL_criterion$Week.End.Date <- BOL_criterion$min_date+BOL_criterion$add
    * */
    BOLCriterion = BOLCriterion.withColumn("diff_weeks", when(col("diff_weeks").isNull || col("diff_weeks")<=0, 0).otherwise(col("diff_weeks")))
    BOLCriterion = BOLCriterion.withColumn("diff_weeks", col("diff_weeks").cast("int"))
    BOLCriterion = BOLCriterion
      .withColumn("repList", createlist(col("diff_weeks").cast("int"))).withColumn("add", explode(col("repList"))).drop("repList")
      .withColumn("add", col("add")+lit(1))
        .withColumn("add", col("add")*lit(7))
        .withColumn("Week_End_Date", addDaystoDateStringUDF(col("min_date"), col("add")))   //CHECK: check if min_date is in timestamp format!

    BOLCriterion = BOLCriterion.drop("min_date","fist_date","diff_weeks","add")
        .withColumn("BOL_criterion", lit(1))
    
    BOLCriterion.write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/comercialTemp/BOLCriterionCalc.csv")
    commercialWithCompCannDF.write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/comercialTemp/commercialWithCompCannDFAFTER_BOL_Calc.csv")
    
    commercialWithCompCannDF = commercialWithCompCannDF.drop("GA date").withColumn("GA date",col("GA date_LEVELS"))//TODO Remove this
    .withColumn("SKU",col("SKU")).withColumn("Reseller_Cluster",col("Reseller_Cluster")).withColumn("Week_End_Date",col("Week_End_Date"))
      .join(BOLCriterion.withColumn("SKU",col("SKU")).withColumn("Reseller_Cluster",col("Reseller_Cluster")).withColumn("Week_End_Date",col("Week_End_Date")), Seq("SKU","Reseller_Cluster","Week_End_Date"), "left")
        .withColumn("BOL_criterion", when(col("BOL_criterion").isNull, 0).otherwise(col("BOL_criterion")))
        .withColumn("BOL", when(col("EOL")===1,0).otherwise(col("BOL_criterion")))
        .withColumn("BOL", when(datediff(col("Week_End_Date"),col("GA date"))<(7*6),1).otherwise(col("BOL")))
        .withColumn("BOL", when(col("BOL").isNull, 0).otherwise(col("BOL")))

    val commercialEOLSpikeFilter = commercialWithCompCannDF.where((col("EOL")===0) && (col("spike")===0))
    val opposite = commercialEOLSpikeFilter
      .groupBy("SKU_Name","Reseller_Cluster")
      .agg(count("SKU_Name").as("n"),mean("Qty").as("Qty_total"), mean(when(col("Promo_Flag")===1,"Qty")).as("Qty_promo"), mean(when(col("Promo_Flag")===0, "Qty")).as("Qty_no_promo"))
      .withColumn("opposite", when((col("Qty_no_promo")>col("Qty_promo")) || (col("Qty_no_promo")<0), 1).otherwise(0))
      .withColumn("opposite", when(col("opposite").isNull, 0).otherwise(col("opposite")))
      .withColumn("no_promo_sales", when(col("Qty_promo").isNull, 1).otherwise(0))

    commercialWithCompCannDF = commercialWithCompCannDF.withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster"))
    .join(opposite.select("SKU_Name", "Reseller_Cluster", "opposite","no_promo_sales").withColumn("SKU_Name",col("SKU_Name")).withColumn("Reseller_Cluster",col("Reseller_Cluster")), Seq("SKU_Name","Reseller_Cluster"), "left")
        .withColumn("NP_Flag", col("Promo_Flag"))
        .withColumn("NP_IR", col("IR"))
        .withColumn("high_disc_Flag", when(col("Promo_Pct")<=0.55, 0).otherwise(1))
    commercialWithCompCannDF.write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/comercialTemp/commercialWithCompCannDFAFTER_OPPOSITE_Calc.csv")
   
    //var commercialWithCompCannDF = spark.read.option("header","true").option("inferSchema","true").csv("/etherData/comercialTemp/commercialWithCompCannDFAFTER_OPPOSITE_Calc.csv")
    //CHECK - ave(commercial$Promo.Flag, commercial$Reseller.Cluster, commercial$SKU.Name, commercial$Season, FUN=mean
    val commercialPromoMean = commercialWithCompCannDF
      .groupBy("Reseller_Cluster","SKU_Name","Season")
      .agg(mean(col("Promo_Flag")).as("PromoFlagAvg"))

    //commercialWithCompCannDF.write.option("header","true").mode(SaveMode.Overwrite).csv("C:\\Users\\avika\\Downloads\\outputs\\commercialWithCompCannDFBeforeWhenOther.csv")

    commercialWithCompCannDF = commercialWithCompCannDF.join(commercialPromoMean, Seq("Reseller_Cluster","SKU_Name","Season"), "left")
        .withColumn("always_promo_Flag", when(col("PromoFlagAvg")===1, 1).otherwise(0)).drop("PromoFlagAvg")
        .withColumn("EOL", when(col("Reseller_Cluster")==="CDW",
          when(col("SKU_Name")==="LJ Pro M402dn", 0).otherwise(col("EOL"))).otherwise(col("EOL")))
        .withColumn("cann_group", lit(null).cast("string"))
        .withColumn("cann_group", when(col("SKU_Name").contains("M20") || col("SKU_Name").contains("M40"),"M201_M203/M402").otherwise(col("cann_group")))
        .withColumn("cann_group", when(col("SKU_Name").contains("M22") || col("SKU_Name").contains("M42"),"M225_M227/M426").otherwise(col("cann_group")))
        .withColumn("cann_group", when(col("SKU_Name").contains("M25") || col("SKU_Name").contains("M45"),"M252_M254/M452").otherwise(col("cann_group")))
        .withColumn("cann_group", when(col("SKU_Name").contains("M27") || col("SKU_Name").contains("M28"),"M277_M281/M477").otherwise(col("cann_group")))
        .withColumn("cann_group", when(col("SKU_Name").contains("8710") || col("SKU_Name").contains("8720") || col("SKU_Name").contains("6968") || col("SKU_Name").contains("6978"),"Weber/Muscatel").otherwise(col("cann_group")))
        .withColumn("cann_group", when(col("SKU_Name").contains("6255") || col("SKU_Name").contains("7155") || col("SKU_Name").contains("7855") || col("SKU_Name").contains("6988") || col("SKU_Name").contains("6978"),"Palermo/Muscatel").otherwise(col("cann_group")))
        .withColumn("cann_receiver", lit(null).cast("string"))
        .withColumn("cann_receiver", when(col("SKU_Name").contains("M40"), "M402").otherwise(col("cann_receiver")))
        .withColumn("cann_receiver", when(col("SKU_Name").contains("M42"), "M426").otherwise(col("cann_receiver")))
        .withColumn("cann_receiver", when(col("SKU_Name").contains("M45"), "M452").otherwise(col("cann_receiver")))
        .withColumn("cann_receiver", when(col("SKU_Name").contains("M47"), "M477").otherwise(col("cann_receiver")))
        .withColumn("cann_receiver", when(col("SKU_Name").contains("8710") || col("SKU_Name").contains("8720"), "Weber").otherwise(col("cann_receiver")))
        .withColumn("cann_receiver", when(col("SKU_Name").contains("6968") || col("SKU_Name").contains("6978"), "Muscatel").otherwise(col("cann_receiver")))
        .withColumn("cann_receiver", when(col("SKU_Name").contains("6255") || col("SKU_Name").contains("7155") || col("SKU_Name").contains("7855"), "Palermo").otherwise(col("cann_receiver")))
    commercialWithCompCannDF.write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/comercialTemp/CommercialFeatEngg/commercialWithCompCannDF_AFTER_CANN_RECEIVER_CALC.csv")
          //TILL HERE
    //commercialWithCompCannDF.write.option("header","true").mode(SaveMode.Overwrite).csv("C:\\Users\\avika\\Downloads\\outputs\\commercialWithCompCannDFBeforeHeads.csv")
    //Confirm where these columns originate from in R code
    val commercialM40MeanIR = commercialWithCompCannDF.where(col("SKU_Name").contains("M40")).agg(mean("IR")).head().getDouble(0)
    val commercialM42MeanIR = commercialWithCompCannDF.where(col("SKU_Name").contains("M42")).agg(mean("IR")).head().getDouble(0)
    val commercialM45MeanIR = commercialWithCompCannDF.where(col("SKU_Name").contains("M45")).agg(mean("IR")).head().getDouble(0)
    val commercialM47MeanIR = commercialWithCompCannDF.where(col("SKU_Name").contains("M47")).agg(mean("IR")).head().getDouble(0)
    val commercialMuscatelMeanIR = commercialWithCompCannDF.where(col("cann_receiver")==="Muscatel").agg(mean("IR")).head().getDouble(0)
    val commercialWeberMeanIR = commercialWithCompCannDF.where(col("cann_receiver")==="Weber").agg(mean("IR")).head().getDouble(0)
    val commercialPalermoMeanIR = commercialWithCompCannDF.where(col("cann_receiver")==="Palermo").agg(mean("IR")).head().getDouble(0)
    val commercialMuscatel2MeanIR = commercialWithCompCannDF.where(col("cann_receiver")==="Muscatel").agg(mean("IR")).head().getDouble(0)

    commercialWithCompCannDF = commercialWithCompCannDF
      .withColumn("Direct_Cann_201", lit(null).cast("string"))
      .withColumn("Direct_Cann_225", lit(null).cast("string"))
      .withColumn("Direct_Cann_252", lit(null).cast("string"))
      .withColumn("Direct_Cann_277", lit(null).cast("string"))
      .withColumn("Direct_Cann_Weber", lit(null).cast("string"))
      .withColumn("Direct_Cann_Muscatel_Weber", lit(null).cast("string"))
      .withColumn("Direct_Cann_Muscatel_Palermo", lit(null).cast("string"))
      .withColumn("Direct_Cann_Palermo", lit(null).cast("string"))
      .withColumn("Direct_Cann_201", when(col("SKU_Name").contains("M201") || col("SKU_Name").contains("M203"), commercialM40MeanIR).otherwise(0))
      .withColumn("Direct_Cann_225", when(col("SKU_Name").contains("M225") || col("SKU_Name").contains("M227"), commercialM42MeanIR).otherwise(0))
      .withColumn("Direct_Cann_252", when(col("SKU_Name").contains("M252") || col("SKU_Name").contains("M254"), commercialM45MeanIR).otherwise(0))
      .withColumn("Direct_Cann_277", when(col("SKU_Name").contains("M277") || col("SKU_Name").contains("M281"), commercialM47MeanIR).otherwise(0))
      .withColumn("Direct_Cann_Weber", when(col("cann_receiver")==="Weber", commercialMuscatelMeanIR).otherwise(0))
      .withColumn("Direct_Cann_Muscatel_Weber", when(col("cann_receiver")==="Muscatel", commercialWeberMeanIR).otherwise(0))
      .withColumn("Direct_Cann_Muscatel_Palermo", when(col("cann_receiver")==="Muscatel", commercialPalermoMeanIR).otherwise(0))
      .withColumn("Direct_Cann_Palermo", when(col("cann_receiver")==="Palermo", commercialMuscatel2MeanIR).otherwise(0))
      .withColumn("Direct_Cann_201",when(col("Direct_Cann_201").isNull, 0).otherwise(col("Direct_Cann_201")))
      .withColumn("Direct_Cann_225",when(col("Direct_Cann_225").isNull, 0).otherwise(col("Direct_Cann_225")))
      .withColumn("Direct_Cann_252",when(col("Direct_Cann_252").isNull, 0).otherwise(col("Direct_Cann_252")))
      .withColumn("Direct_Cann_277",when(col("Direct_Cann_277").isNull, 0).otherwise(col("Direct_Cann_277")))
      .withColumn("Direct_Cann_Weber",when(col("Direct_Cann_Weber").isNull, 0).otherwise(col("Direct_Cann_Weber")))
      .withColumn("Direct_Cann_Muscatel_Weber",when(col("Direct_Cann_Muscatel_Weber").isNull, 0).otherwise(col("Direct_Cann_Muscatel_Weber")))
      .withColumn("Direct_Cann_Muscatel_Palermo", when(col("Direct_Cann_Muscatel_Palermo").isNull, 0).otherwise(col("Direct_Cann_Muscatel_Palermo")))
      .withColumn("Direct_Cann_Palermo", when(col("Direct_Cann_Palermo").isNull, 0).otherwise(col("Direct_Cann_Palermo")))
      .withColumn("Hardware_GM", when(col("Category_1")==="Value" && col("Week_End_Date")>="2016-07-01", col("Hardware_GM")+lit(68)).otherwise(col("Hardware_GM")))
      .withColumn("Hardware_GM", when(col("Category_1")==="Value" && col("Week_End_Date")>="2017-05-01", col("Hardware_GM")+lit(8)).otherwise(col("Hardware_GM")))
      .withColumn("Hardware_GM", when(col("Category Custom")==="A4 SMB" && col("Week_End_Date")>="2017-11-01", col("Hardware_GM")-lit(7.51)).otherwise(col("Hardware_GM")))
      .withColumn("Hardware_GM", when(col("Category Custom").isin("A4 Value","A3 Value") && col("Week_End_Date")>="2017-11-01", col("Hardware_GM")+lit(33.28)).otherwise(col("Hardware_GM")))
      .withColumn("Supplies_GM", when(col("L1_Category")==="Scanners",0).otherwise(col("Supplies_GM")))


    commercialWithCompCannDF = commercialWithCompCannDF
      .withColumn("exclude", when(!col("PL").isin("3Y"),when(col("Reseller_Cluster").isin("Other - Option C", "eTailerOther - Option C", "Stockpiler & Deal Chaser", "eTailerStockpiler & Deal Chaser") || col("low_volume")===1 || col("low_baseline")===1 || col("spike")===1 || col("opposite")===1 || col("EOL")===1 || col("BOL")===1 || col("no_promo_sales")===1,1).otherwise(0)).otherwise(0))
      .withColumn("exclude", when(!col("PL").isin("3Y"),when(col("Reseller_Cluster").isin("Other - Option C", "eTailerOther - Option C", "Stockpiler & Deal Chaser", "eTailerStockpiler & Deal Chaser") || col("low_volume")===1 || /*col("low_baseline")===1 ||*/ col("spike")===1 || col("opposite")===1 || col("EOL")===1 || col("BOL")===1 || col("no_promo_sales")===1,1).otherwise(0)).otherwise(0))
      .withColumn("exclude", when(col("SKU_Name").contains("Sprocket"), 1).otherwise(col("exclude")))
      .withColumn("AE_NP_IR", col("NP_IR"))
      .withColumn("AE_ASP_IR", lit(0))
      .withColumn("AE_Other_IR", lit(0))
      .withColumn("Street_PriceWhoChange_log", when(col("Changed_Street_Price")===0, 0).otherwise(log(col("Street Price")*col("Changed_Street_Price"))))
      .withColumn("SKUWhoChange", when(col("Changed_Street_Price")===0, 0).otherwise(col("SKU")))
      .withColumn("PriceChange_HPS_OPS", when(col("Changed_Street_Price")===0, 0).otherwise(col("HPS_OPS")))


    /*
    * if (length(unique(commercial$Week.End.Date[commercial$Season==unique(commercial$Season[order(commercial$Week.End.Date)])[length(unique(commercial$Season))]]))<13){
        commercial$Season_most_recent <- ifelse(commercial$Season==unique(commercial$Season
        [order(commercial$Week.End.Date)])[length(unique(commercial$Season))], as.character(unique(commercial$Season[order
        (commercial$Week.End.Date)])[length(unique(commercial$Season))-1]), as.character(commercial$Season))
      } else {
        commercial$Season_most_recent <- commercial$Season
      }
    * */
    val maxWED = commercialWithCompCannDF.agg(max("Week_End_Date")).head().getTimestamp(0)
    val maxWEDSeason = commercialWithCompCannDF.where(col("Week_End_Date")===maxWED).sort(col("Week_End_Date").desc).select("Season").head().getString(0)
    val latestSeasonCommercial = commercialWithCompCannDF.where(col("Season")===maxWEDSeason)

    val windForSeason = Window.orderBy(col("Week_End_Date").desc)
    val uniqueSeason = commercialWithCompCannDF.withColumn("rank", dense_rank().over(windForSeason))
      .where(col("rank")===2).select("Season").head().getString(0)


    if (latestSeasonCommercial.select("Week_End_Date").distinct().count()<13) {
      commercialWithCompCannDF = commercialWithCompCannDF
        .withColumn("Season_most_recent", when(col("Season")===maxWEDSeason,uniqueSeason).otherwise(col("Season")))
    }else {
      commercialWithCompCannDF = commercialWithCompCannDF
        .withColumn("Season_most_recent", col("Season"))
    }
    commercialWithCompCannDF = commercialWithCompCannDF.withColumn("GA date",col("GA date").cast("string"))
    .withColumn("ES date",col("ES date").cast("string"))
    List("cann_group","SKUWhoChange","ES date","GA date").foreach(x => {
      commercialWithCompCannDF = commercialWithCompCannDF.withColumn(x+"_LEVELS", col(x))
      val indexer = new StringIndexer().setInputCol(x).setOutputCol(x+"_fact").setHandleInvalid("skip")
      val pipeline = new Pipeline().setStages(Array(indexer))
      commercialWithCompCannDF = pipeline.fit(commercialWithCompCannDF).transform(commercialWithCompCannDF)
        .drop(x).withColumnRenamed(x+"_fact",x).drop(x+"_fact")
    })

    val format = new SimpleDateFormat("d-M-y_h-m-s")
    import java.util.Calendar;

    commercialWithCompCannDF.write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/Pricing/Output/Preregression_Commercial/regression_data_commercial_Jan27.csv")
    //commercialWithCompCannDF.write.option("header","true").mode(SaveMode.Overwrite).csv("/etherData/Pricing/Output/Preregression_Commercial/regression_data_commercial"+format.format(Calendar.getInstance().getTime().toString.replace(" ","%20"))+".csv")
    


  }
}