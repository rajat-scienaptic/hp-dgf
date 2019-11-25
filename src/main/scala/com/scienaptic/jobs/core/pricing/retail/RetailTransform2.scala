package com.scienaptic.jobs.core.pricing.retail

import java.text.SimpleDateFormat

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.bean._
import com.scienaptic.jobs.utility.Utils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, SaveMode}

object RetailTransform2 {

  val INNER_JOIN = "inner"
  val LEFT_JOIN = "leftanti"
  val RIGHT_JOIN = "rightanti"
  val SELECT01 = "select01"
  val SELECT02 = "select02"
  val SELECT03 = "select03"
  val SELECT04 = "select04"
  val SELECT05 = "select05"
  val SELECT06 = "select06"
  val FILTER01 = "filter01"
  val FILTER02 = "filter02"
  val FILTER03 = "filter03"
  val FILTER04 = "filter04"
  val FILTER05 = "filter05"
  val FILTER06 = "filter06"
  val JOIN01 = "join01"
  val JOIN02 = "join02"
  val JOIN03 = "join03"
  val JOIN04 = "join04"
  val JOIN05 = "join05"
  val JOIN06 = "join06"
  val JOIN07 = "join07"
  val NUMERAL0 = 0
  val NUMERAL1 = 1
  val GROUP01 = "group01"
  val GROUP02 = "group02"
  val GROUP03 = "group03"
  val GROUP04 = "group04"
  val GROUP05 = "group05"
  val RENAME01 = "rename01"
  val RENAME02 = "rename02"
  val RENAME03 = "rename03"
  val RENAME04 = "rename04"
  val RENAME05 = "rename05"
  val RENAME06 = "rename06"
  val SORT01 = "sort01"
  val SORT02 = "sort02"
  val SORT03 = "sort03"
  val SORT04 = "sort04"


  val ODOM_ONLINE_ORCA_SOURCE = "ODOM_ONLINE_ORCA"
  val STAPLES_COM_UNITS_SOURCE = "STAPLES_COM_UNITS"
  val HP_COM_SOURCE = "HP_COM"
  val AMAZON_ARAP_SOURCE = "AMAZON_ARAP"
  val AMAZON_ASIN_MAP_SOURCE = "AMAZON_ASIN_MAP"
  val S_PRINT_HISTORICAL_UNITS_SOURCE = "S_PRINT_HISTORICAL_UNITS"
  val ORCA_2014_16_ARCHIVE_SOURCE = "ORCA_2014_16_ARCHIVE"
  val ORCA_QRY_2017_TO_DATE_SOURCE = "ORCA_QRY_2017_TO_DATE"
  val AUX_TABLES_WEEKEND_SOURCE = "AUX_TABLES_WEEKEND"
  val AUX_TABLES_ONLINE_SOURCE = "AUX_TABLES_ONLINE"
  val AUX_TABLES_SKU_HIERARCHY_SOURCE = "AUX_TABLES_SKU_HIERARCHY"
  val BBY_BUNDLE_INFO_SOURCE = "BBY_BUNDLE_INFO"
  val EXISTING_POS_SOURCE = "EXISTING_POS"
  val AUX_TABLE_ONLINE_PATH = "/etherData/retailTemp/retailAlteryx/auxTablesOnlineFormula01DF.csv"
  val MAIN_UNION_PATH = "/etherData/retailTemp/retailAlteryx/mainUnion05Union04AndSPrintFormula.csv"


  def execute(executionContext: ExecutionContext): Unit = {
    /* Source Map with all sources' information */
    val sourceMap = executionContext.configuration.sources

    val currentTS = executionContext.spark.read.json("/etherData/state/currentTS.json").select("ts").head().getString(0)

    /* Map with all operations source operations */
    val auxTablesOnlineSource = sourceMap(AUX_TABLES_ONLINE_SOURCE)
    val auxTablesSKUHierarchySource = sourceMap(AUX_TABLES_SKU_HIERARCHY_SOURCE)
    val bbyBundleInfoSource = sourceMap(BBY_BUNDLE_INFO_SOURCE)
//    val existingPOSSource = sourceMap(EXISTING_POS_SOURCE)
    val auxTablesSKUHierarchy = Utils.loadCSV(executionContext, auxTablesSKUHierarchySource.filePath).get
      .withColumn("GA date", to_date(col("GA date"), "yyyy-MM-dd"))
      .withColumn("ES date", to_date(col("ES date"), "yyyy-MM-dd"))
    val bbyBundleInfo = Utils.loadCSV(executionContext, bbyBundleInfoSource.filePath).get
//    val existingPOS = Utils.loadCSV(executionContext, existingPOSSource.filePath).get
    val auxTablesOnlineFormula01DF = Utils.loadCSV(executionContext, AUX_TABLE_ONLINE_PATH).get
      .withColumn("Max_wed", to_date(unix_timestamp(col("Max_wed"), "yyyy-MM-dd").cast("timestamp")))
      .withColumn("wed", to_date(unix_timestamp(col("wed"), "yyyy-MM-dd").cast("timestamp")))
    val mainUnion05Union04AndSPrintFormula = Utils.loadCSV(executionContext, MAIN_UNION_PATH).get
      .withColumn("wed", to_date(unix_timestamp(col("wed"), "yyyy-MM-dd").cast("timestamp")))


    // formula
    val auxTablesOnlineFormula02DF = auxTablesOnlineFormula01DF.withColumn("Product Base ID",
      when(col("Product Base ID") === "M9L74A", "M9L75A")
        .when((col("Product Base ID") === "J9V91A") || (col("Product Base ID") === "J9V92A"), "J9V90A")
            .when(col("Product Base ID") === "Z3M52A", "K7G93A")
        .when(col("Product Base ID") === "5LJ23A" || (col("Product Base ID") === "4KJ65A"), "3UC66A")
        .when(col("Product Base ID") === "3UK84A", "1KR45A")
        .when(col("Product Base ID") === "T0G26A", "T0G25A")
        .otherwise(col("Product Base ID")))

    // group
    val auxTablesOnlineGroup02 = auxTablesOnlineSource.groupOperation(GROUP02)
    val auxTablesOnlineGroup02DF = GroupOperation.doGroup(auxTablesOnlineFormula02DF, auxTablesOnlineGroup02).get

    // filter
    val maxWedIncluding7000DaysDF = auxTablesOnlineGroup02DF.withColumn("date_last_52weeks", date_sub(col("Max_wed"), 7000))
    val auxTablesOnlineWedGreaterThan7000Filter02 = auxTablesOnlineSource.filterOperation(FILTER02)
//    val auxTablesOnlineWedGreaterThan7000Filter02DF = FilterOperation.doFilter(maxWedIncluding7000DaysDF, auxTablesOnlineWedGreaterThan7000Filter02, auxTablesOnlineWedGreaterThan7000Filter02.conditionTypes(NUMERAL0)).get
    val auxTablesOnlineWedGreaterThan7000Filter02DF = maxWedIncluding7000DaysDF.filter(col("wed") > col("date_last_52weeks"))

    // distribution calculation starts
    // filter
    val auxTablesOnlineIfOnline1Filter = auxTablesOnlineSource.filterOperation(FILTER03)
    val auxTablesOnlineIfOnline1FilterDF = FilterOperation.doFilter(auxTablesOnlineWedGreaterThan7000Filter02DF, auxTablesOnlineIfOnline1Filter, auxTablesOnlineIfOnline1Filter.conditionTypes(NUMERAL0)).get.cache()

    // group
    val auxTablesOnlineGroup03 = auxTablesOnlineSource.groupOperation(GROUP03)
    val auxTablesOnlineGroup03DF = GroupOperation.doGroup(auxTablesOnlineIfOnline1FilterDF, auxTablesOnlineGroup03).get

    // group
    val auxTablesOnlineGroup04 = auxTablesOnlineSource.groupOperation(GROUP04)
    val auxTablesOnlineGroup04DF = GroupOperation.doGroup(auxTablesOnlineGroup03DF, auxTablesOnlineGroup04).get

    // formula
    val auxTablesOnlineFormula03DF = auxTablesOnlineGroup03DF.withColumn("Sum_POS Sales NDP",
      when(col("Sum_POS Sales NDP") < 0, 0)
        .otherwise(col("Sum_POS Sales NDP")))

    // join
    val auxTablesOnlineJoin02 = auxTablesOnlineSource.joinOperation(JOIN02)
    val auxTablesOnlineFormula03ColsRenamedDF = Utils.convertListToDFColumnWithRename(auxTablesOnlineSource.renameOperation(RENAME02), auxTablesOnlineFormula03DF)
    val auxTablesOnlineJoin02Map = JoinAndSelectOperation.doJoinAndSelect(auxTablesOnlineIfOnline1FilterDF, auxTablesOnlineFormula03ColsRenamedDF, auxTablesOnlineJoin02)
    val auxTablesOnlineJoin02InnerDF = auxTablesOnlineJoin02Map(INNER_JOIN)

    // formula
    val auxTablesOnlineFormula04DF = auxTablesOnlineJoin02InnerDF.withColumn("Store POS",
      when(col("POS Qty") > 0, col("Sum_POS Sales NDP"))
        .otherwise(0))
      .withColumn("Store Inv",
        when(col("Inventory Total Qty") > 0, col("Sum_POS Sales NDP"))
          .otherwise(0))

    // group
    val auxTablesOnlineGroup05 = auxTablesOnlineSource.groupOperation(GROUP05)
    val auxTablesOnlineGroup05DF = GroupOperation.doGroup(auxTablesOnlineFormula04DF, auxTablesOnlineGroup05).get

    // join
    val auxTablesOnlineJoin03 = auxTablesOnlineSource.joinOperation(JOIN03)
    val auxTablesOnlineGroup04RenamedDF = Utils.convertListToDFColumnWithRename(auxTablesOnlineSource.renameOperation(RENAME03), auxTablesOnlineGroup04DF)
    val auxTablesOnlineJoin03Map = JoinAndSelectOperation.doJoinAndSelect(auxTablesOnlineGroup05DF, auxTablesOnlineGroup04RenamedDF, auxTablesOnlineJoin03)
    val auxTablesOnlineJoin03InnerDF = auxTablesOnlineJoin03Map(INNER_JOIN)


    // formula
    val auxTablesOnlineFormula05DF = auxTablesOnlineJoin03InnerDF.withColumn("SKU ACV POS",
        when(col("Sum_Sum_POS Sales NDP") < 1, 0)
      .when((col("Sum_Store POS") / col("Sum_Sum_POS Sales NDP")) > 1, 1)
      .otherwise(col("Sum_Store POS") / col("Sum_Sum_POS Sales NDP")))
      .withColumn("SKU ACV Inv", when(col("Sum_Sum_POS Sales NDP") < 1, 0)
      .when((col("Sum_Store Inv") / col("Sum_Sum_POS Sales NDP")) > 1, 1)
      .otherwise(col("Sum_Store Inv") / col("Sum_Sum_POS Sales NDP")))

    // select with rename
    val auxTablesOnlineSelect02 = auxTablesOnlineSource.selectOperation(SELECT02)
    val auxTablesOnlineSelect02DF = Utils.convertListToDFColumnWithRename(auxTablesOnlineSource.renameOperation(RENAME01),
      SelectOperation.doSelect(auxTablesOnlineFormula05DF, auxTablesOnlineSelect02.cols, auxTablesOnlineSelect02.isUnknown).get)

    // browse here
    // distribution calculation Ends

    /* Aux Table SKU Hierarchy */
    //select
    val auxTablesSKUHierarchySelect01 = auxTablesSKUHierarchySource.selectOperation(SELECT01)
    val auxTablesSKUHierarchySelect01DF = SelectOperation.doSelect(auxTablesSKUHierarchy, auxTablesSKUHierarchySelect01.cols, auxTablesSKUHierarchySelect01.isUnknown).get

    // join
    val auxTablesSKUHierarchyJoin01 = auxTablesSKUHierarchySource.joinOperation(JOIN01)
    val auxTablesSKUHierarchyJoin01Map = JoinAndSelectOperation.doJoinAndSelect(mainUnion05Union04AndSPrintFormula, auxTablesSKUHierarchySelect01DF, auxTablesSKUHierarchyJoin01)
    val auxTablesSKUHierarchyJoin01LeftDF = auxTablesSKUHierarchyJoin01Map(LEFT_JOIN)
    val auxTablesSKUHierarchyJoin01InnerDF = auxTablesSKUHierarchyJoin01Map(INNER_JOIN)

    // group
    val auxTablesSKUHierarchyGroup01 = auxTablesSKUHierarchySource.groupOperation(GROUP01)
    val auxTablesSKUHierarchyGroup01DF = GroupOperation.doGroup(auxTablesSKUHierarchyJoin01LeftDF, auxTablesSKUHierarchyGroup01).get

    // sort
    val auxTablesSKUHierarchySort01 = auxTablesSKUHierarchySource.sortOperation(SORT01)
    val auxTablesSKUHierarchySort01DF = SortOperation.doSort(auxTablesSKUHierarchyGroup01DF, auxTablesSKUHierarchySort01.ascending, auxTablesSKUHierarchySort01.descending).get

    // SKU fallout
    auxTablesSKUHierarchySort01DF.write.option("header", true).mode(SaveMode.Overwrite).csv("/etherData/retailTemp/retailAlteryx/fallout-SKUs-retail-" + currentTS + ".csv")

    // browse or write to CSV

    // group
    val auxTablesSKUHierarchyGroup02 = auxTablesSKUHierarchySource.groupOperation(GROUP02)
    val auxTablesSKUHierarchyGroup02DF = Utils.convertListToDFColumnWithRename(auxTablesSKUHierarchySource.renameOperation(RENAME01), GroupOperation.doGroup(auxTablesSKUHierarchyJoin01InnerDF, auxTablesSKUHierarchyGroup02).get).cache()

    // formula
    val auxTablesSKUHierarchyFormula01 = auxTablesSKUHierarchyGroup02DF.withColumn("Raw POS Qty", col("POS Qty"))

    // unique
    val auxTablesSKUHierarchyDistinctDF = auxTablesSKUHierarchyGroup02DF.dropDuplicates(List("Account Major", "Online", "SKU", "WED"))
    // SKU fallout
//    auxTablesSKUHierarchyDistinctDF.write.option("header", true).mode(SaveMode.Overwrite).csv("/etherData/retailTemp/retailAlteryx/fallout-SKUs-retail-" + currentTS + ".csv")
    // browse here

    // filter
    val auxTablesSKUHierarchyFilter01 = auxTablesSKUHierarchySource.filterOperation(FILTER01)
    val auxTablesSKUHierarchyFilter01DF = FilterOperation.doFilter(auxTablesSKUHierarchyGroup02DF, auxTablesSKUHierarchyFilter01, auxTablesSKUHierarchyFilter01.conditionTypes(NUMERAL0)).get

    /* BBY Bundle Info */
    implicit val bBYBundleEncoder = Encoders.product[BBYBundle]
    implicit val bBYBundleTranspose = Encoders.product[BBYBundleTranspose]

    val bbyDataSet = bbyBundleInfo.as[BBYBundle]
    val names = bbyDataSet.schema.fieldNames

    val transposedData = bbyDataSet.flatMap(row => Array(BBYBundleTranspose( row.`Week Ending`,row.Units, row.`HP SKU`, names(3), row.`B&M Units`),
      BBYBundleTranspose(row.`Week Ending`,row.Units, row.`HP SKU`, names(4), row.`_COM Units`)))

    val bbyBundleInfoTransposeDF = transposedData.toDF()
      .withColumn("Week Ending", to_date(col("Week Ending"), "yyyy-MM-dd"))

    // select
    val bbyBundleInfoSelect01 = bbyBundleInfoSource.selectOperation(SELECT01)
    val bbyBundleInfoSelect01DF = SelectOperation.doSelect(bbyBundleInfoTransposeDF, bbyBundleInfoSelect01.cols, bbyBundleInfoSelect01.isUnknown).get

    // formula
    val bbyBundleInfoFormula01DF = bbyBundleInfoSelect01DF.withColumn("Online",
      when(col("Online") === "B&M Units", 0)
        .otherwise(1))

    // group
    val bbyBundleInfoGroup01 = bbyBundleInfoSource.groupOperation(GROUP01)
    val bbyBundleInfoGroup01DF = GroupOperation.doGroup(bbyBundleInfoFormula01DF, bbyBundleInfoGroup01).get

    // join
    val bbyBundleInfoJoin01 = bbyBundleInfoSource.joinOperation(JOIN01)
    val bbyBundleInfoJoin01Map = JoinAndSelectOperation.doJoinAndSelect(auxTablesSKUHierarchyFilter01DF, bbyBundleInfoGroup01DF.withColumnRenamed("Online", "Right_Online"), bbyBundleInfoJoin01)
    val bbyBundleInfoJoin01InnerDF = bbyBundleInfoJoin01Map(INNER_JOIN)

    // unique
    val bbyBundleInfoDistinctDF = bbyBundleInfoJoin01InnerDF.dropDuplicates(List("Online", "SKU", "WED"))

    // browse here

    // group
    val bbyBundleInfoGroup02 = bbyBundleInfoSource.groupOperation(GROUP02)
    val bbyBundleInfoGroup02DF = GroupOperation.doGroup(bbyBundleInfoJoin01InnerDF, bbyBundleInfoGroup02).get

    // join
    val bbyBundleInfoJoin02 = bbyBundleInfoSource.joinOperation(JOIN02)
    val bbyBundleInfoJoin01InnerRenamedDF = Utils.convertListToDFColumnWithRename(bbyBundleInfoSource.renameOperation(RENAME03), bbyBundleInfoJoin01InnerDF)
    val bbyBundleInfoJoin02Map = JoinAndSelectOperation.doJoinAndSelect(bbyBundleInfoGroup02DF, bbyBundleInfoJoin01InnerRenamedDF, bbyBundleInfoJoin02)
    val bbyBundleInfoJoin02InnerDF = bbyBundleInfoJoin02Map(INNER_JOIN)

    // formula
    val bbyBundleInfoFormula02DF = bbyBundleInfoJoin02InnerDF.withColumn("Ratio", (col("POS Qty") / col("Sum_POS Qty")))

    // formula
    val bbyBundleInfoFormula03DF = bbyBundleInfoFormula02DF.withColumn("Bundle Qty Est",
      when(col("Online") === 1, floor(col("Ratio") * col("Units")))
        .otherwise(ceil(col("Ratio") * col("Units"))))

    // formula
    val bbyBundleInfoFormula04DF = bbyBundleInfoFormula03DF.withColumn("Bundle Qty",
      when(!isnull(col("Bundle Qty Raw")), col("Bundle Qty Raw"))
        .otherwise(col("Bundle Qty Est")))
      .withColumn("Bundle Qty Source",
        when(!isnull(col("Bundle Qty Raw")), "Raw")
          .otherwise("Est"))

    // formula
    val bbyBundleInfoFormula05DF = bbyBundleInfoFormula04DF.withColumn("POS Qty", (col("POS Qty") - col("Bundle Qty")))
      .withColumn("Account Major", lit("Best Buy"))

    // join
    val bbyBundleInfoJoin03 = bbyBundleInfoSource.joinOperation(JOIN03)
    val bbyBundleInfoFormula05RenamedDF = Utils.convertListToDFColumnWithRename(bbyBundleInfoSource.renameOperation(RENAME04), bbyBundleInfoFormula05DF)
    val bbyBundleInfoJoin03Map = JoinAndSelectOperation.doJoinAndSelect(auxTablesSKUHierarchyFormula01
        .withColumnRenamed("Raw POS Qty", "Raw POS Qty_org")
        .withColumnRenamed("POS Qty", "Raw POS Qty")
      ,bbyBundleInfoFormula05RenamedDF, bbyBundleInfoJoin03)
    val bbyBundleInfoJoin03LeftDF = bbyBundleInfoJoin03Map(LEFT_JOIN)
      .withColumn("POS Qty", col("Raw POS Qty"))
    val bbyBundleInfoJoin03InnerDF = bbyBundleInfoJoin03Map(INNER_JOIN)

    // union
    val unionLeftAndInnerJoinDF = UnionOperation.doUnion(bbyBundleInfoJoin03LeftDF, bbyBundleInfoJoin03InnerDF).get

    // join
    val bbyBundleInfoAndAuxTablesOnlineJoin04 = bbyBundleInfoSource.joinOperation(JOIN04)

    val unionLeftAndInnerJoinDFLeftRenamedDF = Utils.convertListToDFColumnWithRename(bbyBundleInfoSource.renameOperation(RENAME05), unionLeftAndInnerJoinDF)
    val auxTablesOnlineSelect02RightRenamedDF = Utils.convertListToDFColumnWithRename(bbyBundleInfoSource.renameOperation(RENAME06), auxTablesOnlineSelect02DF)
    val bbyBundleInfoJoin04Map = JoinAndSelectOperation.doJoinAndSelect(unionLeftAndInnerJoinDFLeftRenamedDF, auxTablesOnlineSelect02RightRenamedDF, bbyBundleInfoAndAuxTablesOnlineJoin04)
    val bbyBundleInfoJoin04LeftDF = bbyBundleInfoJoin04Map(LEFT_JOIN)
    val bbyBundleInfoJoin04InnerDF = bbyBundleInfoJoin04Map(INNER_JOIN)
    val bbyBundleInfoJoin04RightDF = bbyBundleInfoJoin04Map(RIGHT_JOIN)


    // formula
    val bbyBundleInfoFormula06DF = bbyBundleInfoJoin04LeftDF.withColumn("Distribution_Inv",
      when(col("POS_Qty") > 0, 1)
        otherwise (0))


    // union with append max weekend date
    val unionFormulaAndInnerJoinDF = UnionOperation.doUnion(bbyBundleInfoJoin04InnerDF, bbyBundleInfoFormula06DF).get
    val calMaxWED = unionFormulaAndInnerJoinDF.agg(max("Week_End_Date")).head().getDate(NUMERAL0)
    val unionAppendMaxWeekEndDate = unionFormulaAndInnerJoinDF.withColumn("Max_Week_End_Date", lit(calMaxWED))

    // browse here

    // filter
//    val last26Weeks70000DaysDF = unionAppendMaxWeekEndDate.withColumn("date_last_26weeks", date_sub(col("Max_Week_End_Date"), 70000))
//    print("Datelast26Weeks " + last26Weeks70000DaysDF.select("date_last_26weeks").show(1))
//    val bbyBundleInfoWedGreaterThan70000Filter01 = bbyBundleInfoSource.filterOperation(FILTER01)
//    val bbyBundleInfoWedGreaterThan70000Filter01DF = FilterOperation.doFilter(last26Weeks70000DaysDF, bbyBundleInfoWedGreaterThan70000Filter01, bbyBundleInfoWedGreaterThan70000Filter01.conditionTypes(NUMERAL0)).get
//    val bbyBundleInfoWedGreaterThan70000Filter01DF = last26Weeks70000DaysDF.filter(col("Week_End_Date") > col("date_last_26weeks"))
//    print("bbyBundleInfoWedGreaterThan70000Filter01DF - " + bbyBundleInfoWedGreaterThan70000Filter01DF.count() + " >> ")
    /* Commenting Existing POS starts */
//    val existingPOSSelect01 = existingPOSSource.selectOperation(SELECT01)
//    val existingPOSSelect01DF = SelectOperation.doSelect(existingPOS, existingPOSSelect01.cols, existingPOSSelect01.isUnknown).get
//
//    // join
//    val bbyBundleInfoAndAuxTablesOnlineJoin05 = bbyBundleInfoSource.joinOperation(JOIN05)
//    val bbyBundleInfoLast26WeeksRenamedDF = Utils.convertListToDFColumnWithRename(bbyBundleInfoSource.renameOperation(RENAME02), bbyBundleInfoWedGreaterThan70000Filter01DF)
//    val bbyBundleInfoJoin05Map = JoinAndSelectOperation.doJoinAndSelect(existingPOSSelect01DF, bbyBundleInfoLast26WeeksRenamedDF, bbyBundleInfoAndAuxTablesOnlineJoin05)
//    val bbyBundleInfoJoin05LeftDF = /*Utils.convertListToDFColumnWithRename(bbyBundleInfoSource.renameOperation(RENAME02),*/  bbyBundleInfoJoin05Map(LEFT_JOIN)
//
//    // union
//    val unionFilterAndJoin05DF = UnionOperation.doUnion(bbyBundleInfoJoin05LeftDF, bbyBundleInfoWedGreaterThan70000Filter01DF).get

    /* Commenting Existing POS ends */

    // formula
    val bbyBundleInfoFormula07DF = unionAppendMaxWeekEndDate.withColumn("Season",
      when(col("IPSLES") === "IPS", col("Season"))
        .otherwise(when(col("Week_End_Date") === "2016-10-01", "BTS'16")
          .when(col("Week_End_Date") === "2016-12-31", "HOL'16")
          .when(col("Week_End_Date") === "2017-04-01", "BTB'17")
          .when(col("Week_End_Date") === "2017-07-01", "STS'17")
          otherwise (col("Season"))))

    // sort
    val bbyBundleInfoSort01 = bbyBundleInfoSource.sortOperation(SORT01)
    val bbyBundleInfoSort01DF = SortOperation.doSort(bbyBundleInfoFormula07DF, bbyBundleInfoSort01.ascending, bbyBundleInfoSort01.descending).get

    // unique
    val format = new SimpleDateFormat("d-M-y h-m-s");


    bbyBundleInfoSort01DF/*.dropDuplicates(List("Account", "Online", "SKU", "Week_End_Date", "Max_Week_End_Date"))*/
      .select("Account","Online","SKU","SKU_Name","IPSLES","Week_End_Date","POS_Qty","Season","Street_Price",
        "Category","Category_Subgroup","Line","PL","L1_Category","L2_Category","Raw_POS_Qty","GA_date","ES_date",
        "Distribution_Inv","Category_1","Category_2","Category_3","HPS/OPS","Series","Category Custom","Brand",
        "Max_Week_End_Date","Season_Ordered","Cal_Month","Cal_Year","Fiscal_Qtr","Fiscal_Year")
      .coalesce(1).write.mode(SaveMode.Overwrite).option("header", true).csv("/etherData/Pricing/Outputs/POS_Retail/temp/before_posqty_output_retail_"+currentTS+".csv")
    // formula
    //val bbyBundleInfoFormula08DF = bbyBundleInfoSort01DF.withColumn("Workflow Run Date", current_date())

    // browse
  }

}
