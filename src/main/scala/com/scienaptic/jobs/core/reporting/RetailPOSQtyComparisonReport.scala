package com.scienaptic.jobs.core.reporting

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.bean.GroupOperation
import org.apache.spark.sql.functions.{col,lit}
import org.apache.spark.sql.functions.udf
import com.scienaptic.jobs.utility.Utils.renameColumns
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import scala.util.matching.Regex

object RetailPOSQtyComparisonReport {

  def execute(executionContext: ExecutionContext): Unit = {

    val spark: SparkSession = executionContext.spark
    val currentTS = spark.read.json("/etherData/state/currentTS.json").select("ts").head().getString(0)
    val hadoopfs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val calculatePercentageDiff = (newValue : Int, orgValue : Int) => {
      if(newValue == 0 && orgValue == 0) { 0 } else if (orgValue == 0 ) { 100 } else {((newValue - orgValue) / orgValue) * 100 }
    }

    def calculatePercentageDiffUDF = udf(calculatePercentageDiff)

    val regExForBrackets = new Regex("\\((.*?)\\)")   // matches text between '(text)' that is round brackets
    val regExForAggOperation = new Regex("^(.+?)\\(")   // matches text before '('
    val reportingKeyMapping : Map[String,String] = executionContext.configuration.reporting
    var aggOperationsMap = scala.collection.mutable.Map[String, Map[String, String]]()
    val alteryxRetailInputLocation = reportingKeyMapping("alteryx-current-pos-path")
    val alteryxRetailAggPath = reportingKeyMapping("alteryx-agg-pos-result-path")
    val groupingColumns = reportingKeyMapping("grouping-columns").split(",").toList
    val aggregatingColumns = reportingKeyMapping("aggregating-columns").split(",").toList
    var posQtyColumn = ""
    val outputLocation = reportingKeyMapping("output-basepath").format(currentTS, "excel") // /home/ether/Pricing/reporting/%s/%s/

    def testDirExist(path: String): Boolean = {
      val p = new Path(path)
      hadoopfs.exists(p) && hadoopfs.getFileStatus(p).isDirectory
    }

    def filterOutOfThresholdPOSQty(dataframe: DataFrame, posQtyCol : String) = {
      dataframe
        .filter(col("percentage_difference_in_" + posQtyCol) > 5.0 || col("percentage_difference_in_units") < -5.0)
    }

    def calculateDiff(retailDF: DataFrame, posQtyCol : String) = {
      retailDF
        .withColumn("difference_in_" + posQtyCol, col(posQtyCol) - col(posQtyCol + "_prev"))
        .withColumn("percentage_difference_in_" + posQtyCol, calculatePercentageDiffUDF(col(posQtyCol), col(posQtyCol + "_prev")))
    }

    def generateAggregationMap = {
      for (aggregateColumn <- aggregatingColumns) {
        val operationOption = regExForAggOperation findFirstIn (aggregateColumn)
        val column = regExForBrackets findFirstIn (aggregateColumn)
        val operation = operationOption.get.replace("(", "")
        val col = column.get.replace("(", "").replace(")", "")
        val renamedColumn = col + "_" + operation
        posQtyColumn = col  // set the pos column for later use
        aggOperationsMap(operation) = Map(col -> renamedColumn)
      }
      aggOperationsMap
    }

    val aggregationMap = generateAggregationMap.toMap[String, Map[String,String]]

    val groupOperation = new GroupOperation(groupingColumns, aggregationMap, List.empty[String])

    val retailDataFrameCurr = renameColumns(spark.read.option("header", "true").option("inferSchema", "true")
      .csv(alteryxRetailInputLocation.format(currentTS)))

    val retailAggPOSCurr = GroupOperation.doGroup(retailDataFrameCurr, groupOperation).get

    // Checking if Path exists for previously aggregated report
    val isAggDirPresent = testDirExist(alteryxRetailAggPath)

    if(isAggDirPresent) {
      // Read Previous Aggregated
      val retailDataFramePrev = renameColumns(spark.read.option("header", "true").option("inferSchema", "true")
        .csv(alteryxRetailAggPath))
        .withColumnRenamed(posQtyColumn, posQtyColumn + "_prev")
          .drop("difference_in_" + posQtyColumn, "percentage_difference_in_" + posQtyColumn)

      val joinCurrAndPrevRetailAggPOS = retailAggPOSCurr.join(retailDataFramePrev, groupingColumns, "left")

      val calculatePercentageDiffAndFilter = filterOutOfThresholdPOSQty(calculateDiff(joinCurrAndPrevRetailAggPOS, posQtyColumn), posQtyColumn)

      // final write to HDFS
      calculatePercentageDiffAndFilter.coalesce(1).write.option("header", "true")
        .mode(SaveMode.Overwrite).csv(alteryxRetailAggPath)

    } else {
      // If aggregated data doesnt exist, then its a fresh run and difference is 100%
      val appendAggcols = retailAggPOSCurr
        .withColumn("difference_in_" + posQtyColumn, lit(100))
        .withColumn("percentage_difference_in_" + posQtyColumn, lit(100.0))

      // final write to HDFS
      appendAggcols.coalesce(1).write.option("header", "true")
        .mode(SaveMode.Overwrite).csv(alteryxRetailAggPath)
    }

  }
}
