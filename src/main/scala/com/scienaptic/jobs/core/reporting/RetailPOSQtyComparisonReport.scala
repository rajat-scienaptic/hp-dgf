package com.scienaptic.jobs.core.reporting

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.bean.GroupOperation
import org.apache.spark.sql.functions.{col,lit}
import org.apache.spark.sql.functions.udf
import com.scienaptic.jobs.utility.Utils.renameColumns
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs._

import scala.util.matching.Regex

object RetailPOSQtyComparisonReport {

  def execute(executionContext: ExecutionContext): Unit = {

    val spark: SparkSession = executionContext.spark
    val currentTS = spark.read.json("/etherData/state/currentTS.json").select("ts").head().getString(0)
    val hadoopfs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val calculatePercentageDiff = (newValue : Double, oldValue : Double) => {
      if(newValue == 0 && oldValue == 0) {
        0.0
      }else if (oldValue == 0 ) {
        if(newValue < 0){
          -100.0
        }else{
          100.0
        }
      }else if((oldValue < 0 && newValue == 0) | (newValue < 0 && oldValue < 0 ) | (newValue > 0 && oldValue < 0 )) {
        ((oldValue - newValue) / oldValue) * 100
      } else {
        ((newValue - oldValue) / oldValue) * 100
      }
    }

    def calculatePercentageDiffUDF = udf(calculatePercentageDiff)

    //read config json
    val regExForBrackets = new Regex("\\((.*?)\\)")   // matches text between '(text)' that is round brackets
    val regExForAggOperation = new Regex("^(.+?)\\(")   // matches text before '('
    val reportingKeyMapping : Map[String,String] = executionContext.configuration.reporting
    var aggOperationsMap = scala.collection.mutable.Map[String, Map[String, String]]()

    var currentFilePath = reportingKeyMapping("currentFileFullPath")
    var previousFilePath = reportingKeyMapping("previousFileFullPath")
    val outputTableName = reportingKeyMapping("outputHiveTableName")

    if(currentFilePath == "" && previousFilePath == ""){

      val baseFilePath = reportingKeyMapping("baseOutputFilePath")
      val path = new Path(baseFilePath)
      val fs = path.getFileSystem(spark.sessionState.newHadoopConf)
      val inodes = fs.listStatus(path).sortBy(_.getModificationTime)
      val allpaths = inodes.filter(_.getModificationTime > 0).map(t => (t.getPath))

      //filter by file name pattern
      val filePattern = reportingKeyMapping("inputFileNamePattern")
      val needed_paths = allpaths.filter(_.getName.contains(filePattern))
      val topTwoPaths=needed_paths.takeRight(2)

      previousFilePath = topTwoPaths.head.toString
      currentFilePath = topTwoPaths.last.toString
    }


    println("################## " + currentFilePath)
    println("################## " + previousFilePath)

    val outputFilePath = reportingKeyMapping("outputFileFullPath")

    val groupingColumns = reportingKeyMapping("grouping-columns").split(",").toList
    val aggregatingColumns = reportingKeyMapping("aggregating-columns").split(",").toList

    var posQtyColumn = ""

    def testDirExist(path: String): Boolean = {
      val p = new Path(path)
      hadoopfs.exists(p) && hadoopfs.getFileStatus(p).isDirectory
    }

    def filterOutOfThresholdPOSQty(dataframe: DataFrame, posQtyCol : String) = dataframe
      .filter(col("percentage_difference_in_" + posQtyCol) > 5.0 || col("percentage_difference_in_" + posQtyCol) < -5.0)

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
        //val renamedColumn = col + "_" + operation
        val renamedColumn = col
        posQtyColumn = col  // set the pos column for later use
        aggOperationsMap(operation) = Map(col -> renamedColumn)
      }
      aggOperationsMap
    }

    val aggregationMap = generateAggregationMap.toMap[String, Map[String,String]]

    val groupOperation = new GroupOperation(groupingColumns, aggregationMap, List.empty[String])

    //load current file
    val retailDataFrameCurr = renameColumns(spark.read.option("header", "true").option("inferSchema", "true")
      .csv(currentFilePath))

    //load previous file
    val retailDataFramePrev = renameColumns(spark.read.option("header", "true").option("inferSchema", "true")
      .csv(previousFilePath))


    //load current retail pos result file
    val retailAggCurr = GroupOperation.doGroup(retailDataFrameCurr, groupOperation).get

    //load previous retail pos result file
    val retailAggPrev = GroupOperation.doGroup(retailDataFramePrev, groupOperation).get
      .withColumnRenamed(posQtyColumn, posQtyColumn + "_prev")

    //join current with previous
    // INNER
    val joinCurrAndPrevRetailAggPOSInner = retailAggCurr.join(retailAggPrev, groupingColumns, "inner")
    // LEFT
    val joinCurrAndPrevRetailAggPOSLeft = retailAggCurr.join(retailAggPrev, groupingColumns, "leftanti")
    // RIGHT
    val joinCurrAndPrevRetailAggPOSRight = retailAggPrev.join(retailAggCurr, groupingColumns, "leftanti")

    val cols1 = joinCurrAndPrevRetailAggPOSInner.columns.toSet
    val cols2 = joinCurrAndPrevRetailAggPOSLeft.columns.toSet
    val cols3 = joinCurrAndPrevRetailAggPOSRight.columns.toSet
    val totalCols = cols1 ++ cols2 ++ cols3

    def expr(myCols: Set[String], allCols: Set[String]) = {
      allCols.toList.map(x => x match {
        case x if myCols.contains(x) => col(x)
        case _ => lit(null).as(x)
      })
    }

    var dfFinal = joinCurrAndPrevRetailAggPOSInner.select(expr(cols1, totalCols):_*)
      .unionAll(joinCurrAndPrevRetailAggPOSLeft.select(expr(cols2, totalCols):_*)
        .unionAll(joinCurrAndPrevRetailAggPOSRight.select(expr(cols3, totalCols):_*)))

    dfFinal = dfFinal.na.fill(0.0, Seq(posQtyColumn + "_prev")).na.fill(0.0, Seq(posQtyColumn))

    //filter records with % diff of >5 OR <-5
    var calculatePercentageDiffAndFilter = filterOutOfThresholdPOSQty(calculateDiff(dfFinal, posQtyColumn), posQtyColumn)


    var selectClmns:List[String] = groupingColumns
    selectClmns = selectClmns :+ posQtyColumn
    selectClmns = selectClmns :+ posQtyColumn + "_prev"
    selectClmns = selectClmns :+ "difference_in_" + posQtyColumn
    selectClmns = selectClmns :+ "percentage_difference_in_" + posQtyColumn

    //write final result file into hdfs
    calculatePercentageDiffAndFilter = calculatePercentageDiffAndFilter
      .withColumn("Week_End_Date",  org.apache.spark.sql.functions.date_format(col("Week_End_Date"), "yyyy-MM-dd"))


    calculatePercentageDiffAndFilter.select(selectClmns.head, selectClmns.tail: _*).coalesce(1).write.option("header", "true")
      .mode(SaveMode.Overwrite).csv(outputFilePath)

    val endResultDF = calculatePercentageDiffAndFilter.columns.foldLeft(calculatePercentageDiffAndFilter)((curr, n) => curr.withColumnRenamed(n, n.replaceAll("\\s", "_")))

    //write to hive
    endResultDF.write.mode(org.apache.spark.sql.SaveMode.Overwrite).saveAsTable(outputTableName)
  }
}
