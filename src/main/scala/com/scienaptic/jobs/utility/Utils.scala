package com.scienaptic.jobs.utility

import com.scienaptic.jobs.ExecutionContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

import scala.util.Try
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType


object Utils {

  def convertListToDFColumn(columnList: List[String], dataFrame: DataFrame) = {
    columnList.map(name => dataFrame.col(name))
  }

  def convertListToDFColumnWithRename(renameMap: Map[String, String], dataFrame: DataFrame) = {
    renameMap.keySet.toList.foldLeft(dataFrame) { (df, col) =>
      df.withColumnRenamed(col, renameMap.getOrElse(col, col))
    }
  }

  def loadCSV(context: ExecutionContext, file: String): Try[DataFrame] = {
    Try {
      val scienapticDataframe = context.spark.read
        .option("header", true)
        .option("inferSchema", true)
        .csv(file)

      var renameMap = scala.collection.mutable.Map[String, String]()
      scienapticDataframe.columns.map(x => {
        if (x contains ".") {
          val orgCol = x
          renameMap(orgCol) = x.replace(".", "_")
        } else {
          renameMap(x) = x
        }
      })
      convertListToDFColumnWithRename(renameMap.toMap,scienapticDataframe)
    }
  }

  def litColumn(dataFrame: DataFrame, columnName: String, litValue: Any): DataFrame = {
    dataFrame.withColumn(columnName, lit(litValue))
  }

  def nullImputationForNumeralColumns(dataFrame: DataFrame): DataFrame = {
    var imputeDataFrame = dataFrame
    val dataFrameColumns = dataFrame.columns
    dataFrameColumns.map(currColumn => {
      imputeDataFrame.schema(currColumn).dataType match {
        case DoubleType => imputeDataFrame = imputeDataFrame.withColumn(currColumn, when(col(currColumn).isNull, 0).otherwise(col(currColumn)))
        case IntegerType => imputeDataFrame = imputeDataFrame.withColumn(currColumn, when(col(currColumn).isNull, 0).otherwise(col(currColumn)))
        case _ => imputeDataFrame
      }
    })
    imputeDataFrame
  }
}