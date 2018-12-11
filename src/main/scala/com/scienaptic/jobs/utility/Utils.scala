package com.scienaptic.jobs.utility

import com.scienaptic.jobs.ExecutionContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

import scala.util.Try

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
        .format("csv")
        .option("header", true)
        .option("inferSchema", true)
        .load(file)

      scienapticDataframe.columns.map(x => {
        if (x contains ".") {
          x.replace(".", "_")
        }
        else x
      })
      scienapticDataframe
    }
  }

  def litColumn(dataFrame: DataFrame, columnName: String, litValue: Any): DataFrame = {
    dataFrame.withColumn(columnName, lit(litValue))
  }
}