package com.scienaptic.jobs.utility

import com.scienaptic.jobs.ExecutionContext
import org.apache.spark.sql.DataFrame

import scala.util.Try

object Utils {

  def convertListToDFColumn(columnList: List[String], dataFrame: DataFrame) = {
    columnList.map(name => dataFrame.col(name) /*.as(s"renamed_$name")*/)
  }

  //TODO: Accept newname as List[String] and merge with 'name' to rename the column
  def convertListToDFColumnWithRename(columnList: List[String], dataFrame: DataFrame, newName: String) = {
    columnList.map(name => dataFrame.col(name).as(s"$newName"))
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
}