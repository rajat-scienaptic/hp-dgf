package com.scienaptic.jobs.utility

import com.scienaptic.jobs.ExecutionContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.util.Try

object Utils {

  val toDouble = udf[Double, String](_.toDouble)

  /*def applyFormula(dataFrame: DataFrame, column: String, formula: String) : Try[DataFrame] = {
    Try{

      dataFrame.withColumn("fixed", toDouble(dataFrame("Fixed"))).select(dataFrame(column) + 100 )
    }
  }*/

  def convertListToDFColumn(columnList: List[String], dataFrame: DataFrame) = {
    columnList.map(name => dataFrame.col(name) /*.as(s"renamed_$name")*/)
  }

  def convertListToDFColumnWithRename(columnList: List[String], dataFrame: DataFrame, newName: String) = {
    columnList.map(name => dataFrame.col(name).as(s"$newName$name"))
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
    }
  }
}