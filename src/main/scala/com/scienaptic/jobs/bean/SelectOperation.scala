package com.scienaptic.jobs.bean

import com.fasterxml.jackson.annotation.JsonProperty
import com.scienaptic.jobs.utility.Utils
import org.apache.spark.sql.DataFrame

import scala.util.Try


case class SelectOperation(@JsonProperty("isUnknown") isUnknown: String,@JsonProperty("cols") cols: List[String]) extends Operation {
  override def scienapticDef() = {
    println("Select initiated")
  }
}

object SelectOperation {
  def doSelect(dataFrame: DataFrame, cols: List[String], unknownFlag: String = "N"): Try[DataFrame] = {
    Try {
      unknownFlag match {
        case "N" | "n" | "" => dataFrame.select(Utils.convertListToDFColumn(cols, dataFrame): _*)
        case "Y" | "y" => {
            val allCols = dataFrame.columns.toList
            val removeCols = cols
            // if unknown flag is true then exclude the columns in select
            dataFrame.select(Utils.convertListToDFColumn(allCols diff removeCols, dataFrame): _*)
        }
      }
    }
  }
}