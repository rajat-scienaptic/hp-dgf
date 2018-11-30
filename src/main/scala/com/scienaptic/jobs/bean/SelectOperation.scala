package com.scienaptic.jobs.bean

import com.fasterxml.jackson.annotation.JsonProperty
import com.scienaptic.jobs.utility.Utils
import org.apache.spark.sql.DataFrame

import scala.util.Try


case class SelectOperation(@JsonProperty("cols") cols: List[String]) extends Operation {
  override def scienapticDef() = {
    println("Select initiated")
  }
}

object SelectOperation {
  def doSelect(dataFrame: DataFrame, cols: List[String]): Try[DataFrame] = {
    Try {
      dataFrame.select(Utils.convertListToDFColumn(cols, dataFrame): _*)
    }
  }
}