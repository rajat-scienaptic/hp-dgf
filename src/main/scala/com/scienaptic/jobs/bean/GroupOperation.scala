package com.scienaptic.jobs.bean

import com.fasterxml.jackson.annotation.JsonProperty
import com.scienaptic.jobs.utility.Utils
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.RelationalGroupedDataset

import scala.util.Try

case class GroupOperation(@JsonProperty("cols") cols: List[String], @JsonProperty("aggreg") aggregation: String, @JsonProperty("aggrColumns") aggColumns: List[String]) extends Operation {
  override def scienapticDef() = {
    //Do Nothing
  }
}

object GroupOperation {
  def doGroup(dataFrame: DataFrame, cols: List[String], operation: String, aggColumns: List[String]) : Try[DataFrame] = {
    val groupedDataSet = dataFrame.groupBy(Utils.convertListToDFColumn(cols, dataFrame): _*)
    operation match {
      case "sum" => {
        performSumAggration(groupedDataSet, aggColumns)
      }
      case "avg" => {
        performAvgAggregation(groupedDataSet, aggColumns)
      }
      case "max" => {
        performMaxAggregation(groupedDataSet, aggColumns)
      }
    }
  }

  def performSumAggration(dataFrame: RelationalGroupedDataset, cols: List[String]): Try[DataFrame] = {
    Try{
      dataFrame.sum(cols: _*)
    }
  }

  def performAvgAggregation(dataFrame: RelationalGroupedDataset, cols: List[String]): Try[DataFrame] = {
    Try{
      dataFrame.avg(cols: _*)
    }
  }

  def performMaxAggregation(dataFrame: RelationalGroupedDataset, cols: List[String]): Try[DataFrame] = {
    Try{
      dataFrame.max(cols: _*)
    }
  }
}