package com.scienaptic.jobs.bean

import com.fasterxml.jackson.annotation.JsonProperty
import com.scienaptic.jobs.utility.Utils
import org.apache.spark.sql.DataFrame

import scala.util.Try

case class GroupOperation(@JsonProperty("cols") cols: List[String], @JsonProperty("aggregations") aggregations: Map[String, Map[String, String]]) extends Operation {
  override def scienapticDef() = {
    //Do Nothing
  }
}

object GroupOperation {
  def doGroup(dataFrame: DataFrame, cols: List[String], aggregations: Map[String, Map[String, String]]) : Try[DataFrame] = {
    Try{
      //var aggColumns = List[String]()
      var renameMap = scala.collection.mutable.Map[String, String]()
      var aggregationMap = scala.collection.mutable.Map[String, String]()

      for ((operation, operationMap) <- aggregations) {
        for ((aggrCol,aggrRenameColumn) <- operationMap) {
          //aggColumns :+ aggrCol
          val aggregatedColumnName = s"$operation" + "(" + s"$aggrCol" + ")"
          renameMap(aggregatedColumnName) = aggrRenameColumn
          if (aggrRenameColumn=="") renameMap(aggregatedColumnName)=aggregatedColumnName
          aggregationMap(aggrCol) = operation
        }
      }
      val aggMap = aggregationMap.toMap
      //println(s"Aggregation Map:  $aggMap")
      val groupedDataSet = dataFrame.groupBy(Utils.convertListToDFColumn(cols, dataFrame): _*)
      var aggregatedDataSet = groupedDataSet.agg(aggMap)
      Utils.convertListToDFColumnWithRename(renameMap.toMap, aggregatedDataSet)
    }
  }
}