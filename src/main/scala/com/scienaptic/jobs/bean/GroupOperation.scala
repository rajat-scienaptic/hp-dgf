package com.scienaptic.jobs.bean

import com.fasterxml.jackson.annotation.JsonProperty
import com.scienaptic.jobs.utility.Utils
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ListBuffer
import scala.util.Try

case class GroupOperation(@JsonProperty("cols") cols: List[String], @JsonProperty("aggregations") aggregations: Map[String, Map[String, String]], @JsonProperty("selecCriteria") selectCols: List[String]) extends Operation {
  override def scienapticDef() = {
    //Do Nothing
  }
}

object GroupOperation {
  def doGroup(dataFrame: DataFrame, groupOp: GroupOperation/*, cols: List[String], aggregations: Map[String, Map[String, String]]*/) : Try[DataFrame] = {
    Try{
      val cols = groupOp.cols
      val aggregations = groupOp.aggregations
      var renameMap = scala.collection.mutable.Map[String, String]()
      var aggregationMap = scala.collection.mutable.Map[String, String]()
      var aggregationColumns = new ListBuffer[String]()

      for ((operation, operationMap) <- aggregations) {
        for ((aggrCol,aggrRenameColumn) <- operationMap) {
          aggregationColumns += aggrCol
          val aggregatedColumnName = s"$operation" + "(" + s"$aggrCol" + ")"
          renameMap(aggregatedColumnName) = aggrRenameColumn
          if (aggrRenameColumn=="") renameMap(aggregatedColumnName)=aggregatedColumnName
          aggregationMap(aggrCol) = operation
        }
      }
      val aggMap = aggregationMap.toMap
      val groupedDataSet = dataFrame.groupBy(Utils.convertListToDFColumn(cols, dataFrame): _*)
      var aggregatedDataSet = groupedDataSet.agg(aggMap)
      //Utils.convertListToDFColumnWithRename(renameMap.toMap, aggregatedDataSet)
      val groupedDF = Utils.convertListToDFColumnWithRename(renameMap.toMap, aggregatedDataSet)
      val groupWithAllColumnsDF = dataFrame.drop(aggregationColumns.toList:_*).join(groupedDF, cols.toSeq, "inner")
      groupWithAllColumnsDF.select(Utils.convertListToDFColumn(groupOp.selectCols, groupWithAllColumnsDF): _*)
    }
  }
}