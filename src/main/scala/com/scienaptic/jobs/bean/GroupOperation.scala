package com.scienaptic.jobs.bean

import com.fasterxml.jackson.annotation.JsonProperty
import com.scienaptic.jobs.utility.Utils
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ListBuffer
import scala.util.Try

case class GroupOperation(@JsonProperty("cols") cols: List[String], @JsonProperty("aggregations") aggregations: Map[String, Map[String, String]], @JsonProperty("selectCriteria") selectCols: List[String]) extends Operation {
  override def scienapticDef() = {
    //Do Nothing
  }
}

object GroupOperation {
  def doGroup(dataFrame: DataFrame, groupOp: GroupOperation/*, cols: List[String], aggregations: Map[String, Map[String, String]]*/) : Try[DataFrame] = {
    Try{
      val cols = groupOp.cols
      val aggregations = groupOp.aggregations
      var selectCols = groupOp.selectCols
      if (selectCols.isEmpty) selectCols=dataFrame.columns.toList
      //val df = dataFrame.select(Utils.convertListToDFColumn(selectCols, dataFrame): _*)
      val df = dataFrame
      var renameMap = scala.collection.mutable.Map[String, String]()
      var aggregationMap = scala.collection.mutable.Map[String, String]()
      var aggregationColumns = List[String]()
      var aggrRenamedColumns = List[String]()

      for ((operation, operationMap) <- aggregations) {
        for ((aggrCol,aggrRenameColumn) <- operationMap) {
          aggrRenamedColumns = aggrRenamedColumns :+ aggrRenameColumn
          aggregationColumns = aggregationColumns :+ aggrCol
          val aggregatedColumnName = s"$operation" + "(" + s"$aggrCol" + ")"
          renameMap(aggregatedColumnName) = aggrRenameColumn
          if (aggrRenameColumn=="") renameMap(aggregatedColumnName)=aggregatedColumnName
          aggregationMap(aggrCol) = operation
        }
      }
//      selectCols = selectCols ::: aggrRenamedColumns
      val aggMap = aggregationMap.toMap
      val groupedDataSet = df.groupBy(Utils.convertListToDFColumn(cols, df): _*)
      val aggregatedDataSet = Utils.convertListToDFColumnWithRename(renameMap.toMap, groupedDataSet.agg(aggMap))
      Utils.convertListToDFColumnWithRename(renameMap.toMap, aggregatedDataSet)
//      val groupWithAllColumnsDF = df.drop(aggregationColumns.toList:_*).join(aggregatedDataSet, cols.toSeq, "inner")
//      if (selectCols.size == 0) {
//        selectCols = (selectCols ::: dataFrame.columns.toList) filterNot aggregationColumns.toSet
//      }
//      groupWithAllColumnsDF.select(Utils.convertListToDFColumn(selectCols, groupWithAllColumnsDF): _*)
    }
  }
}