package com.scienaptic.jobs.bean

import com.fasterxml.jackson.annotation.JsonProperty
import com.scienaptic.jobs.utility.Utils
import org.apache.spark.sql.{Column, DataFrame}


case class JoinAndSelectOperation(@JsonProperty("isUnknown") isUnknown: String,
                                  @JsonProperty("typeOfJoin") typeOfJoin: List[String],
                                  @JsonProperty("joinCriteria") joinCriteria: Map[String, List[String]],
                                  @JsonProperty("selectCriteria") selectCriteria: Map[String, List[String]]) extends Operation {
  override def scienapticDef() = {
    println("Join initiated")
  }
}

object JoinAndSelectOperation {
  def checkIfNullColumns(column_names_left: List[Column], column_names_right: List[Column]) = {
    column_names_left.isEmpty && column_names_right.isEmpty
  }

  def generateColumnsFromJoinDF(dataFrame1: DataFrame, dataFrame2: DataFrame): scala.List[Column] = {
    val leftColumnSet = Utils.convertListToDFColumn(dataFrame1.columns.toList, dataFrame1).toSet
    val rightColumnSet = Utils.convertListToDFColumn(dataFrame2.columns.toList, dataFrame2).toSet
    leftColumnSet union rightColumnSet toList
  }

  def getColumnsWithUnknownFilter(column_names: List[Column], isUnknownChecked: String = "N", dataFrame: DataFrame) = {
    isUnknownChecked match {
      case "Y" | "y" => {
        val allCols = Utils.convertListToDFColumn(dataFrame.columns.toList, dataFrame)
        val removeCols = column_names

        allCols diff removeCols
      }
      case "N" | "n" | "" => column_names
    }
  }

  def checkIfDuplicatesPresent(left_columns: List[String], right_columns: List[String]) = {
    val intersectList  = left_columns.toSet intersect right_columns.toSet
    right_columns diff intersectList.toList
  }

  def doJoinAndSelect(dataFrame1: DataFrame, dataFrame2: DataFrame, joinOperation: JoinAndSelectOperation) = {

    var joinMap = scala.collection.mutable.Map[String, DataFrame]()
    val joinTypes = joinOperation.typeOfJoin
    val isUnknownChecked = joinOperation.isUnknown
    val joinExpr = generateJoinExpression(joinOperation, dataFrame1, dataFrame2)
    var left_columns =  joinOperation.selectCriteria("left")
    var right_columns =  checkIfDuplicatesPresent(left_columns, joinOperation.selectCriteria("right")).toList
    val column_names_left = Utils.convertListToDFColumn(left_columns, dataFrame1)
    val column_names_right = Utils.convertListToDFColumn(right_columns, dataFrame2)

    val selectAll: List[Column] = if (checkIfNullColumns(column_names_left, column_names_right)) {
        generateColumnsFromJoinDF(dataFrame1, dataFrame2)
    } else {
      // if unknown flag is true then exclude the columns in select and merge both left and right column list
      getColumnsWithUnknownFilter(column_names_left, isUnknownChecked, dataFrame1) ::: getColumnsWithUnknownFilter(column_names_right, isUnknownChecked, dataFrame2)
    }

    joinTypes.foreach(joinType => {
      joinType match {
        case "leftanti" => joinMap(joinType) = dataFrame1.join(dataFrame2, joinExpr, joinType).select(dataFrame1.col("*"))
        case "rightanti" => joinMap(joinType) = dataFrame2.join(dataFrame1, joinExpr, "leftanti").select(dataFrame2.col("*"))
        case _ => joinMap(joinType) = dataFrame1.join(dataFrame2, joinExpr, joinType).select(selectAll: _*)
        //joinMap(joinType) = dataFrame1.join(dataFrame2, joinExpr, joinType).select(selectAll: _*)
      }
    })

    joinMap
  }

  private def generateJoinExpression(join: JoinAndSelectOperation, dataFrame1: DataFrame, dataFrame2: DataFrame) = {
    join.joinCriteria("left")
      .zip(join.joinCriteria("right"))
      .map { case (c1, c2) => dataFrame1(c1) === dataFrame2(c2) }
      .reduce(_ && _)
  }
}