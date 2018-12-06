package com.scienaptic.jobs.bean

import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.base.Preconditions
import com.scienaptic.jobs.utility.Utils
import org.apache.spark.sql.{Column, DataFrame}


case class JoinAndSelectOperation(@JsonProperty("leftTableAlias") leftTableAlias: String,
                                  @JsonProperty("rightTableAlias") rightTableAlias: String,
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

  def generateColumnsFromJoinDF(dataFrame1: DataFrame, dataFrame2: DataFrame): scala.List[_root_.org.apache.spark.sql.Column] = {
    val leftColumnSet = Utils.convertListToDFColumn(dataFrame1.columns.toList, dataFrame1).toSet
    val rightColumnSet = Utils.convertListToDFColumn(dataFrame2.columns.toList, dataFrame2).toSet
    leftColumnSet union rightColumnSet toList
  }

  def doJoinAndSelect(dataFrame1: DataFrame, dataFrame2: DataFrame, joinOperation: JoinAndSelectOperation) = {

    var joinMap = Map[String, DataFrame]()
    val joinTypes = joinOperation.typeOfJoin
    val joinExpr = generateJoinExpression(joinOperation, dataFrame1, dataFrame2)
    val column_names_left = Utils.convertListToDFColumn(joinOperation.selectCriteria("left"), dataFrame1)
    val column_names_right = Utils.convertListToDFColumn(joinOperation.selectCriteria("right"), dataFrame2)

    val selectAll: List[Column] = if (checkIfNullColumns(column_names_left, column_names_right)) {
      generateColumnsFromJoinDF(joinOperation.joinCriteria("left"), joinOperation.joinCriteria("right"), dataFrame1, dataFrame2)
    } else {
      column_names_left ::: column_names_right
    }

    joinTypes.foreach(joinType => {
      joinMap(joinType) = dataFrame1.join(dataFrame2, joinExpr, joinType).select(selectAll: _*)
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
