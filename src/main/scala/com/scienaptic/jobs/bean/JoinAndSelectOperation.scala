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
  def doJoinAndSelect(dataFrame1: DataFrame, dataFrame2: DataFrame, joinOperation: JoinAndSelectOperation, typeOfJoin: String) = {

    val joinExpr = generateJoinExpression(joinOperation, dataFrame1, dataFrame2)

    val column_names_left = Utils.convertListToDFColumn(joinOperation.selectCriteria("left"), dataFrame1)
    val column_names_right = Utils.convertListToDFColumn(joinOperation.selectCriteria("right"), dataFrame1)

    val selectAll: List[Column] = column_names_left ::: column_names_right
    if(selectAll.size == 0){
      dataFrame1.join(dataFrame2, joinExpr, typeOfJoin)
    } else {
      dataFrame1.join(dataFrame2, joinExpr, typeOfJoin).select(selectAll: _*)
    }
    //TODO: Check if joining criteria is same and column mentioned in left and right select lists
    //TODO: Check if select criteria is blank and joining column is same.
  }

  private def generateJoinExpression(join: JoinAndSelectOperation, dataFrame1: DataFrame, dataFrame2: DataFrame) = {
    join.joinCriteria("left")
      .zip(join.joinCriteria("right"))
      .map { case (c1, c2) => dataFrame1(c1) === dataFrame2(c2) }
      .reduce(_ && _)
  }
}
