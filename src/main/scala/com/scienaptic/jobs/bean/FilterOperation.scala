package com.scienaptic.jobs.bean

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.{Column, DataFrame}
import scala.util.Try

case class FilterOperation(@JsonProperty("conditionTypes") conditionTypes: List[String],
                           @JsonProperty("multiWordMap") multiWordMap: Map[String, String],
                           @JsonProperty("conditions") conditions: List[String]) extends Operation {
  override def scienapticDef() = {
    println("Filter initiated")
  }
}

object FilterOperation {

  def doFilter(dataFrame: DataFrame, filterOperation: FilterOperation, conditionType: String): Try[DataFrame] = {
    Try {
      val getColumnList = generateFilterCondition(dataFrame, filterOperation.conditions, filterOperation.multiWordMap)
      val filterConditions = reduceWithType(conditionType, getColumnList)

      dataFrame.filter(filterConditions)
    }
  }

  private def reduceWithType(conditionType: String, filterConditions: List[Column]) = {
    conditionType match {
      case "and" => filterConditions.reduce(_ and _)
      case "or" => filterConditions.reduce(_ or _)
      case _ => throw new Exception("Invalid condition type")
    }
  }

  private def generateFilterCondition(dataFrame: DataFrame, conditions: List[String], multiWordMap: Map[String, String] = Map.empty[String, String]): List[Column] = {
    conditions.map(cond => {
      val arr = cond.split("\\s+")
      if (arr.size != 3) throw new Exception("Invalid join conditions!") else
        arr(1) match {
          case "<" => {
            if (multiWordMap != null) {
              dataFrame(multiWordMap.getOrElse(arr(0), arr(0))) < multiWordMap.getOrElse(arr(2), arr(2))
            } else {
              dataFrame(arr(0)) < arr(2)
            }
          }
          case "<=" => {
            if (multiWordMap != null) {
              dataFrame(multiWordMap.getOrElse(arr(0), arr(0))) <= multiWordMap.getOrElse(arr(2), arr(2))
            } else  {
              dataFrame(arr(0)) <= arr(2)
            }
          }
          case "=" => {
            if (multiWordMap != null) {
              dataFrame(multiWordMap.getOrElse(arr(0), arr(0))) === multiWordMap.getOrElse(arr(2), arr(2))
            } else {
              dataFrame(arr(0)) === arr(2)
            }
          }
          case ">=" => {
            if (multiWordMap != null) {
              dataFrame(multiWordMap.getOrElse(arr(0), arr(0))) >= multiWordMap.getOrElse(arr(2), arr(2))
            } else {
              dataFrame(arr(0)) >= arr(2)
            }
          }
          case ">" => {
            if (multiWordMap != null) {
              dataFrame(multiWordMap.getOrElse(arr(0), arr(0))) > multiWordMap.getOrElse(arr(2), arr(2))
            } else {
              dataFrame(arr(0)) > arr(2)
            }
          }
          case "!=" => {
            if (multiWordMap != null) {
              dataFrame(multiWordMap.getOrElse(arr(0), arr(0))) =!= multiWordMap.getOrElse(arr(2), arr(2))
            } else {
              dataFrame(arr(0)) =!= arr(2)
            }
          }
          case "contains" => {
            if (multiWordMap != null) {
              dataFrame(multiWordMap.getOrElse(arr(0), arr(0))).contains(multiWordMap.getOrElse(arr(2), arr(2)))
            } else {
              dataFrame(arr(0)).contains(arr(2))
            }
          }
          case "not_contains" => {
            if (multiWordMap != null) {
              !dataFrame(multiWordMap.getOrElse(arr(0), arr(0))).contains(multiWordMap.getOrElse(arr(2), arr(2)))
            } else {
              !dataFrame(arr(0)).contains(arr(2))
            }
          }
          case _ => throw new Exception("Invalid join conditions!")
        }
    })
  }
}
