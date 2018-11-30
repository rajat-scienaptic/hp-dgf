package com.scienaptic.jobs.bean

import org.apache.spark.sql.DataFrame

import scala.util.Try

class UnionOperation extends Operation {
  override def scienapticDef(): Any = {
    println("Union operation initialized")
  }
}

object UnionOperation {
  def doUnion(dataFrame: DataFrame, dataFrame2: DataFrame): Try[DataFrame] = {
    Try {
      dataFrame.union(dataFrame2)
    }
  }
}
