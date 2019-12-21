package com.scienaptic.jobs.bean

import org.apache.spark.sql.DataFrame
import com.scienaptic.jobs.utility.Utils
import org.apache.spark.sql.Column
import scala.util.Try
import org.apache.spark.sql.functions._


class UnionOperation extends Operation {
  override def scienapticDef(): Any = {
    println("Union operation initialized")
  }
}

object UnionOperation {
  def doUnion(dataFrame: DataFrame, dataFrame2: DataFrame): Try[DataFrame] = {
    Try {
      val df1Columns = dataFrame.columns toSet
      val df2Columns = dataFrame2.columns toSet
      /*if (df1Columns.size != df2Columns.size) {
        //TODO: Throw Log warning or exception
      }*/
      val columnsUnion = df1Columns union(df2Columns)
      dataFrame.select(expr(df1Columns, columnsUnion):_*).union(dataFrame2.select(expr(df2Columns, columnsUnion):_*))
    }
  }

  def expr(myCols: Set[String], allCols: Set[String]) = {
    allCols.toList.map(x => x match {
      case x if myCols.contains(x) => col(x)
      case _ => lit(null).as(x)
    })
  }
}
