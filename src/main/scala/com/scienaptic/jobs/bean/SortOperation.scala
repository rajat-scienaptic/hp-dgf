package com.scienaptic.jobs.bean

import com.fasterxml.jackson.annotation.JsonProperty
import com.scienaptic.jobs.utility.Utils
import org.apache.spark.sql.DataFrame

import scala.util.Try

class SortOperation(@JsonProperty("asc") asc: List[String], @JsonProperty("desc") desc: List[String]) extends Operation {
  override def scienapticDef() = {
    println("Sorting initiated")
  }
}

object SortOperation {
  def doSort(dataFrame: DataFrame, ascending: List[String], descending: List[String]): Try[DataFrame] = {
    Try {
      dataFrame.orderBy(Utils.convertListToDFColumn(ascending, dataFrame): _*)
        .orderBy(convertListToDFColumnDesc(descending, dataFrame): _*)
    }
  }

  private def convertListToDFColumnDesc(columnList: List[String], dataFrame: DataFrame) = {
    columnList.map(name => dataFrame.col(name).desc /*.as(s"renamed_$name")*/)
  }

}

//df.orderBy(desc("columnname1"),desc("columnname2"),"columnname3")
//.orderBy(col(top_value).desc)