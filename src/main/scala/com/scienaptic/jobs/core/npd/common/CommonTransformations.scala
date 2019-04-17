package com.scienaptic.jobs.core.npd.common

import org.apache.spark.sql.DataFrame

object CommonTransformations {

  def withCleanHeaders(df :DataFrame)  = {

    df.toDF(df
      .schema
      .fieldNames
      .map(name => "[ ,;{}.\\n\\t=]+".r.replaceAllIn(name, "_").toLowerCase): _*)

  }
}
