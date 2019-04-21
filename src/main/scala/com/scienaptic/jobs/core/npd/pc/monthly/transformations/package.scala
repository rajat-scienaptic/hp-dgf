package com.scienaptic.jobs.core.npd.pc.monthly

import org.apache.spark.sql.functions.udf

package object transformations {

  val currentPrior = (num : Int)  => {

    if(num >= 13 && num <=24){
      "Prior"
    }
    else if( num >= 1 && num <= 12){
      "Current"
    } else {
      "-"
    }

  }

  val qtdCurrentPrior = (num : Int,fiscal : String)  => {

    val qtr = fiscal.substring(4,6);

    if(num >= 13 && num <=24){
      qtr+" Prior"
    }
    else if( num >= 1 && num <= 12){
      qtr+" Current"
    } else {
      "-"
    }

  }

  def currentPriorUDF = udf(currentPrior)
  def qtdCurrentPriorUDF = udf(qtdCurrentPrior)


}
