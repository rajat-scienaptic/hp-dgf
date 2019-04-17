package com.scienaptic.jobs.utility

import java.util.Date
import com.scienaptic.jobs.ExecutionContext

object NPDUtility {

  def load_csv_to_table(executionContext: ExecutionContext,path:String,tablename:String):Unit={

    val spark = executionContext.spark
    var df=spark.read.option("escape","\"")
      .option("inferSchema", true)
      .option("ignoreLeadingWhiteSpace","false").option("ignoreTrailingWhiteSpace","false")
      .option("header", "true").csv(path)

    val esc="[ ,-]"
    df=df.toDF(df.columns.map(x=>x.replaceAll(esc,"_")):_*)
    df.write.mode(org.apache.spark.sql.SaveMode.Overwrite).saveAsTable("ams_datamart_print.stgtbl_"+s"$tablename")

  }


}
