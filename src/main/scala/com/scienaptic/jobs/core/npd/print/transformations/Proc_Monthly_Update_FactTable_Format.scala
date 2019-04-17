package com.scienaptic.jobs.core.Print_jobs
import org.apache.spark.sql.functions._
import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.NPDUtility._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

class Proc_Monthly_Update_stgtable_Format {


  def execute(executionContext: ExecutionContext): Unit = {
    val spark: SparkSession = executionContext.spark

    val sourceMap = executionContext.configuration.sources


    var stgtable =spark.sql("select * from ams_datamart_print.stgtable")


    //var stgtable =spark.sql("select * from ams_datamart_print."+Monthly_Update_stgtable_Format.tablename)

    val AMS_Format_udf=udf((value:String)=>{
      value match {
        case "A" => "All Other (Suppressed)"
        case "N" => "All Other (Suppressed)"
        case "8" => "A4"
        case _=>"A3"
      }
    })

    stgtable=stgtable.withColumn("AMS_Format",AMS_Format_udf(expr("substring(Maximum_Page_Width, 1, 1")))

    //exportToHive(stgtable,"","stgtable_temp","ams_datamart_print",executionContext)

  }

}
