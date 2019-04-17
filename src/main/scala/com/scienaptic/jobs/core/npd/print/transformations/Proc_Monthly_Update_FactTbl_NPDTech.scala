package com.scienaptic.jobs.core.npd.print.transformations

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.NPDUtility.exportToHive
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

class Proc_Monthly_Update_FactTbl_NPDTech {


  def execute(executionContext: ExecutionContext): Unit = {
    val spark: SparkSession = executionContext.spark

    val sourceMap = executionContext.configuration.sources

    var stgtable =spark.sql("select * from ams_datamart_print.stgtable")

    var Tbl_Master_NPD_Tech =spark.sql("select Sub_Category,Custom_Grouped_Max_Page_Width,NPD_Tech from ams_datamart_print.Tbl_Master_NPD_Tech")
    Tbl_Master_NPD_Tech=Tbl_Master_NPD_Tech.withColumnRenamed("Sub_Category","SubCategory")
      .withColumnRenamed("Custom_Grouped_Max_Page_Width","CustomGroupedMaxPageWidth")
      .withColumnRenamed("NPD_Tech","NPDTech")


    var TmpFctTable=stgtable.join(Tbl_Master_NPD_Tech,
      stgtable("Sub_Category")===Tbl_Master_NPD_Tech("SubCategory") and
        stgtable("Custom_Grouped_Max_Page_Width")===Tbl_Master_NPD_Tech("CustomGroupedMaxPageWidth")
      ,"left")



    TmpFctTable=TmpFctTable.withColumn("NPD_Tech",when(TmpFctTable("NPDTech").isNotNull,
      TmpFctTable("NPDTech")).otherwise(lit(null).cast(StringType)))

    TmpFctTable=TmpFctTable.drop("SubCategory","CustomGroupedMaxPageWidth","NPDTech")

    exportToHive(TmpFctTable,"","stgtable_temp","ams_datamart_print",executionContext)



    val AMS_Commercial_Retail_udf=udf((value:String)=>{
      value match {
        case "Retail ALR" => "Retail"
        case "Retail" => "Retail"
        case "CA Retail" => "Retail"
        case "CA Retail ALR" => "Retail"
        case _=>"Commercial"
      }
    })

    var TmpFctTable2=stgtable.withColumn("AMS_Commercial_Retail",AMS_Commercial_Retail_udf(stgtable("channel")))

    exportToHive(TmpFctTable2,"","stgtable_temp","ams_datamart_print",executionContext)






  }
}
