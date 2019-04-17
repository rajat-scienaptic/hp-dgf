package com.scienaptic.jobs.core.npd.print.transformations

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.NPDUtility.exportToHive
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

class Proc_Update_FactTblColorMono {


  def execute(executionContext: ExecutionContext): Unit = {
    val spark: SparkSession = executionContext.spark

    val sourceMap = executionContext.configuration.sources

    var facttable =spark.sql("select * from ams_datamart_print.facttable")
 var Tbl_Master_Mono_Color =spark.sql("select Mono_Color,Mono_Color_Group from ams_datamart_print.Tbl_Master_Mono_Color")

    var TmpFctTable=facttable.join(Tbl_Master_Mono_Color,
      facttable("Color_Mono_Printing")===Tbl_Master_Mono_Color("Mono_Color_Group"),"left")



    TmpFctTable=TmpFctTable.withColumn("AMS_Color_Mono",when(TmpFctTable("Mono_Color").isNotNull,
      TmpFctTable("Mono_Color")).otherwise(lit(null).cast(StringType)))

    TmpFctTable=TmpFctTable.drop("Mono_Color","Mono_Color_Group")

    exportToHive(TmpFctTable,"","stgtable","ams_datamart_print",executionContext)

  }
}
