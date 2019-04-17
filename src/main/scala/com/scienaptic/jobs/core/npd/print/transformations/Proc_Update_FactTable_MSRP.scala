package com.scienaptic.jobs.core.npd.print.transformations

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.NPDUtility.exportToHive
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}

class Proc_Update_FactTable_MSRP {

  def execute(executionContext: ExecutionContext): Unit = {
    val spark: SparkSession = executionContext.spark

    val sourceMap = executionContext.configuration.sources


    var facttable =spark.sql("select * from ams_datamart_print.facttable")

    var TblMaster_MSRP =spark.sql("select PDDATE,Set_price,MODDESC,intro_year from ams_datamart_print.TblMaster_MSRP")

    var TmpFctTable=facttable.join(TblMaster_MSRP,
      facttable("Item_Description")===TblMaster_MSRP("MODDESC"),"left")

    TmpFctTable=TmpFctTable.withColumn("AMS_Temp_MSRP",
      when((col("PDDATE").isNull)||(col("PDDATE")===lit(0).cast(IntegerType)),col("Set_price"))
        .otherwise(when(col("PDDATE")>col("AMS_Mth_Yr"),col("Set_price")).otherwise(col("Pdrop"))))


    TmpFctTable=TmpFctTable.withColumn("AMS_MSRP",
      when((col("intro_year")-col("AMS_Year")>lit(2).cast(IntegerType)),col("AMS_ASP"))
        .otherwise(when(col("AMS_Temp_MSRP").isNull,col("AMS_ASP")).otherwise(
          when(col("AMS_Temp_MSRP")<col("AMS_ASP"),col("AMS_ASP")).otherwise(col("AMS_Temp_MSRP"))
        )))

    TmpFctTable=TmpFctTable.withColumn("AMS_MSRP_Dollars",col("Units")*col("AMS_MSRP"))


    TmpFctTable=TmpFctTable.drop("PDDATE","Set_price","MODDESC","intro_year")

    exportToHive(TmpFctTable,"","tmp_","ams_datamart_print",executionContext)

  }


}
