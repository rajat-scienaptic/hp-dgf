package com.scienaptic.jobs.core.npd.print.transformations

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.NPDUtility.exportToHive
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, when}
import org.apache.spark.sql.types.StringType

class Proc_Monthly_Update_FactTblVendor {


  def execute(executionContext: ExecutionContext): Unit = {
    val spark: SparkSession = executionContext.spark

    val sourceMap = executionContext.configuration.sources

    //val Update_stgtable_ASP = sourceMap(Update_stgtable_ASP_SOURCE)
    //val stgtablename=Update_stgtable_ASP.tablename
    var stgtable =spark.sql("select * from ams_datamart_print.stgtable")
    var TblMaster_MSRP =spark.sql("select ModelNPD,Item_Description from ams_datamart_print.TblMaster_MSRP")



    var TmpFctTable=stgtable.join(TblMaster_MSRP,
      stgtable("MODDESC")===TblMaster_MSRP("Item_Description"),"left")



    TmpFctTable=TmpFctTable.withColumn("AMS_NPD_Model",when(TmpFctTable("ModelNPD").isNotNull,
      TmpFctTable("ModelNPD")).otherwise(lit(null).cast(StringType)))
       .withColumn("AMS_Concate_Qtr_Year_NPDModel",TmpFctTable("AMS_Year_Quarter")+TmpFctTable("AMS_NPD_Model"))
      .withColumn("AMS_HP_Non_HP",lit("Non HP").cast(StringType))


    var Tbl_Master_HP_NonHP =spark.sql("select Brand,VendorFamily from ams_datamart_print.Tbl_Master_HP_NonHP")

    TmpFctTable=stgtable.join(Tbl_Master_HP_NonHP,
      stgtable("BRAND")===Tbl_Master_HP_NonHP("Brand"),"left")

    TmpFctTable=TmpFctTable.withColumn("AMS_HP_Non_HP",when(TmpFctTable("ModelNPD").isNotNull,
      TmpFctTable("VendorFamily")).otherwise(lit(null).cast(StringType)))

    TmpFctTable=TmpFctTable.drop("Brand","VendorFamily","ModelNPD","Item_Description")


    //exportToHive(stgtable,"",stgtablename,"ams_datamart_print",executionContext)
    exportToHive(stgtable,"","stgtable_temp","ams_datamart_print",executionContext)

  }


}
