package com.scienaptic.jobs.core.npd.print.transformations

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.NPDUtility.exportToHive
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.{IntegerType, StringType}

class Proc_Monthly_Update_FactTbl_Technology_Category {

  def execute(executionContext: ExecutionContext): Unit = {
    val spark: SparkSession = executionContext.spark

    val sourceMap = executionContext.configuration.sources

    //val Update_FactTable_ASP = sourceMap(Update_FactTable_ASP_SOURCE)
    //val facttablename=Update_FactTable_ASP.tablename
    var facttable =spark.sql("select * from ams_datamart_print.facttable")
    var Tbl_Master_Category =spark.sql("select Category,SFP_MFP,Sub_Category from ams_datamart_print.Tbl_Master_Category")
    Tbl_Master_Category=Tbl_Master_Category.withColumnRenamed("Sub_Category","SubCategory")


    var TmpFctTable=facttable.join(Tbl_Master_Category,
      facttable("Sub_Category")===Tbl_Master_Category("SubCategory"),"left")



    TmpFctTable=TmpFctTable.withColumn("AMS_Technology",when(TmpFctTable("Category").isNotNull,
      TmpFctTable("Category")).otherwise(lit(null).cast(StringType)))
      .withColumn("AMS_SFP_MFP",when(TmpFctTable("SFP_MFP").isNotNull,
        TmpFctTable("SFP_MFP")).otherwise(lit(null).cast(StringType)))



    TmpFctTable=TmpFctTable.drop("Category","SFP_MFP","SubCategory")


    //exportToHive(facttable,"",facttablename,"ams_datamart_print",executionContext)
    exportToHive(facttable,"","facttable_temp","ams_datamart_print",executionContext)

  }

}


