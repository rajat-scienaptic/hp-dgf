package com.scienaptic.jobs.core.npd.print.transformations

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.NPDUtility.exportToHive
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

class Proc_Update_FactTblSegmentation {
  def execute(executionContext: ExecutionContext): Unit = {
    val spark: SparkSession = executionContext.spark

    val sourceMap = executionContext.configuration.sources

    var facttable =spark.sql("select * from ams_datamart_print.facttable")

    var TblMaster_Segmentation =spark.sql(
      """select Hp_Market_Category,Hp_Market_Detail_Ops_Hps ,
        Hp_Market_Focus_Ops_Hps,Hp_Market_Function_Ops_Hps,Hp_Market_Group,
         Hp_Product_Structure_Ops_Hps,Hp_Gbu_Ops_Hps,HP_A3_Segment,Hp_Cis,Hp_Targeted_In_Current_Fy,MODDESC from
         ams_datamart_print.TblMaster_Segmentation""")



    var TmpFctTable=facttable.join(TblMaster_Segmentation,
      facttable("Item_Description")===TblMaster_Segmentation("MODDESC"),"left")



    TmpFctTable=TmpFctTable.withColumn("AMS_HP_Market_Category",when(TmpFctTable("Hp_Market_Category").isNotNull,
      TmpFctTable("Hp_Market_Category")).otherwise(lit(null).cast(StringType)))
      .withColumn("AMS_HP_Market_Detail_Ops_Hps",when(TmpFctTable("Hp_Market_Detail_Ops_Hps").isNotNull,
      TmpFctTable("Hp_Market_Detail_Ops_Hps")).otherwise(lit(null).cast(StringType)))
      .withColumn("AMS_HP_Market_Focus_Ops_Hps",when(TmpFctTable("Hp_Market_Focus_Ops_Hps").isNotNull,
        TmpFctTable("Hp_Market_Focus_Ops_Hps")).otherwise(lit(null).cast(StringType)))
      .withColumn("AMS_HP_Market_Function_Ops_Hps",when(TmpFctTable("Hp_Market_Function_Ops_Hps").isNotNull,
        TmpFctTable("Hp_Market_Function_Ops_Hps")).otherwise(lit(null).cast(StringType)))
      .withColumn("AMS_HP_Market_Group",when(TmpFctTable("Hp_Market_Group").isNotNull,
        TmpFctTable("Hp_Market_Group")).otherwise(lit(null).cast(StringType)))
      .withColumn("AMS_HP_Product_Structure_Ops_Hps",when(TmpFctTable("Hp_Product_Structure_Ops_Hps").isNotNull,
        TmpFctTable("Hp_Product_Structure_Ops_Hps")).otherwise(lit(null).cast(StringType)))
      .withColumn("AMS_HP_Gbu_Ops_Hps",when(TmpFctTable("Hp_Gbu_Ops_Hps").isNotNull,
        TmpFctTable("Hp_Gbu_Ops_Hps")).otherwise(lit(null).cast(StringType)))
      .withColumn("AMS_HP_A3_Segment",when(TmpFctTable("HP_A3_Segment").isNotNull,
        TmpFctTable("HP_A3_Segment")).otherwise(lit(null).cast(StringType)))
      .withColumn("AMS_HP_CIS",when(TmpFctTable("Hp_Cis").isNotNull,
        TmpFctTable("Hp_Cis")).otherwise(lit(null).cast(StringType)))
      .withColumn("AMS_HP_Targeted_In_Current_FY",when(TmpFctTable("Hp_Targeted_In_Current_Fy").isNotNull,
        TmpFctTable("Hp_Targeted_In_Current_Fy")).otherwise(lit(null).cast(StringType)))


    val NPDTech="Instant"
    val NPDTechValue="Instant Print"



    TmpFctTable=TmpFctTable.withColumn("AMS_HP_Market_Category",when(col("NPD_Tech")===NPDTech,NPDTechValue).otherwise(col("AMS_HP_Market_Category")))
      .withColumn("AMS_HP_Market_Detail_Ops_Hps",when(col("NPD_Tech")===NPDTech,NPDTechValue).otherwise(col("AMS_HP_Market_Detail_Ops_Hps")))
      .withColumn("AMS_HP_Market_Focus_Ops_Hps",when(col("NPD_Tech")===NPDTech,NPDTechValue).otherwise(col("AMS_HP_Market_Focus_Ops_Hps")))
      .withColumn("AMS_HP_Market_Function_Ops_Hps",when(col("NPD_Tech")===NPDTech,NPDTechValue).otherwise(col("AMS_HP_Market_Function_Ops_Hps")))
      .withColumn("AMS_HP_Market_Group",when(col("NPD_Tech")===NPDTech,NPDTechValue).otherwise(col("AMS_HP_Market_Group")))
      .withColumn("AMS_HP_Product_Structure_Ops_Hps",when(col("NPD_Tech")===NPDTech,NPDTechValue).otherwise(col("AMS_HP_Product_Structure_Ops_Hps")))
      .withColumn("AMS_HP_Gbu_Ops_Hps",when(col("NPD_Tech")===NPDTech,NPDTechValue).otherwise(col("AMS_HP_Gbu_Ops_Hps")))
      .withColumn("AMS_HP_A3_Segment",when(col("NPD_Tech")===NPDTech,NPDTechValue).otherwise(col("AMS_HP_A3_Segment")))
      .withColumn("AMS_HP_CIS",when(col("NPD_Tech")===NPDTech,NPDTechValue).otherwise(col("AMS_HP_CIS")))
      .withColumn("AMS_HP_Targeted_In_Current_FY",when(col("NPD_Tech")===NPDTech,NPDTechValue).otherwise(col("AMS_HP_Targeted_In_Current_FY")))



    val NPDTech2="LargeFormat"
    val NPDTechValue2="LFP"

    TmpFctTable=TmpFctTable.withColumn("AMS_HP_Market_Category",when(col("NPD_Tech")===NPDTech2,NPDTechValue2).otherwise(col("AMS_HP_Market_Category")))
      .withColumn("AMS_HP_Market_Detail_Ops_Hps",when(col("NPD_Tech")===NPDTech2,NPDTechValue2).otherwise(col("AMS_HP_Market_Detail_Ops_Hps")))
      .withColumn("AMS_HP_Market_Focus_Ops_Hps",when(col("NPD_Tech")===NPDTech2,NPDTechValue2).otherwise(col("AMS_HP_Market_Focus_Ops_Hps")))
      .withColumn("AMS_HP_Market_Function_Ops_Hps",when(col("NPD_Tech")===NPDTech2,NPDTechValue2).otherwise(col("AMS_HP_Market_Function_Ops_Hps")))
      .withColumn("AMS_HP_Market_Group",when(col("NPD_Tech")===NPDTech2,NPDTechValue2).otherwise(col("AMS_HP_Market_Group")))
      .withColumn("AMS_HP_Product_Structure_Ops_Hps",when(col("NPD_Tech")===NPDTech2,NPDTechValue2).otherwise(col("AMS_HP_Product_Structure_Ops_Hps")))
      .withColumn("AMS_HP_Gbu_Ops_Hps",when(col("NPD_Tech")===NPDTech2,NPDTechValue2).otherwise(col("AMS_HP_Gbu_Ops_Hps")))
      .withColumn("AMS_HP_A3_Segment",when(col("NPD_Tech")===NPDTech2,NPDTechValue2).otherwise(col("AMS_HP_A3_Segment")))
      .withColumn("AMS_HP_CIS",when(col("NPD_Tech")===NPDTech2,NPDTechValue2).otherwise(col("AMS_HP_CIS")))
      .withColumn("AMS_HP_Targeted_In_Current_FY",when(col("NPD_Tech")===NPDTech2,NPDTechValue2).otherwise(col("AMS_HP_Targeted_In_Current_FY")))


    var Tbl_Master_HP_NonHP =spark.sql("select VendorFamily,BRAND from ams_datamart_print.Tbl_Master_HP_NonHP")
    Tbl_Master_HP_NonHP=Tbl_Master_HP_NonHP.withColumnRenamed("BRAND","Brand_1")

     TmpFctTable=TmpFctTable.join(Tbl_Master_HP_NonHP,
       TmpFctTable("Brand")===Tbl_Master_HP_NonHP("Brand_1"),"left")


    TmpFctTable=TmpFctTable.withColumn("AMS_HP_Non_HP_GBU",concat(col("VendorFamily"),col("AMS_Hp Gbu Ops/Hps")))

    TmpFctTable=TmpFctTable.drop("VendorFamily","BRAND","CA_Exchange_Rate","Hp_Market_Category","Hp_Market_Detail_Ops_Hps" ,
      "Hp_Market_Focus_Ops_Hps","Hp_Market_Function_Ops_Hps","Hp_Market_Group",
      "Hp_Product_Structure_Ops_Hps","Hp_Gbu_Ops_Hps,HP_A3_Segment","Hp_Cis,Hp_Targeted_In_Current_Fy","MODDESC")

    exportToHive(TmpFctTable,"","facttable_temp","ams_datamart_print",executionContext)

  }
}
