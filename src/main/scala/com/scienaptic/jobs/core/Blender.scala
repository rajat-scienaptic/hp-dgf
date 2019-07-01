package com.scienaptic.jobs.core

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.Utils._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.col
import com.scienaptic.jobs.bean.UnionOperation.doUnion

/*
Blender : Responsible for merging ORCA's historic and daily file into one file.
 */
object Blender {
  def execute(executionContext: ExecutionContext): Unit = {

    val sourceMap = executionContext.configuration.sources

    for ((sourceName, sourceDefinition) <- sourceMap) {
      val historicSource = loadCSV(executionContext, sourceDefinition.historicFilePath).get
      var orcaNewSource = loadCSV(executionContext, sourceDefinition.orcaNewFilePath).get
      val nameSource = sourceDefinition.name
      
      nameSource match {
        case "ODOM_ONLINE_ORCA" => orcaNewSource = orcaNewSource.where(col("Week")>="2019W01")
        case "HP_COM" => orcaNewSource = orcaNewSource    //TODO: Dont have source from Fabio yet to compare
        case "ORCA_QRY_2017_TO_DATE" => orcaNewSource = orcaNewSource.where(col("Week")>"2019W02")
        case "WALMART" => orcaNewSource = orcaNewSource.where(col("Week")>"2019W23")
        case "CI" => orcaNewSource = orcaNewSource.where(col("Quarter")>"2018Q02")
        case "ST" => orcaNewSource = orcaNewSource.where(col("Quarter")>"2019Q02")
        case "Walmart_Pos_Qty" => orcaNewSource = orcaNewSource   //Only Historic was faulty. So replaced Carlos' feed with Fabio's file.
        case _ => orcaNewSource = orcaNewSource
      }

      nameSource match {
        case "ORCA_QRY_2017_TO_DATE" => {
          doUnion(historicSource,orcaNewSource).get.distinct()
            .coalesce(1)
            .write.mode(SaveMode.Overwrite)
            .option("header", true)
            .csv(sourceDefinition.orcaNewFilePath.replace("_new",""))
        }
        case "ST" => {
          doUnion(historicSource,orcaNewSource).get.distinct()
            .coalesce(1)
            .write.mode(SaveMode.Overwrite)
            .option("header", true)
            .csv(sourceDefinition.orcaNewFilePath.replace("_new",""))
        }
        case _ => {
          historicSource.unionByName(orcaNewSource).distinct()
            .coalesce(1)
            .write.mode(SaveMode.Overwrite)
            .option("header", true)
            .csv(sourceDefinition.orcaNewFilePath.replace("_new",""))

        }
      }
    }
  }
}
