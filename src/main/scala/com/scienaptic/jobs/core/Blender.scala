package com.scienaptic.jobs.core

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.utility.Utils._
import org.apache.spark.sql.{DataFrame, SaveMode}

/*
Blender : Responsible for merging ORCA's historic and daily file into one file.
 */
object Blender {
  def execute(executionContext: ExecutionContext): Unit = {

    val sourceMap = executionContext.configuration.sources

    for ((sourceName, sourceDefinition) <- sourceMap) {
      val historicSource = loadCSV(executionContext, sourceDefinition.historicFilePath).get
      val orcaNewSource = loadCSV(executionContext, sourceDefinition.orcaNewFilePath).get

      historicSource.unionByName(orcaNewSource).distinct()
        .coalesce(1)
        .write.mode(SaveMode.Overwrite)
        .option("header", true)
        .csv(sourceDefinition.orcaNewFilePath.replace("_new",""))
    }
  }
}
