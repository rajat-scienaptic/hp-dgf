package com.scienaptic.jobs.utility

import com.scienaptic.jobs.ExecutionContext
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.scienaptic.jobs.core.npd.common.CommonTransformations._
import org.apache.spark.sql.functions._

object NPDUtility {

  val logger = Logger.getLogger(this.getClass.getName)

  def load_csv_to_table(executionContext: ExecutionContext,path:String,datamart:String,tablename:String):Unit={

    val spark = executionContext.spark
    val df = spark.read.option("escape","\"")
      .option("inferSchema", true)
      .option("ignoreLeadingWhiteSpace","false").option("ignoreTrailingWhiteSpace","false")
      .option("header", "true").csv(path)
    df
      .transform(withCleanHeaders)
      .write.mode(org.apache.spark.sql.SaveMode.Overwrite)
      .saveAsTable(datamart+"."+tablename)

  }


  def writeToDataMart(spark: SparkSession,df : DataFrame,dataMart : String,tableName : String) = {

    val sparkTableName = dataMart+"."+tableName

    val tempViewName = tableName+"_tempview"

    df.createOrReplaceTempView(tempViewName)

    spark.sql("drop table if exists "+sparkTableName)

    spark.sql("create table "+sparkTableName+" STORED AS ORC AS select * from "+tempViewName)

    logger.info("Creating hive table : "+sparkTableName)

  }

  def writeToDebugTable(spark: SparkSession,df : DataFrame,tableName : String) = {

    val sparkTableName = "npd_sandbox.debug_"+tableName

    df.createOrReplaceTempView(tableName)

    spark.sql("drop table if exists "+sparkTableName)

    spark.sql("create table "+sparkTableName+" STORED AS ORC AS select * from "+tableName)

    logger.info("Creating hive table : "+sparkTableName)

  }


  def exportToHive(df: DataFrame, partitionColumn: String,
                   outTable: String, hiveDB: String, executionContext: ExecutionContext):
  Unit = {


    var dfWriter=df.write.mode(org.apache.spark.sql.SaveMode.Overwrite)

    if (!partitionColumn.isEmpty)
      dfWriter = dfWriter.partitionBy(partitionColumn)

    dfWriter.saveAsTable(s"$hiveDB" + "." + outTable)

  }

  def updateCol(df1:DataFrame,col_to_be_updated:String,col2:String):DataFrame={

    var df = df1.withColumn(col_to_be_updated, when(df1(col2).isNotNull,
      df1(col2)).otherwise(df1(col_to_be_updated)))

    df
  }

}