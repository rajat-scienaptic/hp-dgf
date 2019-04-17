package com.scienaptic.jobs.utility

import com.scienaptic.jobs.ExecutionContext
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.scienaptic.jobs.core.npd.common.CommonTransformations._

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

    val sparkTableName = dataMart+"."+tableName+"_Spark"

    df.createOrReplaceTempView(tableName)

    spark.sql("drop table if exists "+sparkTableName)

    spark.sql("create table "+sparkTableName+" STORED AS ORC AS select * from "+tableName)

    logger.info("Creating hive table : "+sparkTableName)

  }

  def writeToDebugTable(spark: SparkSession,df : DataFrame,tableName : String) = {

    val sparkTableName = "npd_sandbox.debug_"+tableName

    df.createOrReplaceTempView(tableName)

    spark.sql("drop table if exists "+sparkTableName)

    spark.sql("create table "+sparkTableName+" STORED AS ORC AS select * from "+tableName)

    logger.info("Creating hive table : "+sparkTableName)

  }


}
