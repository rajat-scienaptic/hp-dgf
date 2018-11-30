package com.scienaptic.jobs.core

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.bean._
import com.scienaptic.jobs.utility.Utils

object RetailTransform {

  def execute(executionContext: ExecutionContext): Unit = {

    val sourceMap = executionContext.configuration.sources

    print("Source Name " + sourceMap("aux").name)
    //TODO: Check if argument lists empty. Don't call utility if empty!
    //TODO: Implement Logger
    //TODO: Implement Custom Exceptions

    // Load
    val dfAux = Utils.loadCSV(executionContext, sourceMap("aux").filePath)

    // Select01
    val selectCols = sourceMap("aux").selectOperation("select01")
    val selectDF = SelectOperation.doSelect(dfAux.get, selectCols.cols).get
    selectDF.show()

    // Join01
    val joins = sourceMap("aux").joinOperation
    val joinDF = JoinAndSelectOperation.doJoinAndSelect(selectDF, selectDF, joins("join01"))

    // filter01
    val filterOperation = sourceMap("aux").filterOperation("filter01")
    val filterDF = FilterOperation.doFilter(joinDF, filterOperation.conditions, filterOperation.conditionTypes(0))

    val sortOperation = sourceMap("aux").sortOperation("sort01")
    val sortedDF = SortOperation.doSort(filterDF, sortOperation.ascending, sortOperation.descending).get

    val _ = UnionOperation.doUnion(sortedDF, filterDF).get
  }

}

