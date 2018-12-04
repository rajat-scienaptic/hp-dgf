package com.scienaptic.jobs.core

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.bean._
import com.scienaptic.jobs.utility.Utils

object CommercialTransform {

  val (SELECT01,SELECT02,SELECT03,SELECT04,SELECT05,SELECT06) = ("select01","select02","select03","select04","select05","select06")
  val (FILTER01,FILTER02,FILTER03,FILTER04,FILTER05,FILTER06) = ("filter01","filter02","filter03","filter04","filter05","filter06")
  val (JOIN01,JOIN02,JOIN03,JOIN04,JOIN05,JOIN06,JOIN07) = ("join01","join02","join03","join04","join05","join06","join07")
  val (NUMERAL0, NUMERAL1) = (0,1)
  val (IECSOURCE, XSCLAIMSSOURCE) = ("iec_claims","xs_claims")

  def execute(executionContext: ExecutionContext): Unit = {

    /*val joins = sourceMap("aux").joinOperation
    val joinDF = JoinAndSelectOperation.doJoinAndSelect(selectDF, selectDF, joins("join01"))
    val sortOperation = sourceMap("aux").sortOperation("sort01")
    val sortedDF = SortOperation.doSort(filterDF, sortOperation.ascending, sortOperation.descending).get
    val _ = UnionOperation.doUnion(sortedDF, filterDF).get
    */

    /* Source Map with all sources' information */
    val sourceMap = executionContext.configuration.sources

    /* Map with all operations for IEC_Claims source operations */
    val iecSource = sourceMap(IECSOURCE)
    val xsClaimsSource = sourceMap(XSCLAIMSSOURCE)

    /* Load all sources */
    val iecDF = Utils.loadCSV(executionContext, iecSource.filePath)
    val xsClaimsDF = Utils.loadCSV(executionContext, xsClaimsSource.filePath)

    val iecSelect01 = iecSource.selectOperation(SELECT01)
    val iecSelectDF = SelectOperation.doSelect(iecDF.get, iecSelect01.cols).get

    val xsClaimFilter01 = iecSource.filterOperation(FILTER01)
    val iecFiltered01DF = FilterOperation.doFilter(iecSelectDF, xsClaimFilter01.conditions, xsClaimFilter01.conditionTypes(NUMERAL0))

    val xsClaimsSelect01 = xsClaimsSource.selectOperation(SELECT01)
    val xsClaimsSelect01DF = SelectOperation.doSelect(iecFiltered01DF.get, xsClaimsSelect01.cols).get

    val xsClaimsUnionDF = UnionOperation.doUnion(iecFiltered01DF.get, xsClaimsSelect01DF)








  }
}
