package com.scienaptic.jobs.core

import java.util.Date

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.bean._
import com.scienaptic.jobs.utility.Utils
import org.apache.spark.sql.functions.udf

object CommercialTransform {

  val SELECT01="select01"; val SELECT02="select02"; val SELECT03="select03"; val SELECT04="select04"; val SELECT05="select05"; val SELECT06="select06";
  val FILTER01="filter01"; val FILTER02="filter02"; val FILTER03="filter03"; val FILTER04="filter04"; val FILTER05="filter05"; val FILTER06="filter06";
  val JOIN01="join01"; val JOIN02="join02"; val JOIN03="join03"; val JOIN04="join04"; val JOIN05="join05"; val JOIN06="join06"; val JOIN07="join07";
  val NUMERAL0=0; val NUMERAL1=1;
  val IECSOURCE="IEC_CLAIMS"; val XSCLAIMSSOURCE="XS_CLAIMS";
  val GROUP01

  def createBaseSKUFromProductID(productID: String): String ={
    //TODO: IF Left(Right([Product ID], 4),1)="#" THEN Left([Product ID],Length([Product ID])-4) ELSE [Product ID] ENDIF
    "Sample"
  }
  val createBaseSKUFromProductIDUDF = udf[String, String](createBaseSKUFromProductID)

  def extractWeekFromDate(dateStr: String): Int = {
    //TODO: DateTimeFormat([Partner Ship Calendar Date],"%w")
    64
  }
  val extractWeekFromDateUDF = udf[String, Int](extractWeekFromDate)

  def addDaystoDateString(dateString: String, days: Int): Date = {
    //TODO: DateTimeAdd([Partner Ship Calendar Date],6-[Temp Date Calc],"days")
    new Date()
  }
  val addDaystoDateStringUDF = udf[String, Int, Date](addDaystoDateString)

  def baseSKUFormula(basesku: String): String= {
    basesku match {
      case "M9L74A" => "M9L75A"
      case "J9V91A" => "J9V90A"
      case "J9V92A" => "J9V90A"
      case _ => basesku
    }
  }
  val baseSKUFormulaUDF = udf[String, String](baseSKUFormula)



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

    /* Union XS Claims and IEC Claims DF TODO: Check if both Dataframes have same number of columns*/
    val xsClaimsUnionDF = UnionOperation.doUnion(iecFiltered01DF.get, xsClaimsSelect01DF).get

    val baseSKUproductDF = xsClaimsUnionDF.withColumn("Base SKU",createBaseSKUFromProductIDUDF($"Product ID"))
    val tempDateCalDF = baseSKUproductDF.withColumn("Temp Date Calc String",extractWeekFromDateUDF($"Partner Ship Calendar Date", "week"))
    val weekEndDateDF = tempDateCalDF.withColumn("Week End Date",addDaystoDateStringUDF($"Partner Ship Calendar Date", "day"))
    val xsClaimsBaseSKUDF = weekEndDateDF.withColumn("Base SKU",baseSKUFormulaUDF($"Base SKU"))

    //127
    val xsClaimsGroup01 = xsClaimsSource.groupOperation("group01")
    val xsClaimsGroupedClaimQuanAggDF = xsClaimsBaseSKUDF.groupBy() //TODO
      .sum("Claim Quantity")
      .withColumnRenamed("Claim Quantity","Sum_Claim Quantity")

    //172
    val xsClaimsGroupedSumClaimQuanAggDF = xsClaimsGroupedClaimQuanAggDF.groupBy() //TODO
      .sum("Sum_Claim Quantity")
      .withColumnRenamed("Sum_Claim Quantity","Claim Quantity")








  }
}
