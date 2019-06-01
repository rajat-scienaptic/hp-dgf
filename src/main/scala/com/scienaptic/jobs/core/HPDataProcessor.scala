package com.scienaptic.jobs.core

import com.scienaptic.jobs.ExecutionContext
import com.scienaptic.jobs.core.gap._
import com.scienaptic.jobs.core.npd.pc.monthly.staging.{CAMonthlyStaging, USMonthlyStaging}
import com.scienaptic.jobs.core.npd.pc.monthly.transformations.{CATransformer, USTransformer}
import com.scienaptic.jobs.core.npd.pc.weekly.stagging.{CAWeeklyStaging, USWeeklyStaging}
import com.scienaptic.jobs.core.npd.pc.weekly.transformations.{CAWeeklyTransformer, USWeeklyTransformer}
import com.scienaptic.jobs.core.npd.print._
import com.scienaptic.jobs.core.pricing.amazon.AmazonTransform
import com.scienaptic.jobs.core.pricing.commercial._
import com.scienaptic.jobs.core.pricing.retail._

object HPDataProcessor {
  def execute(executionContext: ExecutionContext): Unit = {
    executionContext.configuration.config match {
      case "reporting-NEW" => ExcelDQValidation.execute(executionContext)
      case "reporting" => RCodeDQValidation.execute(executionContext)
      case "HP-orca-merge" => Blender.execute(executionContext)
      case "retail-ORCA_MAIN_UNION" => RetailTransform.execute(executionContext)
      case "retail-DISTRIBUTION_INV" => RetailTransform2.execute(executionContext)
      case "retail-WALMART" => RetailTransform3.execute(executionContext)
      case "commercial" => CommercialSimplifiedTransform.execute(executionContext)
      case "preregression-commercial-price_holidays" => CommercialFeatEnggProcessor.execute(executionContext)
      case "preregression-commercial-competition" => CommercialFeatEnggProcessor2.execute(executionContext)
      case "preregression-commercial-cannibalization" => CommercialFeatEnggProcessor3.execute(executionContext)
      case "preregression-commercial-seasonality_npd" => CommercialFeatEnggProcessor4.execute(executionContext)
      case "preregression-commercial-hardware_supplies" => CommercialFeatEnggProcessor5.execute(executionContext)
      case "preregression-commercial-spike" => CommercialFeatEnggProcessor6.execute(executionContext)
      case "preregression-commercial-EOL" => CommercialFeatEnggProcessor7.execute(executionContext)
      case "preregression-commercial-BOL" => CommercialFeatEnggProcessor8.execute(executionContext)
      case "preregression-commercial-opposite" => CommercialFeatEnggProcessor9.execute(executionContext)
      case "preregression-commercial-output" => CommercialFeatEnggProcessor10.execute(executionContext)
      case "preregression-retail-GAP_AND_AD_INITIAL_JOIN" => RetailPreRegressionPart01.execute(executionContext)
      case "preregression-retail-AD" => RetailPreRegressionPart02.execute(executionContext)
      case "preregression-retail-AGGUPSTREAM_INITIAL_JOIN" => RetailPreRegressionPart03.execute(executionContext)
      case "preregression-retail-UNION_OTHER_ACCOUNTS" => RetailPreRegressionPart04.execute(executionContext)
      case "preregression-retail-EOL" => RetailPreRegressionPart05.execute(executionContext)
      case "preregression-retail-BOL" => RetailPreRegressionPart06.execute(executionContext)
      case "preregression-retail-HOLIDAYS" => RetailPreRegressionPart07.execute(executionContext)
      case "preregression-retail-L1_L2_COMPETITION_HALF" => RetailPreRegressionPart08.execute(executionContext)
      case "preregression-retail-L1_L2_COMPETITION" => RetailPreRegressionPart09.execute(executionContext)
      case "preregression-retail-BOGO" => RetailPreRegressionPart10.execute(executionContext)
      case "preregression-retail-L2_L2_CANNIBALIZATION" => RetailPreRegressionPart11.execute(executionContext)
      case "preregression-retail-SEASONALITY_HARDWARE_GM" => RetailPreRegressionPart12.execute(executionContext)
      case "preregression-retail-SUPPLIES_GM" => RetailPreRegressionPart13.execute(executionContext)
      case "preregression-retail-NOPROMO_SKU_CATEGORY" => RetailPreRegressionPart14.execute(executionContext)
      case "preregression-retail-L1_L2_CANN_OFFLINE_ONLINE" => RetailPreRegressionPart15.execute(executionContext)
      case "preregression-retail-L1_L2_INNERCANN_OFFLINE_HALF" => RetailPreRegressionPart16.execute(executionContext)
      case "preregression-retail-L1_L2_INNERCANN" => RetailPreRegressionPart17.execute(executionContext)
      case "preregression-retail-PRICE_BRAND_CANN_OFFLINE_ONLINE" => RetailPreRegressionPart18.execute(executionContext)
      case "preregression-retail-CATE_CANN_OFFLINE_ONLINE" => RetailPreRegressionPart19.execute(executionContext)
      case "preregression-retail-DIRECT_CANN" => RetailPreRegressionPart20.execute(executionContext)
      case "preregression-retail-BEFORE_OUTPUT" => RetailPreRegressionPart21.execute(executionContext)
      case "preregression-retail-OUTPUT" => RetailPreRegressionPart22.execute(executionContext)
      case "gap" => GAPTransform1.execute(executionContext)
      case "gap-2" => GAPTransform2.execute(executionContext)
      case "gap-3" => GAPTransform3.execute(executionContext)
      case "gap-4" => GAPTransform4.execute(executionContext)
      case "gap-5" => GAPTransform5.execute(executionContext)
      case "amz" => AmazonTransform.execute(executionContext)
      case "Load-print-files" => LoadRawPrintTables.execute(executionContext)
      case "npd-pc-monthly-us-stg" => USMonthlyStaging.execute(executionContext)
      case "npd-pc-monthly-us-fact" => USTransformer.execute(executionContext)
      case "npd-pc-monthly-ca-stg" => CAMonthlyStaging.execute(executionContext)
      case "npd-pc-monthly-ca-fact" => CATransformer.execute(executionContext)
      case "npd-pc-weekly-us-stg" => USWeeklyStaging.execute(executionContext)
      case "npd-pc-weekly-us-fact" => USWeeklyTransformer.execute(executionContext)
      case "npd-pc-weekly-ca-stg" => CAWeeklyStaging.execute(executionContext)
      case "npd-pc-weekly-ca-fact" => CAWeeklyTransformer.execute(executionContext)
      //case "preregression-retail" => RetailFeatEnggProcessor.execute(executionContext)
    }
  }
}
