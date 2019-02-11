package com.scienaptic.jobs.core

import com.scienaptic.jobs.ExecutionContext
object HPDataProcessor {
  def execute(executionContext: ExecutionContext): Unit = {
    executionContext.configuration.config match {
      case "retail" => RetailTransform.execute(executionContext)
      case "commercial" => CommercialSimplifiedTransform.execute(executionContext)
      case "preregression-commercial" => CommercialFeatEnggProcessor.execute(executionContext)
      case "preregression-commercial-2" => CommercialFeatEnggProcessor2.execute(executionContext)
      case "preregression-commercial-3" => CommercialFeatEnggProcessor3.execute(executionContext)
      case "preregression-commercial-4" => CommercialFeatEnggProcessor4.execute(executionContext)
      case "preregression-commercial-5" => CommercialFeatEnggProcessor5.execute(executionContext)
      case "preregression-commercial-6" => CommercialFeatEnggProcessor6.execute(executionContext)
      case "preregression-commercial-7" => CommercialFeatEnggProcessor7.execute(executionContext)
      case "preregression-commercial-8" => CommercialFeatEnggProcessor8.execute(executionContext)
      case "preregression-commercial-9" => CommercialFeatEnggProcessor9.execute(executionContext)
      case "preregression-commercial-10" => CommercialFeatEnggProcessor10.execute(executionContext)
      case "preregression-retail" => RetailPreRegressionPart01.execute(executionContext)
      case "preregression-retail-2" => RetailPreRegressionPart02.execute(executionContext)
      case "preregression-retail-3" => RetailPreRegressionPart03.execute(executionContext)
      case "preregression-retail-4" => RetailPreRegressionPart04.execute(executionContext)
      case "preregression-retail-5" => RetailPreRegressionPart05.execute(executionContext)
      case "preregression-retail-6" => RetailPreRegressionPart06.execute(executionContext)
      case "preregression-retail-7" => RetailPreRegressionPart07.execute(executionContext)
      case "preregression-retail-8" => RetailPreRegressionPart08.execute(executionContext)
      case "preregression-retail-9" => RetailPreRegressionPart09.execute(executionContext)
      case "preregression-retail-10" => RetailPreRegressionPart10.execute(executionContext)
      case "preregression-retail-11" => RetailPreRegressionPart11.execute(executionContext)
      case "preregression-retail-12" => RetailPreRegressionPart12.execute(executionContext)
      case "preregression-retail-13" => RetailPreRegressionPart13.execute(executionContext)
      case "preregression-retail-14" => RetailPreRegressionPart14.execute(executionContext)
      case "preregression-retail-15" => RetailPreRegressionPart15.execute(executionContext)
      case "preregression-retail-16" => RetailPreRegressionPart16.execute(executionContext)
      case "preregression-retail-17" => RetailPreRegressionPart17.execute(executionContext)

      case "gap" => GAPTransform.execute(executionContext)
      case "amz" => AmazonTransform.execute(executionContext)
      //case "preregression-retail" => RetailFeatEnggProcessor.execute(executionContext)
    }
  }
}
