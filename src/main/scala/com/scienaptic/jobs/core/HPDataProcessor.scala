package com.scienaptic.jobs.core

import com.scienaptic.jobs.ExecutionContext

object HPDataProcessor {
  def execute(executionContext: ExecutionContext): Unit = {
    executionContext.configuration.config match {
      case "retail" => RetailTransform.execute(executionContext)
      case "commercial" => CommercialSimplifiedTransform.execute(executionContext)
      case "preregression-commercial" => CommercialFeatEnggProcessor.execute(executionContext)
      case "preregression-commercial-2" => CommercialFeatEnggProcessorPart2.execute(executionContext)
      case "gap" => GAPTransform.execute(executionContext)
      //case "preregression-retail" => RetailFeatEnggProcessor.execute(executionContext)
    }
  }
}
