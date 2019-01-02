package com.scienaptic.jobs.core

import com.scienaptic.jobs.ExecutionContext

object HPDataProcessor {
  def execute(executionContext: ExecutionContext): Unit = {
    executionContext.configuration.config match {
      case "retail" => RetailTransform.execute(executionContext)
      case "commercial" => CommercialSimplifiedTransform.execute(executionContext)
    }
  }
}
