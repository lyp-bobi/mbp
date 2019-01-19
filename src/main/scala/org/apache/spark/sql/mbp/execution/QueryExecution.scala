package org.apache.spark.sql.mbp.execution

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, QueryExecution => SQLQueryExecution}

class QueryExecution (val mbpSession:SparkSession, override val logical:LogicalPlan)
  extends SQLQueryExecution(mbpSession,logical) {
  lazy val withIndexedData: LogicalPlan = {
    assertAnalyzed()
    mbpSession.sessionState.indexManager.useIndexedData(withCachedData)
  }

  override lazy val optimizedPlan: LogicalPlan = {
    mbpSession.sessionState.optimizer.execute(withIndexedData)
  }

  override lazy val sparkPlan: SparkPlan ={
    SimbaSession.setActiveSession(simbaSession)
    simbaSession.sessionState.planner.plan(optimizedPlan).next()
  }

}
