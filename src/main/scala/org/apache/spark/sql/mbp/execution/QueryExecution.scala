package org.apache.spark.sql.mbp.execution

import org.apache.spark.sql.{ExperimentalMethods, SparkSession, Strategy}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, SparkPlanner, QueryExecution => SQLQueryExecution}
import org.apache.spark.sql.mbp.MBPSession

class QueryExecution (val mbpSession:MBPSession, override val logical:LogicalPlan)
  extends SQLQueryExecution(mbpSession,logical) {
  /*lazy val withIndexedData: LogicalPlan = {
    assertAnalyzed()
    mbpSession.sessionState.indexManager.useIndexedData(withCachedData)
  }

  override lazy val optimizedPlan: LogicalPlan = {
    mbpSession.sessionState.optimizer.execute(withIndexedData)
  }*/


  override def planner: SparkPlanner = {
    lazy val experimentalMethods = new ExperimentalMethods
    experimentalMethods.extraStrategies = (MBPFilter::Nil)++experimentalMethods.extraStrategies
    new SparkPlanner(mbpSession.sparkContext, mbpSession.sqlContext.conf,
      experimentalMethods )
  }
  override lazy val sparkPlan: SparkPlan ={
    MBPSession.setActiveSession(mbpSession)
    mbpSession.sessionState.planner.plan(optimizedPlan).next()
  }

}
object MBPFilter extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case logical.Filter(condition, child) =>
      FilterExec(condition, planLater(child)) :: Nil
    case _ => Nil
  }
}
