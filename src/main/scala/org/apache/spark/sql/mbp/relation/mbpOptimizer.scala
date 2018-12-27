package org.apache.spark.sql.mbp.relation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule


case class mbpOptimizer(spark: SparkSession) extends Rule[LogicalPlan] {
  // TODO: replace the Relation with B_RTreeRelation if Possible(*simba indexManager)
  override def apply(plan: LogicalPlan): LogicalPlan = plan
}
