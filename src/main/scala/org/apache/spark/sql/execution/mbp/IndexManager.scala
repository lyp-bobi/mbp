package org.apache.spark.sql.execution.mbp

import org.apache.spark.sql.catalyst.expression.mbp.relation.B_RTreeRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan


private case class IndexedData(name: String, plan: LogicalPlan, indexedData: B_RTreeRelation)

class IndexManager() {

}
