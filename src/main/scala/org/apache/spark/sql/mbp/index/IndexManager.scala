package org.apache.spark.sql.mbp.index

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}


private case class IndexedData(name: String, plan: LogicalPlan, indexedData: B_RTreeRelation)

class IndexManager() {

}
