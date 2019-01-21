package org.apache.spark.sql.mbp.execution

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.mbp.MBPSession

class MBPPlan extends SparkPlan {
  protected override def sparkContext = MBPSession.getActiveSession.map(_.sparkContext).orNull

}
