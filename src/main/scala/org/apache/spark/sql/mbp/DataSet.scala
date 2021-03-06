package org.apache.spark.sql.mbp

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.mbp.InRange
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.mbp.QueryExecution
import org.apache.spark.sql.{Encoder, Row, SparkSession, Dataset => SQLDataset}
import com.mbp.Feature.{Point, Trajectory}
import org.apache.spark.sql.catalyst.expressions.mbp._

private[mbp] object Dataset {
  def apply[T: Encoder](mbpSession: MBPSession, logicalPlan: LogicalPlan): Dataset[T] = {
    new Dataset(mbpSession, logicalPlan, implicitly[Encoder[T]])
  }

  def ofRows(mbpSession: MBPSession, logicalPlan: LogicalPlan): DataFrame = {
    val qe = mbpSession.executePlan(logicalPlan)
    qe.assertAnalyzed()
    new Dataset[Row](mbpSession, qe, RowEncoder(qe.analyzed.schema))
  }
}

class Dataset[T] private[mbp] (@transient val mbpSession: MBPSession,
                               @transient override val queryExecution: QueryExecution,
                               encoder: Encoder[T])
  extends SQLDataset[T](mbpSession, queryExecution.logical, encoder){
  def this(mbpSession: MBPSession, logicalPlan: LogicalPlan, encoder: Encoder[T]) = {
    this(mbpSession, {
      val qe = mbpSession.executePlan(logicalPlan)
      qe
    }, encoder)
  }

  /**
    * Spatial operation, range query.
    * {{{
    *   trajectory.range(Array("x", "y"), Array(10, 10), Array(20, 20))
    *   trajectory.filter($"x" >= 10 && $"x" <= 20 && $"y" >= 10 && $"y" <= 20)
    * }}}
    */
  def range(point1: Array[Double], point2:Array[Double]): DataFrame = withPlan {
    //val attrs = getAttributes(keys)
    //attrs.foreach(attr => assert(attr != null, "column not found"))

    //Filter(InRange(TrajectoryWrapper(attrs),
    //  LiteralUtil(traj1),
    //  LiteralUtil(traj2)), logicalPlan)
    Filter(InRange(LiteralUtil(new Point(point1)), LiteralUtil(new Point(point2))), logicalPlan)
  }

  /**
    * Spatial operation, range query
    * {{{
    *   point.range(p, Array(10, 10), Array(20, 20))
    * }}}
    */

  private def getAttributes(keys: Array[String], attrs: Seq[Attribute] = this.queryExecution.analyzed.output)
  : Array[Attribute] = {
    keys.map(key => {
      val temp = attrs.indexWhere(_.name == key)
      if (temp >= 0) attrs(temp)
      else null
    })
  }

  @inline private def withPlan(logicalPlan: => LogicalPlan): DataFrame = {
    Dataset.ofRows(mbpSession, logicalPlan)
  }
}





