package org.apache.spark.sql.mbp.expression

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._
import org.apache.spark.sql.mbp.udt.{FeatureType, Point, Trajectory}

case class TrajectoryWrapper(exps: Seq[Expression])
  extends Expression with CodegenFallback {
    override def nullable: Boolean = false

    override def dataType: DataType = FeatureType

    override def children: Seq[Expression] = exps

    override def eval(input: InternalRow): Any = {
      val coord = exps.map(_.eval(input).asInstanceOf[Point]).toArray
      val traj=Trajectory(2)
      traj

    }
}
