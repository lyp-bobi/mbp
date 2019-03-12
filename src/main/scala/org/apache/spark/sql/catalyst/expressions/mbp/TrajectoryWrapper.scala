package org.apache.spark.sql.catalyst.expressions.mbp

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._
import com.mbp.Feature._

case class TrajectoryWrapper(exps: Seq[Expression])
  extends Expression with CodegenFallback {
    override def nullable: Boolean = false

    override def dataType: DataType = FeatureType

    override def children: Seq[Expression] = exps

    override def eval(input: InternalRow): Any = {
      val coord = exps.map(_.eval(input).asInstanceOf[Point]).toArray
      val traj=new Trajectory()
      traj

    }
}
