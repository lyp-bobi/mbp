package org.apache.spark.sql.catalyst.expressions.mbp

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types._
import com.mbp.Feature._

case class InRange(range_low:Expression, range_high:Expression)
  extends Expression with CodegenFallback {
  override def dataType: DataType = BooleanType

  override def nullable: Boolean = false

  override def children: Seq[Expression] = Seq(range_low, range_high)

  override def eval(input: InternalRow):Any={
    val eval_shape = FeatureType.deserialize(input).asInstanceOf[Trajectory]
    val eval_low = range_low.asInstanceOf[Literal].value.asInstanceOf[Point]
    val eval_high = range_high.asInstanceOf[Literal].value.asInstanceOf[Point]
    //require(eval_shape.dimensions == eval_low.dimensions && eval_shape.dimensions == eval_high.dimensions)
    val mbr = MBR(eval_low, eval_high)
    mbr.intersects(eval_shape)
  }

}
