package org.apache.spark.sql.mbp.expression

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

case class InRange(shape:Expression,range_low:Expression, range_high:Expression)
  extends Expression with CodegenFallback {
  override def dataType: DataType = BooleanType

  override def nullable: Boolean = false

  override def children: Seq[Expression] = Seq(shape, range_low, range_high)

  override def eval(input: InternalRow):Any={

  }

}
