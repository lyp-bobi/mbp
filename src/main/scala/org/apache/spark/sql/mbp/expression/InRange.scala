package org.apache.spark.sql.mbp.expression

import org.apache.spark.sql.catalyst.expressions.Expression

case class InRange(range_low:Expression, range_high:Expression) {

}
