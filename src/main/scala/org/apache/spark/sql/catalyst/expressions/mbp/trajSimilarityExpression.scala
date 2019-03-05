package org.apache.spark.sql.catalyst.expressions.mbp
/*
import java.util.Locale

import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, UnaryExpression, UnsafeArrayData}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType}
import com.mbp.Feature._

case class TrajectorySimilarityExpression(function: TrajectorySimilarityFunction,
                                          traj1: Expression, traj2: Expression)
  extends BinaryExpression with CodegenFallback {

  override def left: Expression = traj1
  override def right: Expression = traj2

  override def dataType: DataType = DoubleType

  override def nullSafeEval(traj1: Any, traj2: Any): Any = function match {
    case TrajectorySimilarityFunction.DTW =>
      val trajectory1 = traj1 match {
        case t: Trajectory => t
        case uad: UnsafeArrayData => TrajectorySimilarityExpression.getTrajectory(uad)
      }
      val trajectory2 = traj2 match {
        case t: Trajectory => t
        case uad: UnsafeArrayData => TrajectorySimilarityExpression.getTrajectory(uad)
      }
      TrajectorySimilarity.DTWDistance.evalWithTrajectory(trajectory1, trajectory2)
  }
}

object TrajectorySimilarityExpression {
  def getPoints(rawData: UnsafeArrayData): Array[Point] = {
    (0 until rawData.numElements()).map(i =>
      Point(rawData.getArray(i).toDoubleArray)).toArray
  }

  def getTrajectory(rawData: UnsafeArrayData): Trajectory = {
    Trajectory((0 until rawData.numElements()).map(i =>
      Point(rawData.getArray(i).toDoubleArray)).toArray)
  }
}

case class TrajectorySimilarityWithThresholdExpression(similarity: TrajectorySimilarityExpression,
                                                       threshold: Double)
  extends UnaryExpression with CodegenFallback {

  override def child: Expression = similarity

  override def dataType: DataType = BooleanType

  override def nullSafeEval(input: Any): Any = {
    input.asInstanceOf[Double] <= threshold
  }
}

case class TrajectorySimilarityWithKNNExpression(similarity: TrajectorySimilarityExpression,
                                                 count: Int)
  extends UnaryExpression with CodegenFallback {

  override def child: Expression = similarity

  override def dataType: DataType = BooleanType

  override def nullSafeEval(left: Any): Any = {
    throw new NotImplementedError()
  }
}

sealed abstract class TrajectorySimilarityFunction {
  def sql: String
}

object TrajectorySimilarityFunction {

  case object DTW extends TrajectorySimilarityFunction {
    override def sql: String = "DTW"
  }

  case object INTEGRAL extends TrajectorySimilarityFunction {
    override def sql: String = "INTEGRAL"
  }


  def apply(typ: String): TrajectorySimilarityFunction =
    typ.toLowerCase(Locale.ROOT).replace("_", "") match {
      case "dtw" => DTW
      case "integral" => INTEGRAL
      case _ =>
        val supported = Seq("dtw")
        throw new IllegalArgumentException(s"Unsupported trajectory similarity function '$typ'. " +
          "Supported trajectory similarity functions include: "
          + supported.mkString("'", "', '", "'") + ".")
    }
}

case class TrajectoryMBRRangeExpression(traj: Expression, mbr: MBR)
  extends UnaryExpression with CodegenFallback {

  override def child: Expression = traj

  override def dataType: DataType = BooleanType

  override def nullSafeEval(input: Any): Any = {
    val trajectory = TrajectorySimilarityExpression.getTrajectory(
      input.asInstanceOf[UnsafeArrayData])
    trajectory.points.forall(mbr.contains)
  }
}

case class TrajectoryCircleRangeExpression(traj: Expression, center: Point, radius: Double)
  extends UnaryExpression with CodegenFallback {

  override def child: Expression = traj

  override def dataType: DataType = BooleanType

  override def nullSafeEval(input: Any): Any = {
    val trajectory = TrajectorySimilarityExpression.getTrajectory(
      input.asInstanceOf[UnsafeArrayData])
    trajectory.points.forall(p => p.minDist3(center) <= radius)
  }
}
*/