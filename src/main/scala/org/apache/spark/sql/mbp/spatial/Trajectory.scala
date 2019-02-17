package org.apache.spark.sql.mbp.spatial

import org.apache.spark.sql.mbp.udt.Feature
case class Trajectory(points: Array[Point]) extends Feature {
  override def minDist(other: Shape): Double={

  }

  override def intersects(other: Shape): Boolean={

  }

  override def getMBR: MBR={

  }

  override val dimensions: Int = 1

}

object Trajectory{
  def apply(points:Array[Point]):Trajectory={
    new Trajectory(points)
  }
}