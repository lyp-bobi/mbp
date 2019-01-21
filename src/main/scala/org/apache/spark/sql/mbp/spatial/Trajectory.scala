package org.apache.spark.sql.mbp.spatial

case class Trajectory(points: Array[Point]) extends Shape {
  override def minDist(other: Shape): Double={

  }

  override def intersects(other: Shape): Boolean={

  }

  override def getMBR: MBR={

  }

  override val dimensions: Int = 1

}
