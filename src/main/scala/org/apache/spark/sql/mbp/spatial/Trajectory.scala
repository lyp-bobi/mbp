package org.apache.spark.sql.mbp.spatial

case class Trajectory(id: Int, segments: Array[Point]) extends Shape {

}
