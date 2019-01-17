package org.apache.spark.sql.mbp.udt

import scala.collection.mutable

class Trajectory(dim:Int) extends Feature {
  override val dimensions: Int = dim
  var segmented= false
  val points = new mutable.MutableList[Point]
  val segments = new mutable.MutableList[Segment]
  def segmentate(time_interval:Double): Unit ={
    if(!segmented&& points.nonEmpty &&segments.isEmpty){
      // TODO: cut Trajectory into segments
      segmented=true
    }
  }
}


class Segment extends Feature {
  override val dimensions: Int = 3
  val points = new mutable.MutableList[Point]
}