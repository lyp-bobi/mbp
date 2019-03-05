package com.mbp.Feature

import scala.collection.mutable

class Trajectory(dim:Int) extends Feature {
  // TODO: implement these methods
  override def intersects3(other: Feature): Boolean = false

  override def minDist3(other: Feature): Double = 0
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
  val points = new mutable.MutableList[Point]

  override def intersects3(other: Feature): Boolean = false

  override def minDist3(other: Feature): Double = 0
}