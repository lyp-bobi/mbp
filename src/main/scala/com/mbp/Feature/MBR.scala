
package com.mbp.Feature
case class MBR(low: Point, high: Point)  {
  require(low.dimensions == high.dimensions)
  def intersects(traj:Trajectory):Boolean={
    true
  }

}