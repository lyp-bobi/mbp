
package com.mbp.Feature
case class MBB(var low: Point, high: Point) extends Feature {
  def contains(p: Point): Boolean = {
    for (i <- 1 to 3)
      if (low.coord(i) > p.coord(i) || high.coord(i) < p.coord(i)) {
        return false
      }
    true
  }
  override def intersects(other: Feature): Boolean = {
    other match {
      case p: Point => contains(p)
      case mbr: MBB => intersects(mbr)
    }
  }
  def intersects(other: MBB): Boolean = {
    for (i <- 1 to 3)
      if (low.coord(i) > other.high.coord(i) || high.coord(i) < other.low.coord(i)) {
        return false
      }
    true
  }
  override def minDist(other: Feature): Double = {
    other match {
      case p: Point => minDist(p)
      case mbr: MBB => minDist(mbr)
    }
  }
  def minDist(p: Point): Double = {
    var ans = 0.0
    for (i <- 1 to 3) {
      if (p.coord(i) < low.coord(i)) {
        ans += (low.coord(i) - p.coord(i)) * (low.coord(i) - p.coord(i))
      } else if (p.coord(i) > high.coord(i)) {
        ans += (p.coord(i) - high.coord(i)) * (p.coord(i) - high.coord(i))
      }
    }
    Math.sqrt(ans)
  }
  def minDist(other: MBB): Double = {
    var ans = 0.0
    for (i <- 1 to 3) {
      var x = 0.0
      if (other.high.coord(i) < low.coord(i)) {
        x = Math.abs(other.high.coord(i) - low.coord(i))
      } else if (high.coord(i) < other.low.coord(i)) {
        x = Math.abs(other.low.coord(i) - high.coord(i))
      }
      ans += x * x
    }
    Math.sqrt(ans)
  }
  def area: Double = low.coord.toArray.zip(high.coord.toArray).map(x => x._2 - x._1).product

  def calcRatio(query: MBB): Double = {
    val intersect_low = low.coord.toArray.zip(query.low.coord.toArray).map(x => Math.max(x._1, x._2))
    val intersect_high = high.coord.toArray.zip(query.high.coord.toArray).map(x => Math.min(x._1, x._2))
    val diff_intersect:Array[Double] = intersect_low.zip(intersect_high).map(x => x._2 - x._1)
    if (diff_intersect.forall(_ > 0)) 1.0 * diff_intersect.product / area
    else 0.0
  }
}