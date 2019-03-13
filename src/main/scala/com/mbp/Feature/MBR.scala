
package com.mbp.Feature
case class MBR(var low: Point,var high: Point) extends Feature {
  //TODO: Use squared distance
  def this(xrange:Tuple2[Double,Double],yrange:Tuple2[Double,Double],trange:Tuple2[Long,Long]){
    this(new Point(xrange._1,yrange._1,trange._1),new Point(xrange._2,yrange._2,trange._2))
  }
  def contains(p: Point): Boolean = {
    for (i <- 0 to 2)
      if (low.coord(i) > p.coord(i) || high.coord(i) < p.coord(i)) {
        return false
      }
    true
  }
  override def intersects3(other: Feature): Boolean = {
    other match {
      case p: Point => contains(p)
      case mbr: MBR => intersects3(mbr)
    }
  }
  override def intersects2(other: Feature): Boolean = {
    other match {
      case p: Point => contains(p)
      case mbr: MBR => intersects2(mbr)
    }
  }
  def intersects2(other: MBR): Boolean = {
    for (i <- 0 to 1)
      if (low.coord(i) > other.high.coord(i) || high.coord(i) < other.low.coord(i)) {
        return false
      }
    true
  }
  def intersects3(other: MBR): Boolean = {
    for (i <- 0 to 2)
      if (low.coord(i) > other.high.coord(i) || high.coord(i) < other.low.coord(i)) {
        return false
      }
    true
  }
  override def minDist3(other: Feature): Double = {
    other match {
      case p: Point => minDist3(p)
      case mbr: MBR => minDist3(mbr)
    }
  }
  def minDist3(p: Point): Double = {
    var ans = 0.0
    for (i <- 0 to 2) {
      if (p.coord(i) < low.coord(i)) {
        ans += (low.coord(i) - p.coord(i)) * (low.coord(i) - p.coord(i))
      } else if (p.coord(i) > high.coord(i)) {
        ans += (p.coord(i) - high.coord(i)) * (p.coord(i) - high.coord(i))
      }
    }
    Math.sqrt(ans)
  }
  def minDist3(other: MBR): Double = {
    var ans = 0.0
    for (i <- 0 to 2) {
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

  def calcRatio(query: MBR): Double = {
    val intersect_low = low.coord.toArray.zip(query.low.coord.toArray).map(x => Math.max(x._1, x._2))
    val intersect_high = high.coord.toArray.zip(query.high.coord.toArray).map(x => Math.min(x._1, x._2))
    val diff_intersect:Array[Double] = intersect_low.zip(intersect_high).map(x => x._2 - x._1)
    if (diff_intersect.forall(_ > 0)) 1.0 * diff_intersect.product / area
    else 0.0
  }
  def stDist(other:MBR): Tuple2[Double,Double] ={
    var ans = 0.0
    for (i <- 1 to 2) {
      var x = 0.0
      if (other.high.coord(i) < low.coord(i)) {
        x = Math.abs(other.high.coord(i) - low.coord(i))
      } else if (high.coord(i) < other.low.coord(i)) {
        x = Math.abs(other.low.coord(i) - high.coord(i))
      }
      ans += x * x
    }
    var anst:Double=0.0
    if (other.high.coord(3) < low.coord(3)) {
      anst = Math.abs(other.high.coord(3) - low.coord(3))
    } else if (high.coord(3) < other.low.coord(3)) {
      anst = Math.abs(other.low.coord(3) - high.coord(3))
    }
    (Math.sqrt(ans),anst)
  }

  override def toString: String = {
    low.coord.x.toString()+","+high.coord.x.toString()+","+low.coord.y.toString()+","+high.coord.y.toString()+","+low.coord.t.toString()+","+high.coord.t.toString()
  }
}