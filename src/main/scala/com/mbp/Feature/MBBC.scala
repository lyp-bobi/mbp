
package com.mbp.Feature
case class MBBC(var start:MBR,var end:MBR,var maxspeed:Double,var mbr:MBR) extends Feature {
  def contains(p: Point): Boolean = {
    if(p.coord.t<=end.low.coord.t&&p.coord.t>=start.low.coord.t&&start.expand(p).contains(p) && end.expand(p).contains(p))
      return true
    return false
  }
  override def intersects3(other: Feature): Boolean = {
    other match {
      case p: Point => contains(p)
      case mbr: MBR => intersects3(mbr)
      case mbbc:MBBC => intersects3(mbbc)
    }
  }
  override def intersects2(other: Feature): Boolean = {
    other match {
      case p: Point => false
      case mbr: MBR => intersects2(mbr)
    }
  }
  def intersects2(other: MBR): Boolean = {
    if(expand(other.low).intersects2(other)&&mbr.intersects2(other)) return true
    return false
  }
  def intersects3(other: MBR): Boolean = {
    //TODO: implement this
    false
  }
  def intersects3(other: MBBC): Boolean = {
    //TODO: implement this
    false
  }
  override def minDist3(other: Feature): Double = {
    other match {
      case p: Point => minDist3(p)
      case mbr: MBR => minDist3(mbr)
      case mbbc:MBBC => minDist3(mbbc)
    }
  }
  def minDist3(p: Point): Double = {
    0
  }
  def minDist3(other: MBR): Double = {
    0
  }
  def minDist3(other: MBBC): Double = {
    0
  }


  def expand(point: Point): MBR ={
    start.expand(point).intersection2(end.expand(point)).get
  }
  override def toString: String = {
    start.toString+","+end.toString+","+maxspeed+","+mbr.toString
  }
}