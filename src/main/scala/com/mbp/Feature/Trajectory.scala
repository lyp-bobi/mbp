package com.mbp.Feature

import scala.collection.mutable

class Trajectory(var points:mutable.ArrayBuffer[Point] = new mutable.ArrayBuffer[Point],var id:Long=0) extends Feature {
  // TODO: implement these methods
  override def intersects2(other: Feature): Boolean = false
  override def intersects3(other: Feature): Boolean = false

  override def minDist3(other: Feature): Double = 0
  var segmented= false
  var segments = new mutable.ArrayBuffer[Segment]
  def segmentate(td:timeDivision): Unit ={
    if(!segmented&& points.nonEmpty &&segments.isEmpty){
      // TODO: cut Trajectory into segments
      val segp=points.groupBy(p=>td.getPeriod(p.coord.t))
      for((period,ps)<-segp){
        val seg = new Segment(period,ps.sortBy(x=>x.coord.t))
        segments.append(seg)
      }
      segmented=true
    }
  }
  def toMBR():Array[MBR]={
    segments.map(seg=>seg.toMBR()).toArray
  }
  def toMBBC():Array[MBBC]={
    segments.map(seg=>seg.toMBBC()).toArray
  }
  def caltime():Tuple2[Long,Long]={
    if(points.nonEmpty){
      var tlow=points(0).coord.t
      var thigh = points(0).coord.t
      for(p<-points){
        if(p.coord.t<tlow) tlow=p.coord.t
        if(p.coord.t>thigh) thigh=p.coord.t
      }
      return (tlow,thigh)
    }
    else {
      var tlow=points(0).coord.t
      var thigh = points(0).coord.t
      for(s<-segments){
        if(s.caltime()._1<tlow) tlow=s.caltime()._1
        if(s.caltime()._2>thigh) thigh=s.caltime()._2
      }
      return (tlow,thigh)
    }
  }
}


class Segment(var time:Tuple2[Long,Long]=(0,0),
              var points:mutable.ArrayBuffer[Point] = new mutable.ArrayBuffer[Point])
  extends Feature {
  override def intersects2(other: Feature): Boolean = false
  override def intersects3(other: Feature): Boolean = false
  override def minDist3(other: Feature): Double = 0

  override def toString: String = points.toString()
  def toMBR():MBR={
    if(points.isEmpty) return MBR(new Point(0,0,0),new Point(0,0,0))
    var low:Point=points(0)
    var high:Point=points(0)
    for(p<-points){
      low=low.getlow(p)
      high=high.gethigh(p)
    }
    MBR(low,high)
  }
  def maxspeed(): Double ={
    val sp=points(0)
    val ep=points(points.length-1)
    var max:Double=0
    for(p<-points){
      if(p.coord.t!=sp.coord.t){
        if(p.minDist2(sp)/p.minDist1(sp)>max) max=p.minDist2(sp)/p.minDist1(sp)
      }
      if(p.coord.t!=ep.coord.t) {
        if (p.minDist2(ep) / p.minDist1(ep) > max) max = p.minDist2(ep) / p.minDist1(ep)
      }

    }
    return max
  }
  def toMBBC():MBBC={
    new MBBC(MBR(points(0),points(0)),MBR(points(points.length-1),points(points.length-1)),maxspeed(),toMBR())
  }
  def caltime():Tuple2[Long,Long]= {
    if (points.nonEmpty) {
      var tlow = points(0).coord.t
      var thigh = points(0).coord.t
      for (p <- points) {
        if (p.coord.t < tlow) tlow = p.coord.t
        if (p.coord.t > thigh) thigh = p.coord.t
      }
      return (tlow, thigh)
    }
    else return (0, 0)
  }
}