package com.mbp.Feature

class stRange(var xlow:Double,var xhigh:Double,var ylow:Double,var yhigh:Double,var tlow:Double,var thigh:Double)
  extends Feature{
  override val dimensions: Int = 3
  def distToVisit(range:stRange): (Double,Double) ={
    //return (spatial dist,time dist)
    var s=0d
    var t=0d
    var xdist = 0d
    var ydist = 0d
    var tdist = 0d
    if((range.xlow>xlow&&range.xlow<xhigh)||(range.xhigh>xlow&&range.xhigh<xhigh)) xdist = 0
    else xdist = math.min(math.abs(range.xhigh-xlow),math.abs(xhigh-range.xlow))
    if((range.ylow>ylow&&range.ylow<yhigh)||(range.yhigh>ylow&&range.yhigh<yhigh)) ydist = 0
    else ydist = math.min(math.abs(range.yhigh-ylow),math.abs(yhigh-range.ylow))
    tdist=math.abs(range.tlow-thigh)//need more though
    (xdist+ydist,tdist)
  }
}
