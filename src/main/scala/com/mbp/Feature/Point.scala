
package com.mbp.Feature

class Point(var coord: xytList) extends Feature {
  def this(x:Double,y:Double,t:Double){
    this(new xytList(x,y,t))
  }
  def this(array: Array[Double]){
    this(new xytList(array(0),array(1),array(2)))
  }
  override def intersects(other: Feature): Boolean = {
    other match {
      case p: Point => p == this
      case mbr: MBR => mbr.contains(this)
    }
  }

  override def minDist(other: Feature): Double = {
    other match {
      case p: Point => minDist(p)
      case mbr: MBR => mbr.minDist(this)
    }
  }

  def minDist(other: Point): Double = {
    var ans = 0.0
    for (i <- 1 to 3)
      ans += (coord(i) - other.coord(i)) * (coord(i) - other.coord(i))
    Math.sqrt(ans)
  }
}

class xytList(var x:Double,var y:Double,var t:Double){
  def this(array: Array[Double]){
    this(array(0),array(1),array(2))
  }
  def apply(n:Int):Double={
    if(n==1){
      return x
    } else if (n==2){
      return y
    } else if(n==3){
      return t
    } else{
      throw new IllegalArgumentException
    }
  }
  def toArray:Array[Double]={
    Array(x,y,t)
  }

}