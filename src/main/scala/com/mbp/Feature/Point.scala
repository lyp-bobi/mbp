
package com.mbp.Feature

class Point(var coord: xytList) extends Feature {
  def this(x:Double,y:Double,t:Double){
    this(new xytList(x,y,t))
  }
  def this(array: Array[Double]){
    this(new xytList(array(0),array(1),(if(array.length>2) array(2) else 0)))
  }

  override def intersects3(other: Feature): Boolean = {
    other match {
      case p: Point => p == this
      case mbr: MBR => mbr.contains(this)
    }
  }

  override def minDist3(other: Feature): Double = {
    other match {
      case p: Point => minDist3(p)
      case mbr: MBR => mbr.minDist3(this)
    }
  }

  def minDist3(other: Point): Double = {
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
    if(n==0){
      return x
    } else if (n==1){
      return y
    } else if(n==2){
      return t
    } else{
      throw new IllegalArgumentException
    }
  }
  def toArray:Array[Double]={
    Array(x,y,t)
  }

}