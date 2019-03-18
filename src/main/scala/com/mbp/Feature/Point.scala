
package com.mbp.Feature

class Point(var coord: xytList) extends Feature {
  override def toString: String = "("+coord.x.toString+","+coord.y.toString+","+coord.t.toString+")"
  def this(x:Double,y:Double,t:Long){
    this(new xytList(x,y,t))
  }
  def this(array: Array[Double]){
    this(new xytList(array(0),array(1),(if(array.length>2) array(2).toLong else 0)))
  }
  override def intersects2(other: Feature): Boolean = {
    other match {
      case p: Point => p == this
      case mbr: MBR => mbr.contains(this)
    }
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
    for (i <- 0 to 2)
      ans += (coord(i) - other.coord(i)) * (coord(i) - other.coord(i))
    Math.sqrt(ans)
  }
  def minDist2(other: Point): Double = {
    var ans = 0.0
    for (i <- 0 to 1)
      ans += (coord(i) - other.coord(i)) * (coord(i) - other.coord(i))
    Math.sqrt(ans)
  }
  def minDist1(other: Point): Double = {
    val i = 2
    (coord(i) - other.coord(i)) * (coord(i) - other.coord(i))
  }
  def gethigh(other:Point):Point={
    new Point(math.max(coord.x,other.coord.x),math.max(coord.y,other.coord.y),math.max(coord.t,other.coord.t).toLong)
  }
  def getlow(other:Point):Point={
    new Point(math.min(coord.x,other.coord.x),math.min(coord.y,other.coord.y),math.min(coord.t,other.coord.t).toLong)
  }
}

class xytList(var x:Double,var y:Double,var t:Long){
  def this(array: Array[Double]){
    this(array(0),array(1),array(2).toLong)
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