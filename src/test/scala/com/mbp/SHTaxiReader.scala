package com.mbp

import java.text.SimpleDateFormat

import scala.io.Source
import scala.collection.mutable
import com.mbp.Feature._
class SHTaxiReader {

}

object SHTaxiReader{
  def apply(s:String): Trajectory ={
    val file=Source.fromFile(s)
    val lines = file.getLines()
    val points= new mutable.ArrayBuffer[Point]()
    var id:Long = 0
    for(line<-lines){
      //println(line)
      val arr=line.split(",")
      val fm=new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss")
      val t=fm.parse(arr(1)).getTime
      val x = arr(2).toDouble
      val y = arr(3).toDouble
      id=arr(0).toLong
      points.append(new Point(x,y,t))
    }
    new Trajectory(points,id)
  }
}
