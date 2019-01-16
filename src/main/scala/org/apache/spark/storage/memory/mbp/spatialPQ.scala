package org.apache.spark.storage.memory.mbp


import org.apache.spark.sql.mbp.udt.stRange

import scala.collection.mutable
import org.apache.spark.internal.Logging

import scala.collection.mutable


// visit of an element would prevent elements "near" enough from being evicted
case class spatialPQ[A,B](spatial_thres:Double,temporal_thres:Double)
  extends mutable.LinkedHashMap[A,B] with Logging {
  val locs= new mutable.HashMap[A,stRange]()
  val neighbours= new mutable.HashMap[A,mutable.MutableList[A]]()
  def moveToTail(key:A): Unit ={
    var last:Entry = lastEntry
    if(lastEntry.key != key){
      var p = findEntry(key)
      var b=p.earlier
      var a=p.later
      p.later = null
      if (b == null)
        firstEntry = a
      else
        b.later = a
      if (a != null)
        a.earlier = b
      else
        last = b
      if (last == null)
        firstEntry = p
      else {
        p.earlier = last
        last.earlier = p
      }
      lastEntry = p
    }

  }
  override def get(key: A): Option[B] ={
    for (nei <- neighbours.get(key)){
      moveToTail(key)
    }
    moveToTail(key)
    super.get(key)
  }
  override def put(key:A,value:B): Option[B] ={
    logWarning("Inserting a block without spatial infomation")
    super.put(key,value)
  }
  def put(key:A,value:B,range:stRange):Option[B]={
    locs.put(key,range)
    val neighList = new mutable.MutableList[A]
    locs.foreach[Unit](x=>{
      val (sdist,tdist) =range.distToVisit(x._2)
      if(sdist<spatial_thres&&tdist<temporal_thres){
        neighList+=x._1
        neighbours(x._1)+=key
        moveToTail(x._1)
      }
    })
    neighbours.put(key,neighList)
    super.put(key,value)
  }
  override def clear(): Unit ={
    locs.clear()
    neighbours.clear()
    super.clear()
  }
}

