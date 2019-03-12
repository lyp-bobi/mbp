package com.mbp.index


import com.mbp.Feature._

import scala.collection.mutable
import scala.util.control.Breaks

//TODO: implement HRTree

class HRTreeEntry(rTree: RTree) {

}

case class HRTree(var map:mutable.HashMap[Tuple2[Long,Long],HRTreeEntry]) extends Index with Serializable{
  def range(query: MBR): Array[(Feature, Int)] ={
    null
  }
  def kNN(query: Trajectory, k: Int, keepSame: Boolean = false): Array[(Feature, Int)] = {
    null
  }
}

object HRTree{
  def apply(entries: Array[(Trajectory, Int)], max_entries_per_node: Int): HRTree = {
    if(!entries(0)._1.segmented){
      for(ent<-entries){
        ent._1.segmentate(new timeDivision())//should use a new td
      }
    }
    val segments=entries.flatMap(x=>x._1.segments.map(y=>(y.time,y,x._2))) //the segments
    val grouped=segments.groupBy(_._1)
    val map =new mutable.HashMap[Tuple2[Long,Long],HRTreeEntry]
    for((time,seg)<-grouped){
      val rt=new HRTreeEntry(RTree(seg.map(x=>(x._2.toMBR(),x._3,1)),10))
      map(time)=rt
    }
    new HRTree(map)
  }
  def apply(entries: Array[(MBR, Int, Int)], max_entries_per_node: Int): HRTree = null
}