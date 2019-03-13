package com.mbp.index


import com.mbp.Feature._

import scala.collection.mutable
import scala.util.control.Breaks
import org.slf4j.Logger
import org.slf4j.LoggerFactory

//TODO: implement HRTree


case class HRTree(var map:mutable.HashMap[Tuple2[Long,Long],RTree],td:timeDivision) extends Index with Serializable {
  val logger = LoggerFactory.getLogger(classOf[HRTree])
  def cross(query: MBR): Array[(Feature, Int)] ={
    if(query.low.coord.t!=query.high.coord.t) logger.warn("the time of the MBR is not an instant, using only the smaller time bound")
    val rtree=map(td.getPeriod(query.low.coord.t))
    println(rtree.root.m_mbr)
    println(rtree.root.size)
    rtree.range(query)
  }
  def dist(query: Trajectory, k: Int, keepSame: Boolean = false): Array[(Feature, Int)] = {
    null
  }
}

object HRTree{
  def apply(entries: Array[(Trajectory, Int)], max_entries_per_node: Int,td:timeDivision): HRTree = {
    if(!entries(0)._1.segmented){
      for(ent<-entries){
        ent._1.segmentate(td)//should use a new td
      }
    }
    val segments=entries.flatMap(x=>x._1.segments.map(y=>(y.time,y,x._2))) //the segments
    val grouped=segments.groupBy(_._1)
    val map =new mutable.HashMap[Tuple2[Long,Long],RTree]
    for((time,seg)<-grouped){
      val rt=RTree(seg.map(x=>(x._2.toMBR(),x._3,1)),10)
      map(time)=rt
    }
    new HRTree(map,td)
  }
}