package com.mbp.index

import com.mbp.Feature._

import scala.collection.mutable

//TODO: implement HRTree


case class HR2Tree(var map:mutable.HashMap[Tuple2[Long,Long],R2Tree],td:timeDivision) extends Index with Serializable {
  def cross(query: MBR): Array[(Feature, Int)] ={
    if(query.low.coord.t!=query.high.coord.t) println("warn:the time of the MBR is not an instant, using only the smaller time bound")
    val r2tree=map(td.getPeriod(query.low.coord.t))
    println(r2tree.root.m_mbbc)
    println(r2tree.root.size)
    r2tree.range2(query)
  }
  def dist(query: Trajectory, k: Int, keepSame: Boolean = false): Array[(Feature, Int)] = {
    null
  }
}

object HR2Tree{
  def apply(entries: Array[(Trajectory, Int)], max_entries_per_node: Int,td:timeDivision): HR2Tree = {
    if(!entries(0)._1.segmented){
      for(ent<-entries){
        ent._1.segmentate(td)//should use a new td
      }
    }
    val segments=entries.flatMap(x=>x._1.segments.map(y=>(y.time,y,x._2))) //the segments
    val grouped=segments.groupBy(_._1)
    val map =new mutable.HashMap[Tuple2[Long,Long],R2Tree]
    for((time,seg)<-grouped){
      val rt=R2Tree(seg.map(x=>(x._2.toMBBC(),x._3,1)),max_entries_per_node)
      map(time)=rt
    }
    new HR2Tree(map,td)
  }
}
