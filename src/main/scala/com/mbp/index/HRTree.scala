package com.mbp.index


import com.mbp.Feature._

import scala.collection.mutable
import scala.util.control.Breaks

//TODO: implement HRTree

class HRTreeEntry(rTree: RTree) {

}

case class HRTree(map:mutable.HashMap[Tuple2[Double,Double],HRTreeEntry]) extends Index with Serializable{
  def range(query: MBR): Array[(Feature, Int)] ={
    null
  }
  def kNN(query: Trajectory, k: Int, keepSame: Boolean = false): Array[(Feature, Int)] = {
    null
  }
}

object HRTree{
  def apply(entries: Array[(Trajectory, Int)], max_entries_per_node: Int): HRTree = null
  def apply(entries: Array[(MBR, Int, Int)], max_entries_per_node: Int): HRTree = null
}