package com.mbp.index

import com.mbp.Feature._

import scala.collection.mutable
import scala.util.control.Breaks


abstract class R2TreeEntry {
  def minDist3(x: Feature): Double

  def intersects2(x: Feature): Boolean
}

case class R2TreeLeafEntry(feature: Feature, m_data: Int, size: Int) extends R2TreeEntry {
  override def minDist3(x: Feature): Double = feature.minDist3(x)
  override def intersects2(x: Feature): Boolean = x.intersects2(feature)
}

case class R2TreeInternalEntry(mbbc: MBBC, node: R2TreeNode) extends R2TreeEntry {
  override def minDist3(x: Feature): Double = mbbc.minDist3(x)
  override def intersects2(x: Feature): Boolean = x.intersects2(mbbc)
}

case class R2TreeNode(m_mbbc: MBBC, m_child: Array[R2TreeEntry], isLeaf: Boolean) {
  def this(m_mbbc: MBBC, children: Array[(MBBC, R2TreeNode)]) = {
    this(m_mbbc, children.map(x => R2TreeInternalEntry(x._1, x._2)), false)
  }


  def this(m_mbbc: MBBC, children: => Array[(Point, Int)]) = {
    this(m_mbbc, children.map(x => R2TreeLeafEntry(x._1, x._2, 1)), true)
  }

  def this(m_mbbc: MBBC, children: Array[(MBBC, Int, Int)]) = {
    this(m_mbbc, children.map(x => R2TreeLeafEntry(x._1, x._2, x._3)), true)
  }

  val size: Long = {
    if (isLeaf) m_child.map(x => x.asInstanceOf[R2TreeLeafEntry].size).sum
    else m_child.map(x => x.asInstanceOf[R2TreeInternalEntry].node.size).sum
  }
}

case class R2Tree(root: R2TreeNode) extends Index with Serializable {
  def range2(query: MBR): Array[(Feature, Int)] = {
    var visit=0 //for test
    val ans = mutable.ArrayBuffer[(Feature, Int)]()
    val st = new mutable.Stack[R2TreeNode]()
    if (root.m_mbbc.intersects2(query) && root.m_child.nonEmpty) st.push(root)
    while (st.nonEmpty) {
      val now = st.pop()
      if (!now.isLeaf) {
        now.m_child.foreach {
          case R2TreeInternalEntry(mbbc, node) =>
            if (query.intersects2(mbbc)) {
              st.push(node)
            }
        }
      } else {
        now.m_child.foreach {
          case R2TreeLeafEntry(feature, m_data, _) =>
            if (query.intersects2(feature)) ans += ((feature, m_data))
            visit+=1
        }
      }
    }
    println("R2Tree visited"+visit)
    ans.toArray
  }
}

object R2Tree {

  def apply(entries: Array[(MBBC, Int, Int)], max_entries_per_node: Int): R2Tree = { //Int,Int for Id and size
    val dimension = Array(2,2)
    val entries_len = entries.length.toDouble
    val dim = new Array[Int](dimension.sum)
    var remaining = entries_len / max_entries_per_node
    for (i <- 0 until dimension.sum) {
      dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension.sum - i))).toInt
      remaining /= dim(i)
    }

    def compMBR(dim: Int)(left: MBR, right: MBR): Boolean = {
      val left_center = left.low.coord(dim) + left.high.coord(dim)
      val right_center = right.low.coord(dim) + right.high.coord(dim)
      left_center < right_center
    }
    def compMBBC(dim: Int)(left: (MBBC, Int, Int), right: (MBBC, Int, Int)): Boolean = {
      if(dim== 0 ||dim==1){
        return compMBR(dim)(left._1.start,right._1.start)
      }else if(dim==2 || dim==3){
        return compMBR(dim-2)(left._1.end,right._1.end)
      }else{
        return false
      }
    }

    def recursiveGroupMBBC(entries: Array[(MBBC, Int, Int)], cur_dim: Int, until_dim: Int)
    : Array[Array[(MBBC, Int, Int)]] = {
      val len = entries.length.toDouble
      val grouped = entries.sortWith(compMBBC(cur_dim))
        .grouped(Math.ceil(len / dim(cur_dim)).toInt).toArray
      if (cur_dim < until_dim) {
        grouped.flatMap(now => recursiveGroupMBBC(now, cur_dim + 1, until_dim))
      } else grouped
    }

    val grouped = recursiveGroupMBBC(entries, 0, dimension.sum - 1)
    val R2Tree_nodes = mutable.ArrayBuffer[(MBBC, R2TreeNode)]()
    grouped.foreach(list => {
      val min = new Array[Double](dimension(1)).map(x => Double.MaxValue)
      val max = new Array[Double](dimension(1)).map(x => Double.MinValue)
      val min1 = new Array[Double](dimension(1)).map(x => Double.MaxValue)
      val max1 = new Array[Double](dimension(1)).map(x => Double.MinValue)
      val min2 = new Array[Double](dimension(1)).map(x => Double.MaxValue)
      val max2 = new Array[Double](dimension(1)).map(x => Double.MinValue)
      var maxspeed = 0.0
      list.foreach(now => {
        for (i <- 0 until dimension(1)) {
          min(i) = Math.min(min(i), now._1.mbr.low.coord(i))
          max(i) = Math.max(max(i), now._1.mbr.high.coord(i))
          min1(i) = Math.min(min1(i), now._1.start.low.coord(i))
          max1(i) = Math.max(max1(i), now._1.start.high.coord(i))
          min2(i) = Math.min(min2(i), now._1.end.low.coord(i))
          max2(i) = Math.max(max2(i), now._1.end.high.coord(i))
        }
        maxspeed=Math.max(maxspeed,now._1.maxspeed)
      })
      val mbbc = MBBC(MBR(new Point(min1),new Point(max1)),
        MBR(new Point(min2),new Point(max2)),
        maxspeed,MBR(new Point(min), new Point(max)))
      R2Tree_nodes += ((mbbc, new R2TreeNode(mbbc, list)))
    })

    var cur_R2Tree_nodes = R2Tree_nodes.toArray
    var cur_len = cur_R2Tree_nodes.length.toDouble
    remaining = cur_len / max_entries_per_node
    for (i <- 0 until dimension.sum) {
      dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension.sum - i))).toInt
      remaining /= dim(i)
    }

    def over(dim : Array[Int]) : Boolean = {
      for (i <- dim.indices)
        if (dim(i) != 1) return false
      true
    }

    def comp(dim: Int)(left : (MBBC, R2TreeNode), right : (MBBC, R2TreeNode)) : Boolean = {
      if(dim== 0 ||dim==1){
        return compMBR(dim)(left._1.start,right._1.start)
      }else if(dim==2 || dim==3){
        return compMBR(dim-2)(left._1.end,right._1.end)
      }else{
        return false
      }
    }

    def recursiveGroupR2TreeNode(entries: Array[(MBBC, R2TreeNode)],
                                cur_dim : Int, until_dim : Int) : Array[Array[(MBBC, R2TreeNode)]] = {
      val len = entries.length.toDouble
      val grouped = entries.sortWith(comp(cur_dim))
        .grouped(Math.ceil(len / dim(cur_dim)).toInt).toArray
      if (cur_dim < until_dim) {
        grouped.flatMap(now => recursiveGroupR2TreeNode(now, cur_dim + 1, until_dim))
      } else grouped
    }

    while (!over(dim)) {
      val grouped = recursiveGroupR2TreeNode(cur_R2Tree_nodes, 0, dimension.sum - 1)
      var tmp_nodes = mutable.ArrayBuffer[(MBBC, R2TreeNode)]()
      grouped.foreach(list => {
        val min = new Array[Double](dimension(1)).map(x => Double.MaxValue)
        val max = new Array[Double](dimension(1)).map(x => Double.MinValue)
        val min1 = new Array[Double](dimension(1)).map(x => Double.MaxValue)
        val max1 = new Array[Double](dimension(1)).map(x => Double.MinValue)
        val min2 = new Array[Double](dimension(1)).map(x => Double.MaxValue)
        val max2 = new Array[Double](dimension(1)).map(x => Double.MinValue)
        var maxspeed = 0.0
        list.foreach(now => {
          for (i <- 0 until dimension(1)) {
            min(i) = Math.min(min(i), now._1.mbr.low.coord(i))
            max(i) = Math.max(max(i), now._1.mbr.high.coord(i))
            min1(i) = Math.min(min1(i), now._1.start.low.coord(i))
            max1(i) = Math.max(max1(i), now._1.start.high.coord(i))
            min2(i) = Math.min(min2(i), now._1.end.low.coord(i))
            max2(i) = Math.max(max2(i), now._1.end.high.coord(i))
          }
          maxspeed=Math.max(maxspeed,now._1.maxspeed)
        })
        val mbbc = MBBC(MBR(new Point(min1),new Point(max1)),
          MBR(new Point(min2),new Point(max2)),
          maxspeed,MBR(new Point(min), new Point(max)))
        tmp_nodes += ((mbbc, new R2TreeNode(mbbc, list)))
      })
      cur_R2Tree_nodes = tmp_nodes.toArray
      cur_len = cur_R2Tree_nodes.length.toDouble
      remaining = cur_len / max_entries_per_node
      for (i <- 0 until dimension.sum) {
        dim(i) = Math.ceil(Math.pow(remaining, 1.0 / (dimension.sum - i))).toInt
        remaining /= dim(i)
      }
    }

    val min = new Array[Double](dimension(1)).map(x => Double.MaxValue)
    val max = new Array[Double](dimension(1)).map(x => Double.MinValue)
    val min1 = new Array[Double](dimension(1)).map(x => Double.MaxValue)
    val max1 = new Array[Double](dimension(1)).map(x => Double.MinValue)
    val min2 = new Array[Double](dimension(1)).map(x => Double.MaxValue)
    val max2 = new Array[Double](dimension(1)).map(x => Double.MinValue)
    var maxspeed = 0.0
    cur_R2Tree_nodes.foreach(now => {
      for (i <- 0 until dimension(1)) {
        min(i) = Math.min(min(i), now._1.mbr.low.coord(i))
        max(i) = Math.max(max(i), now._1.mbr.high.coord(i))
        min1(i) = Math.min(min1(i), now._1.start.low.coord(i))
        max1(i) = Math.max(max1(i), now._1.start.high.coord(i))
        min2(i) = Math.min(min2(i), now._1.end.low.coord(i))
        max2(i) = Math.max(max2(i), now._1.end.high.coord(i))
      }
      maxspeed=Math.max(maxspeed,now._1.maxspeed)
    })

    val mbbc = MBBC(MBR(new Point(min1),new Point(max1)),
      MBR(new Point(min2),new Point(max2)),
      maxspeed,MBR(new Point(min), new Point(max)))
    val root = new R2TreeNode(mbbc, cur_R2Tree_nodes)
    new R2Tree(root)
  }
}