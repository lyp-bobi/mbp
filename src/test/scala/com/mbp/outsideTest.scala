package com.mbp
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import com.mbp.Feature._
import com.mbp.index._
import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat

import scala.collection.mutable


class outsideTest extends FunSuite with BeforeAndAfter{
  /*
  test("group points to mbrs"){
    val file=Source.fromFile("D://AIS_2017_01_Zone03.csv")
    val lines = file.getLines().drop(1).toArray
    val points = new ArrayBuffer[Point]
    for(line<-lines){
      var arr= line.split(",")
      var y = arr(2).toDouble
      var x = arr(3).toDouble
      points.append(new Point(x,y,0))
    }
    val writer = new PrintWriter(new File("./mbrs.txt"))
    writer.flush()
    val mbrs=RTree.groupPointToMBR(points.zipWithIndex.toArray,3)
    var lowx= 180.0
    var highx= -180.0
    var lowy= 90.0
    var highy= -90.0
    var count =0
    for(mbr<-mbrs){
      if(mbr.low.coord.x<lowx) lowx=mbr.low.coord.x
      if(mbr.high.coord.x>highx) highx=mbr.high.coord.x
      if(mbr.low.coord.y<lowy) lowy=mbr.low.coord.y
      if(mbr.high.coord.y>highy) highy=mbr.high.coord.y
      writer.println(mbr.toString)
      count +=1
      println(mbr.toString+count.toString)
    }
    println(lowx,lowy,highx,highy,mbrs.length)
    writer.close()
  }
  */
  /*
  test("trajectory segmentation"){
    val file=Source.fromFile("./src/test/scala/com/mbp/220593000.csv")
    val lines = file.getLines().drop(1).toArray
    val fm=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    val points=new mutable.ArrayBuffer[Point]()
    for(line<-lines){
      val arr=line.split(",")
      val tm =fm.parse(arr(0)).getTime
      points.append(new Point(arr(2).toDouble,arr(1).toDouble,tm))
    }
    val traj = new Trajectory(points)
    val (tlow,thigh)=traj.caltime()
    val td = new timeDivision(tlow,thigh)
    print(tlow,thigh)
    traj.segmentate(td)
    println(traj.segments)
    println(traj.segments.length)
  }
  */

  test("cross query"){

    val file=Source.fromFile("D://AIS_2017_01_Zone03.csv")
    val lines = file.getLines().drop(1).toArray
    val points = new ArrayBuffer[(Long,Point)]
    val td = new timeDivision(1483200000000L,1485878400000L)
    val fm=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    for(line<-lines){
      var arr= line.split(",")
      var id = arr(0).toLong
      var t = fm.parse(arr(1)).getTime
      var y = arr(2).toDouble
      var x = arr(3).toDouble
      points.append((id,new Point(x,y,t)))
    }
    val trajs = points.groupBy(rec=>rec._1).map(traj=>new Trajectory(traj._2.map(tp=>tp._2),traj._1)).toArray.zipWithIndex
    for(traj<-trajs){
      traj._1.segmentate(td)
    }
    val query=MBR(new Point(-168,53,1484719231000L),new Point(-166,55,1484719231000L))
    val hrtree=HRTree(trajs,3,td)
    val res1=hrtree.cross(query)
    val hr2tree=HR2Tree(trajs,3,td)
    val res2=hr2tree.cross(query)

  }

}
