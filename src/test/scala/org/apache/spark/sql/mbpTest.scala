package org.apache.spark.sql

import org.apache.spark.mbp.mbpContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import org.apache.spark.sql.mbp.SessionProvider
import org.apache.spark.storage.StorageLevel
/*
Procedure:
  1. load a file with (id,x,y,t)
  2. use "MKTRAJ" to get a Trajectory
    2.1 groupby "id",and get (id, List[(x,y,t)])
    2.2 divide by timeperiod(should be stated in sparkConf), and get class[Segment]s
    2.3 use class[Segment]s to form class[Trajectory]
  3. use the B_RTreePartitioner to get a Global index
  3. query with a SQL like "SELECT * FROM df WHERE df.traj in CIRCLERANGE(POINT(x1,y2),POINT(x2,y2)"
    3.1 get a LogicalPlan from Parser
    3.2 get a OptimizedLogicalPlan by applying RTreeRelation(is this global or local?)
    3.3 get PhysicalPlan with RTreeRelationStrategy
    3.4 errr how to use global index here?
  4. Read Blocks from mbpBlockManager.
 */

class mbpTest extends FunSuite with BeforeAndAfter{
  var sc:SparkContext=_
  var ss:SparkSession=_
  before{
    val conf = new SparkConf().setMaster("local").setAppName("test")
    sc= new mbpContext(conf)
    ss= SessionProvider.getOrCreateSession(conf)
  }
  // TODO: Test the Session Provider
  test("read and store data"){
    val df=ss.read.csv("/home/chuang/43676060.csv")
    df.persist(StorageLevel.DISK_ONLY)
    df.count()
  }
}
