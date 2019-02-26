package org.apache.spark.sql.mbp

import org.apache.spark.SparkConf
import org.apache.spark.mbp.{mbpContext=>MBPContext}

package object examples {
  case class PointData(x: Double, y: Double, z: Double, other: String)
  case class Traj(points:Array[PointData])

  def main(args: Array[String]): Unit = {
    val mbpConf= new SparkConf().setMaster("local").setAppName("mbp")
    val mbpContext= new MBPContext(mbpConf)
    val mbpSession = new MBPSession(mbpContext)
    import mbpSession.implicits._
    val traj = Seq(Traj(Array(PointData(1.0, 1.0, 3.0, "1"),  PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
      PointData(2.0, 2.0, 3.0, "4"),PointData(3.0, 3.0, 3.0, "5"),PointData(4.0, 4.0, 3.0, "6"))),
      Traj(Array(PointData(1.0, 1.0, 3.0, "1"), PointData(2.0, 2.0, 3.0, "3"),
        PointData(2.0, 2.0, 3.0, "4"),PointData(3.0, 3.0, 3.0, "5")))
    )
    val ds=traj.toDS()
    import mbpSession.mbpImplicits._
    ds.range(Array(1,2),Array(3,4))

  }
}
