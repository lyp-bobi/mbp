package org.apache.spark.sql

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import org.apache.spark.sql.mbp.SessionProvider

class mbpTest extends FunSuite with BeforeAndAfter{
  before{
    val ss=SessionProvider.getSession()
  }
  test("BoomBoomPow"){

  }
}
