
package com.mbp.Feature

class Point(coord: Array[Double]) extends Feature {
  override val dimensions: Int = coord.length
  def this() = this(Array())

  //  override def getMBR: MBR = MBR(this, this)
}