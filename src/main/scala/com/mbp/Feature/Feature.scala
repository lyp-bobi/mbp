package com.mbp.Feature

abstract class Feature extends Serializable  {
  def minDist(other: Feature): Double

  def intersects(other: Feature): Boolean
}