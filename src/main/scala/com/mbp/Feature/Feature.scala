package com.mbp.Feature

abstract class Feature extends Serializable  {
  def minDist3(other: Feature): Double

  def intersects3(other: Feature): Boolean
}