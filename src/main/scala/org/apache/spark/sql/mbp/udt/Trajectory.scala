package org.apache.spark.sql.mbp.udt

class Trajectory(val segments: List[Segment]) extends Feature {
  override val dimensions: Int = segments.head.dimensions
}


class Segment extends Feature {
  override val dimensions: Int = 2
}