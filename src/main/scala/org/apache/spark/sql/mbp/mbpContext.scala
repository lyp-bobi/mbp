package org.apache.spark.sql.mbp

import org.apache.spark.scheduler.LiveListenerBus
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}

class mbpContext extends SparkContext {
  // TODO: create a SparkEnv by mbpBlockManager :D
  override def createSparkEnv(conf: SparkConf, isLocal: Boolean, listenerBus: LiveListenerBus): SparkEnv = super.createSparkEnv(conf, isLocal, listenerBus)
}
