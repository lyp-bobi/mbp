/*
 * Copyright 2017 by mbp Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.spark.sql.mbp

import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal._
import org.apache.spark.{ExecutorAllocationClient, SparkConf, SparkContext, SparkEnv}
import org.apache.spark.scheduler.local.LocalSchedulerBackend
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.parser.mbp.mbpCatalystSqlParser
import org.apache.spark.sql.execution.mbp.mbpOptimizer
//import org.apache.spark.sql.mbp.relation.B_RTreeRelationScanStrategy
import org.apache.spark.storage.mbp.mbpBlockManager
import org.apache.spark.launcher.SparkAppHandle

/*
The only interface to interact with spark
 */

object SessionProvider {
  val pvd=new SessionProvider()
  def getOrCreateSession(conf:SparkConf):SparkSession=pvd.getOrInit(conf)
  def getOrCreateSession():SparkSession=pvd.get()
}

class SessionProvider private(var ss: SparkSession=null) extends Logging{
  def get(): SparkSession={
    if(ss == null){
      val sc = SparkContext.getActive
      if(sc.isEmpty){
        logError("You got a null as sparkSession because we don't have one." +
          " Try SessionProvider.getSession(conf:SparkConf) to initialize one. ")
        return null
      }
      else{
        ss=getOrInit(sc.get.getConf)
      }
    }
    ss
  }
  def getOrInit(conf: SparkConf): SparkSession = {
    if(ss==null){
      val sc = SparkContext.getOrCreate(conf)
      val oldss = SparkSession.getDefaultSession
      def injection(extensions:SparkSessionExtensions):Unit = {
        // use a self defined parser to parse the sql trees
        extensions.injectParser((_, _) => mbpCatalystSqlParser)
        // use a Rule to replace Relations with B_RTreeRelation at here
        extensions.injectOptimizerRule(mbpOptimizer)
        // implement the strategy that parse the B_RTreeRelation to B_RTreeRelationScan
//        extensions.injectPlannerStrategy(_=>B_RTreeRelationScanStrategy)
      }

      if(oldss.isDefined){
        ss= oldss.get
        injection(ss.extensions)
      }
      else{
        ss = SparkSession.builder().sparkContext(sc)
          .withExtensions(injection)
          .getOrCreate()
      }

    }
    ss
  }
}
