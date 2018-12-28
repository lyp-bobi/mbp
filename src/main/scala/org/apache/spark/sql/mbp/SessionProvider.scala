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

import org.apache.spark.{SparkEnv, SparkContext}
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.mbp.relation.mbpOptimizer
import org.apache.spark.sql.catalyst.parser.mbp.{MbpCatalystSqlParser}
import org.apache.spark.sql.mbp.relation.B_RTreeRelationScanStrategy
import org.apache.spark.storage.mbp.mbpBlockManager
import org.apache.spark.sql.mbp.mbpContext

/*
The only interface to interact with spark
 */

object SessionProvider {
  val pvd=new SessionProvider()
  def getSession()=pvd.getOrInit()
}

class SessionProvider private(var ss: SparkSession=null) {
  type ExtensionsBuilder = SparkSessionExtensions => Unit
  private def create(builder: ExtensionsBuilder): ExtensionsBuilder = builder
  def getOrInit(): SparkSession = {
    if(ss==null){
      val mbpsc = new mbpContext()
      ss = SparkSession.builder().sparkContext(mbpsc).master("...").config("...", true)
         .withExtensions(extensions =>{
           // TODO: use a self defined parser to parse the sql trees
           extensions.injectParser((_, _) => MbpCatalystSqlParser)
           // TODO: use a Rule to replace Relations with B_RTreeRelation at here
           extensions.injectOptimizerRule(mbpOptimizer)
           // TODO: implement the strategy that parse the B_RTreeRelation to B_RTreeRelationScan
           extensions.injectPlannerStrategy(_=>B_RTreeRelationScanStrategy)
         })
        .getOrCreate()
    }
    ss
  }
}
