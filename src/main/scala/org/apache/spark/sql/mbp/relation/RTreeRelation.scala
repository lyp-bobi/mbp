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

package org.apache.spark.sql.mbp.relation

import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.storage.StorageLevel

//I think maybe we don't have to use Relation( =  local index)

case class RTreeRelation(output: Seq[Attribute], child: SparkPlan, table_name: Option[String],
                         column_keys: List[Attribute], index_name: String)
  extends LogicalPlan with MultiInstanceRelation{

  override def children: Seq[LogicalPlan] = Nil

  override def newInstance(): RTreeRelation = {
    // TODO: the rand part should be replace by the RDD id of Index
    RTreeRelation(output.map(_.newInstance()), child, table_name,
      column_keys, index_name).asInstanceOf[this.type]
  }
}
