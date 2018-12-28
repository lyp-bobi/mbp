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

import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Literal, PredicateHelper, UnsafeProjection}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

@Deprecated case class B_RTreeRelationScan(attributes: Seq[Attribute],
                          predicates: Seq[Expression],
                          relation: RTreeRelation) extends SparkPlan with PredicateHelper{
  override def output: Seq[Attribute] = attributes
  override def children:Seq[SparkPlan] = Nil // for UnaryNode
  override protected def doExecute(): RDD[InternalRow] = null
}

@Deprecated object B_RTreeRelationScanStrategy extends Strategy with PredicateHelper{
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = super.apply(plan)
}