package org.apache.spark.sql.mbp

import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.{Encoder, SparkSession, Strategy, DataFrame => SQLDataFrame, Dataset => SQLDataset}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.mbp.{FilterExec, QueryExecution}
import org.apache.spark.sql.execution.{SparkPlan, SparkPlanner}
import org.apache.spark.sql.execution.mbp.MBPFilter.planLater
import org.apache.spark.mbp.{mbpContext=>MBPContext}

class MBPSession private[mbp](@transient val mbpContext: MBPContext)
  extends SparkSession(mbpContext){
  self=>
  protected[mbp] val indexManager: IndexManager = new IndexManager

  def executePlan(plan: LogicalPlan)= new QueryExecution(self, plan)


  /*def indexTable(tableName: String, indexType: IndexType,
                 indexName: String, column: Array[String]): Unit = {
    val tbl = table(tableName)
    assert(tbl != null, "Table not found")
    val attrs = tbl.queryExecution.analyzed.output
    val columnKeys = column.map(attr => {
      var ans: Attribute = null
      for (i <- attrs.indices)
        if (attrs(i).name.equals(attr)) ans = attrs(i)
      assert(ans != null, "Attribute not found")
      ans
    }).toList
    sessionState.indexManager.createIndexQuery(table(tableName), indexType,
      indexName, columnKeys, Some(tableName))
  }

  def showIndex(tableName: String): Unit = sessionState.indexManager.showQuery(tableName)

  def persistIndex(indexName: String, fileName: String): Unit =
    sessionState.indexManager.persistIndex(this, indexName, fileName)

  def loadIndex(indexName: String, fileName: String): Unit =
    sessionState.indexManager.loadIndex(this, indexName, fileName)

  def dropIndexTableByName(tableName: String, indexName: String): Unit = {
    sessionState.indexManager.dropIndexByNameQuery(table(tableName), indexName)
  }

  def clearIndex(): Unit = sessionState.indexManager.clearIndex()
  */
  object mbpImplicits extends Serializable {
    protected[mbp] def _mbpContext: SparkSession = self

    implicit def datasetToMBPDataSet[T : Encoder](ds: SQLDataset[T]): Dataset[T] =
      Dataset(self, ds.queryExecution.logical)

    implicit def dataframeToMBPDataFrame(df: SQLDataFrame): DataFrame =
      Dataset.ofRows(self, df.queryExecution.logical)
  }
}

object MBPSession{
  def setActiveSession(session: MBPSession): Unit = {
    activeThreadSession.set(session)
  }

  private val activeThreadSession = new InheritableThreadLocal[MBPSession]

}
