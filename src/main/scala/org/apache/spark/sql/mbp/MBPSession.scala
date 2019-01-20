package org.apache.spark.sql.mbp

import org.apache.spark.sql.{DataFrame=>SQLDataFrame, Dataset=>SQLDataset, Encoder,SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

class MBPSession private[mbp](@transient val mbpContext: MBPContext)
  extends SparkSession(mbpContext){
  self=>
  protected[simba] val indexManager: IndexManager = new IndexManager

  def executePlan(plan: LogicalPlan)= new execution.QueryExecution(self, plan)


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
    protected[mbp] def _simbaContext: SparkSession = self

    implicit def datasetToMBPDataSet[T : Encoder](ds: SQLDataset[T]): Dataset[T] =
      Dataset(self, ds.queryExecution.logical)

    implicit def dataframeToMBPDataFrame(df: SQLDataFrame): SQLDataFrame =
      Dataset.ofRows(self, df.queryExecution.logical)
  }
}