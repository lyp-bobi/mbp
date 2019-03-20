package org.apache.spark.storage
import org.apache.spark.SparkConf
import org.apache.spark.memory.UnifiedMemoryManager
import org.apache.spark.serializer.{KryoSerializer, SerializerManager}
import org.apache.spark.storage.memory.{BlockEvictionHandler, MemoryStore}
import org.apache.spark.util.io.ChunkedByteBuffer
import org.scalatest._

import scala.reflect.ClassTag

class mbpMemoryStoreSuite {
  var conf: SparkConf = new SparkConf(false).set(STORAGE_UNROLL_MEMORY_THRESHOLD, 512L)

  // Reuse a serializer across tests to avoid creating a new thread-local buffer on each test
  val serializer = new KryoSerializer(new SparkConf(false).set("spark.kryoserializer.buffer", "1m"))
  val serializerManager = new SerializerManager(serializer, conf)
  def makeMemoryStore(maxMem: Long): (MemoryStore, BlockInfoManager) = {
    val memManager = new UnifiedMemoryManager(conf, maxMem, maxMem / 2, 1)
    val blockInfoManager = new BlockInfoManager
    val blockEvictionHandler = new BlockEvictionHandler {
      var memoryStore: MemoryStore = _
      override private[storage] def dropFromMemory[T: ClassTag](
                                                                 blockId: BlockId,
                                                                 data: () => Either[Array[T], ChunkedByteBuffer]): StorageLevel = {
        memoryStore.remove(blockId)
        StorageLevel.NONE
      }
    }
    val memoryStore =
      new MemoryStore(conf, blockInfoManager, serializerManager, memManager, blockEvictionHandler)
    memManager.setMemoryStore(memoryStore)
    blockEvictionHandler.memoryStore = memoryStore
    (memoryStore, blockInfoManager)
  }

}
