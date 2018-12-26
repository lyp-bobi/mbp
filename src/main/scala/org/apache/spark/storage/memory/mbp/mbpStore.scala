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

package org.apache.spark.storage.memory.mbp

import org.apache.spark.SparkConf
import org.apache.spark.memory.{MemoryManager, MemoryMode}
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.storage.{BlockId, BlockInfoManager}
import org.apache.spark.storage.memory.{BlockEvictionHandler, MemoryStore}

class mbpStore (
                 conf: SparkConf,
                 blockInfoManager: BlockInfoManager,
                 serializerManager: SerializerManager,
                 memoryManager: MemoryManager,
                 blockEvictionHandler: BlockEvictionHandler)
  extends MemoryStore (conf, blockInfoManager, serializerManager,
    memoryManager, blockEvictionHandler: BlockEvictionHandler) {
  override def evictBlocksToFreeSpace(blockId: Option[BlockId],
                                      space: Long,
                                      memoryMode: MemoryMode): Long =
    super.evictBlocksToFreeSpace(blockId, space, memoryMode)

}
