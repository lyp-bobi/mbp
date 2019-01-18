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
package org.apache.spark.storage.mbp


import org.apache.spark.storage._
import org.apache.spark.storage.memory.mbp.mbpMemoryStore

import org.apache.spark._
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.memory.{MemoryManager, MemoryMode}

import org.apache.spark.network._

import org.apache.spark.rpc.RpcEnv
import org.apache.spark.serializer.{SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.storage.memory._

import org.apache.spark.util._

class mbpBlockManager(
                       executorId: String,
                       rpcEnv: RpcEnv,
                       override val master: BlockManagerMaster,
                       override val serializerManager: SerializerManager,
                       override val conf: SparkConf,
                       memoryManager: MemoryManager,
                       mapOutputTracker: MapOutputTracker,
                       shuffleManager: ShuffleManager,
                       override val blockTransferService: BlockTransferService,
                       securityManager: SecurityManager,
                       numUsableCores: Int)
  extends BlockManager (
    executorId, rpcEnv, master, serializerManager, conf: SparkConf,
    memoryManager, mapOutputTracker, shuffleManager, blockTransferService,
    securityManager, numUsableCores) with BlockDataManager with BlockEvictionHandler with Logging{
  // Actual storage of where blocks are kept
  private[spark] override val memoryStore =
    new mbpMemoryStore(conf, blockInfoManager, serializerManager, memoryManager, this)
  memoryManager.setMemoryStore(memoryStore)
}



