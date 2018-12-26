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

import scala.reflect.ClassTag

import org.apache.spark.{MapOutputTracker, SecurityManager, SparkConf}
import org.apache.spark.memory.MemoryManager
import org.apache.spark.network.BlockTransferService
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.storage.{BlockId, BlockManager, BlockManagerMaster, BlockResult}


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
  extends BlockManager(
    executorId, rpcEnv, master, serializerManager, conf: SparkConf,
    memoryManager, mapOutputTracker, shuffleManager, blockTransferService,
    securityManager, numUsableCores) {
  override def get[T: ClassTag](blockId: BlockId): Option[BlockResult] = super.get(blockId)

}
