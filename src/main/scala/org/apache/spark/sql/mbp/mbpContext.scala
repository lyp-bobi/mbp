package org.apache.spark.sql.mbp

import java.util.Locale

import org.apache.spark._
import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.memory.{MemoryManager, StaticMemoryManager, UnifiedMemoryManager}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.network.netty.NettyBlockTransferService
import org.apache.spark.rpc.{RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.OutputCommitCoordinator.OutputCommitCoordinatorEndpoint
import org.apache.spark.scheduler.{LiveListenerBus, OutputCommitCoordinator}
import org.apache.spark.security.CryptoStreamUtils
import org.apache.spark.serializer.{JavaSerializer, Serializer, SerializerManager}
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.storage._
import org.apache.spark.storage.mbp.mbpBlockManager
import org.apache.spark.util.{RpcUtils, Utils}

class mbpContext(val config:SparkConf) extends SparkContext(config) with Logging {

  private[spark] override def createSparkEnv(
                                              conf: SparkConf,
                                              isLocal: Boolean,
                                              listenerBus: LiveListenerBus): SparkEnv = {
    mbpCreateDriverEnv(conf, isLocal, listenerBus, SparkContext.numDriverCores(master))
  }
  private[spark] def mbpCreateDriverEnv(
                                       conf: SparkConf,
                                       isLocal: Boolean,
                                       listenerBus: LiveListenerBus,
                                       numCores: Int,
                                       mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {
    assert(conf.contains(DRIVER_HOST_ADDRESS),
      s"${DRIVER_HOST_ADDRESS.key} is not set on the driver!")
    assert(conf.contains("spark.driver.port"), "spark.driver.port is not set on the driver!")
    val bindAddress = conf.get(DRIVER_BIND_ADDRESS)
    val advertiseAddress = conf.get(DRIVER_HOST_ADDRESS)
    val port = conf.get("spark.driver.port").toInt
    val ioEncryptionKey = if (conf.get(IO_ENCRYPTION_ENABLED)) {
      Some(CryptoStreamUtils.createKey(conf))
    } else {
      None
    }
    create(
      conf,
      SparkContext.DRIVER_IDENTIFIER,
      bindAddress,
      advertiseAddress,
      Option(port),
      isLocal,
      numCores,
      ioEncryptionKey,
      listenerBus = listenerBus,
      mockOutputCommitCoordinator = mockOutputCommitCoordinator
    )
  }
  /**
    * Helper method to create a SparkEnv for a driver or an executor.
    */
  private def create(
                      conf: SparkConf,
                      executorId: String,
                      bindAddress: String,
                      advertiseAddress: String,
                      port: Option[Int],
                      isLocal: Boolean,
                      numUsableCores: Int,
                      ioEncryptionKey: Option[Array[Byte]],
                      listenerBus: LiveListenerBus = null,
                      mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {

    val isDriver = executorId == SparkContext.DRIVER_IDENTIFIER

    // Listener bus is only used on the driver
    if (isDriver) {
      assert(listenerBus != null, "Attempted to create driver SparkEnv with null listener bus!")
    }

    val securityManager = new SecurityManager(conf, ioEncryptionKey)
    if (isDriver) {
      securityManager.initializeAuth()
    }

    ioEncryptionKey.foreach { _ =>
      if (!securityManager.isEncryptionEnabled()) {
        logWarning("I/O encryption enabled without RPC encryption: keys will be visible on the " +
          "wire.")
      }
    }

    val systemName = if (isDriver) SparkEnv.driverSystemName else SparkEnv.executorSystemName
    val rpcEnv = RpcEnv.create(systemName, bindAddress, advertiseAddress, port.getOrElse(-1), conf,
      securityManager, numUsableCores, !isDriver)

    // Figure out which port RpcEnv actually bound to in case the original port is 0 or occupied.
    if (isDriver) {
      conf.set("spark.driver.port", rpcEnv.address.port.toString)
    }

    // Create an instance of the class with the given name, possibly initializing it with our conf
    def instantiateClass[T](className: String): T = {
      val cls = Utils.classForName(className)
      // Look for a constructor taking a SparkConf and a boolean isDriver, then one taking just
      // SparkConf, then one taking no arguments
      try {
        cls.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
          .newInstance(conf, new java.lang.Boolean(isDriver))
          .asInstanceOf[T]
      } catch {
        case _: NoSuchMethodException =>
          try {
            cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]
          } catch {
            case _: NoSuchMethodException =>
              cls.getConstructor().newInstance().asInstanceOf[T]
          }
      }
    }

    // Create an instance of the class named by the given SparkConf property, or defaultClassName
    // if the property is not set, possibly initializing it with our conf
    def instantiateClassFromConf[T](propertyName: String, defaultClassName: String): T = {
      instantiateClass[T](conf.get(propertyName, defaultClassName))
    }

    val serializer = instantiateClassFromConf[Serializer](
      "spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    logDebug(s"Using serializer: ${serializer.getClass}")

    val serializerManager = new SerializerManager(serializer, conf, ioEncryptionKey)

    val closureSerializer = new JavaSerializer(conf)

    def registerOrLookupEndpoint(
                                  name: String, endpointCreator: => RpcEndpoint):
    RpcEndpointRef = {
      if (isDriver) {
        logInfo("Registering " + name)
        rpcEnv.setupEndpoint(name, endpointCreator)
      } else {
        RpcUtils.makeDriverRef(name, conf, rpcEnv)
      }
    }

    val broadcastManager = new BroadcastManager(isDriver, conf, securityManager)

    val mapOutputTracker = if (isDriver) {
      new MapOutputTrackerMaster(conf, broadcastManager, isLocal)
    } else {
      new MapOutputTrackerWorker(conf)
    }

    // Have to assign trackerEndpoint after initialization as MapOutputTrackerEndpoint
    // requires the MapOutputTracker itself
    mapOutputTracker.trackerEndpoint = registerOrLookupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(
        rpcEnv, mapOutputTracker.asInstanceOf[MapOutputTrackerMaster], conf))

    // Let the user specify short names for shuffle managers
    val shortShuffleMgrNames = Map(
      "sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName,
      "tungsten-sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName)
    val shuffleMgrName = conf.get("spark.shuffle.manager", "sort")
    val shuffleMgrClass =
      shortShuffleMgrNames.getOrElse(shuffleMgrName.toLowerCase(Locale.ROOT), shuffleMgrName)
    val shuffleManager = instantiateClass[ShuffleManager](shuffleMgrClass)

    val useLegacyMemoryManager = conf.getBoolean("spark.memory.useLegacyMode", defaultValue = false)
    val memoryManager: MemoryManager =
      if (useLegacyMemoryManager) {
        new StaticMemoryManager(conf, numUsableCores)
      } else {
        UnifiedMemoryManager(conf, numUsableCores)
      }

    val blockManagerPort = if (isDriver) {
      conf.get(DRIVER_BLOCK_MANAGER_PORT)
    } else {
      conf.get(BLOCK_MANAGER_PORT)
    }

    val blockTransferService =
      new NettyBlockTransferService(conf, securityManager, bindAddress, advertiseAddress,
        blockManagerPort, numUsableCores)

    val blockManagerMaster = new BlockManagerMaster(registerOrLookupEndpoint(
      BlockManagerMaster.DRIVER_ENDPOINT_NAME,
      new BlockManagerMasterEndpoint(rpcEnv, isLocal, conf, listenerBus)),
      conf, isDriver)

    // NB: blockManager is not valid until initialize() is called later.
    val blockManager = new mbpBlockManager(executorId, rpcEnv, blockManagerMaster,
      serializerManager, conf, memoryManager, mapOutputTracker, shuffleManager,
      blockTransferService, securityManager, numUsableCores)

    val metricsSystem = if (isDriver) {
      // Don't start metrics system right now for Driver.
      // We need to wait for the task scheduler to give us an app ID.
      // Then we can start the metrics system.
      MetricsSystem.createMetricsSystem("driver", conf, securityManager)
    } else {
      // We need to set the executor ID before the MetricsSystem is created because sources and
      // sinks specified in the metrics configuration file will want to incorporate this executor's
      // ID into the metrics they report.
      conf.set("spark.executor.id", executorId)
      val ms = MetricsSystem.createMetricsSystem("executor", conf, securityManager)
      ms.start()
      ms
    }

    val outputCommitCoordinator = mockOutputCommitCoordinator.getOrElse {
      new OutputCommitCoordinator(conf, isDriver)
    }
    val outputCommitCoordinatorRef = registerOrLookupEndpoint("OutputCommitCoordinator",
      new OutputCommitCoordinatorEndpoint(rpcEnv, outputCommitCoordinator))
    outputCommitCoordinator.coordinatorRef = Some(outputCommitCoordinatorRef)

    val envInstance = new SparkEnv(
      executorId,
      rpcEnv,
      serializer,
      closureSerializer,
      serializerManager,
      mapOutputTracker,
      shuffleManager,
      broadcastManager,
      blockManager,
      securityManager,
      metricsSystem,
      memoryManager,
      outputCommitCoordinator,
      conf)

    // Add a reference to tmp dir created by driver, we will delete this tmp dir when stop() is
    // called, and we only need to do it for driver. Because driver may run as a service, and if we
    // don't delete this tmp dir when sc is stopped, then will create too many tmp dirs.
    if (isDriver) {
      val sparkFilesDir = Utils.createTempDir(Utils.getLocalDir(conf), "userFiles").getAbsolutePath
      envInstance.driverTmpDir = Some(sparkFilesDir)
    }

    envInstance
  }
}