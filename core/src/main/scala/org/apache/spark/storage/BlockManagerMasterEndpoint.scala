/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.storage

import java.util.{HashMap => JHashMap}

import scala.util.{Failure, Success}
import scala.collection.mutable
import scala.collection.mutable.{HashMap, MultiMap}
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint, RpcEndpointAddress}
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.util.{ThreadUtils, Utils, RpcUtils}
import org.apache.spark.SparkEnv

/**
 * BlockManagerMasterEndpoint is an [[ThreadSafeRpcEndpoint]] on the master node to track statuses
 * of all slaves' block managers.
 */
private[spark]
class BlockManagerMasterEndpoint(
    override val rpcEnv: RpcEnv,
    val isLocal: Boolean,
    conf: SparkConf,
    listenerBus: LiveListenerBus)
  extends ThreadSafeRpcEndpoint with Logging {

  // Mapping from block manager id to the block manager's information.
  private val blockManagerInfo = new mutable.HashMap[BlockManagerId, BlockManagerInfo]

  // Mapping from executor ID to block manager ID.
  private val blockManagerIdByExecutor = new mutable.HashMap[String, BlockManagerId]

  // Mapping from block id to the set of block managers that have the block.
  private val blockLocations = new JHashMap[BlockId, mutable.HashSet[BlockManagerId]]

  private val askThreadPool = ThreadUtils.newDaemonCachedThreadPool("block-manager-ask-thread-pool")
  private implicit val askExecutionContext = ExecutionContext.fromExecutorService(askThreadPool)

  private val topologyMapper = {
    val topologyMapperClassName = conf.get(
      "spark.storage.replication.topologyMapper", classOf[DefaultTopologyMapper].getName)
    val clazz = Utils.classForName(topologyMapperClassName)
    val mapper =
      clazz.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[TopologyMapper]
    logInfo(s"Using $topologyMapperClassName for getting topology information")
    mapper
  }

  val proactivelyReplicate = conf.get("spark.storage.replication.proactive", "false").toBoolean

  logInfo("BlockManagerMasterEndpoint up")
  // Mapping of remote block master managers and their respective endpoints
  private val remoteBlockManagerMasters = new mutable.HashMap
    [String, RpcEndpointRef]

  def addRemoteBlockManagerMaster(host: String, port: Int){
    Utils.checkHost(host, "Expected hostname")
    val driverUrl = RpcEndpointAddress(host, port, BlockManagerMaster.DRIVER_ENDPOINT_NAME).toString

    rpcEnv.asyncSetupEndpointRefByURI(driverUrl).flatMap { ref =>
      // This is a very fast action so we can use "ThreadUtils.sameThread"
      //driver = Some(ref) //hoping that we set endpoints via register response
      ref.ask[RegisterRemoteBlockManagerMasterResponse](
        RegisterRemoteBlockManagerMaster(self))
    }(ThreadUtils.sameThread).onComplete {
      // This is a very fast action so we can use "ThreadUtils.sameThread"
      case Success(msg) => Utils.tryLogNonFatalError {
        Option(self).foreach(_.send(msg)) // msg must be RegisteredRemoteBlockManagerMaster
      }
      case Failure(e) => {
        logError(s"Cannot register with driver: $driverUrl", e)
        //System.exit(1)
      }
    }(ThreadUtils.sameThread)
  }

  def deregisterFromRemoteBlockManagerMasters(){
    //tell all the remote BMMs that we're unavailablAe
    for((_,rbmm) <- remoteBlockManagerMasters) {
      rbmm.send( DeregisterRemoteBlockManagerMaster(self) )
    }
  }

  override def receive: PartialFunction[Any, Unit] = {
    case RegisteredRemoteBlockManagerMaster(remoteBlockManagerMasterRef) => 
      logInfo(s"Successfully registered $remoteBlockManagerMasterRef")
      remoteBlockManagerMasters.put(remoteBlockManagerMasterRef.address.toString,
        remoteBlockManagerMasterRef)
    case DeregisterRemoteBlockManagerMaster(remoteBlockManagerMasterRef) =>
      logInfo(s"Deregistering remote BlockManagerMaster $remoteBlockManagerMasterRef")
      remoteBlockManagerMasters.remove(
        remoteBlockManagerMasterRef.address.toString)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterBlockManager(blockManagerId, maxOnHeapMemSize, maxOffHeapMemSize, slaveEndpoint) =>
      context.reply(register(blockManagerId, maxOnHeapMemSize, maxOffHeapMemSize, slaveEndpoint))

    case AddRemoteBlockManagerMaster(host, port) =>
      addRemoteBlockManagerMaster(host, port)
      context.reply(true)

    case RegisterRemoteBlockManagerMaster(remoteBlockManagerMasterRef) =>
      logInfo(s"Registered remote black manager master $remoteBlockManagerMasterRef")
      remoteBlockManagerMasters.put(remoteBlockManagerMasterRef.address.toString,
        remoteBlockManagerMasterRef)
      context.reply(RegisteredRemoteBlockManagerMaster(self))

    case _updateBlockInfo @
        UpdateBlockInfo(blockManagerId, blockId, storageLevel, deserializedSize, size, hash, hash_took) =>
      context.reply(updateBlockInfo(blockManagerId, blockId, storageLevel, deserializedSize, size, hash, hash_took))
      listenerBus.post(SparkListenerBlockUpdated(BlockUpdatedInfo(_updateBlockInfo)))

    case GetLocations(blockId, remote) =>
      val f = Future {
        getLocations(blockId, remote)
      }
      f.onSuccess { case response => context.reply(response) }
      f.onFailure { case t: Throwable => context.sendFailure(t) }

    case GetLocationsMultipleBlockIds(blockIds, remote) =>
      val f = Future {
        getLocationsMultipleBlockIds(blockIds, remote)
      }
      f.onSuccess { case response => context.reply(response) }
      f.onFailure { case t: Throwable => context.sendFailure(t) }

    case GetRDDsUsingBlockId(blockId) =>
      context.reply(getRDDsUsingBlockId(blockId))

    case RegisterRDDUsingBlockId(blockId, rddId) =>
      context.reply(registerRDDUsingBlockId(blockId, rddId))

    case DeregisterRDDUsingBlockId(blockId, rddId) =>
      context.reply(deregisterRDDUsingBlockId(blockId, rddId))

    case GetAllRDDsUsingBlockIds() =>
      context.reply(getAllRDDsUsingBlockIds)

    case GetPeers(blockManagerId) =>
      context.reply(getPeers(blockManagerId))

    case GetExecutorEndpointRef(executorId) =>
      context.reply(getExecutorEndpointRef(executorId))

    case GetMemoryStatus =>
      context.reply(memoryStatus)

    case GetStorageStatus =>
      context.reply(storageStatus)

    case GetBlockStatus(blockId, askSlaves) =>
      context.reply(blockStatus(blockId, askSlaves))

    case GetMatchingBlockIds(filter, askSlaves) =>
      context.reply(getMatchingBlockIds(filter, askSlaves))

    case RemoveRdd(rddId) =>
      context.reply(removeRdd(rddId))

    case RemoveShuffle(shuffleId) =>
      context.reply(removeShuffle(shuffleId))

    case RemoveBroadcast(broadcastId, removeFromDriver) =>
      context.reply(removeBroadcast(broadcastId, removeFromDriver))

    case RemoveBlock(blockId) =>
      removeBlockFromWorkers(blockId)
      context.reply(true)

    case RemoveExecutor(execId) =>
      removeExecutor(execId)
      context.reply(true)

    case StopBlockManagerMaster =>
      deregisterFromRemoteBlockManagerMasters()
      context.reply(true)
      stop()

    case BlockManagerHeartbeat(blockManagerId) =>
      context.reply(heartbeatReceived(blockManagerId))

    case HasCachedBlocks(executorId) =>
      blockManagerIdByExecutor.get(executorId) match {
        case Some(bm) =>
          if (blockManagerInfo.contains(bm)) {
            val bmInfo = blockManagerInfo(bm)
            context.reply(bmInfo.cachedBlocks.nonEmpty)
          } else {
            context.reply(false)
          }
        case None => context.reply(false)
      }
  }

  private def removeRdd(rddId: Int): Future[Seq[Int]] = {
    // First remove the metadata for the given RDD, and then asynchronously remove the blocks
    // from the slaves.

    // Find all blocks for the given RDD, remove the block from both blockLocations and
    // the blockManagerInfo that is tracking the blocks.
    val blocks = blockLocations.asScala.keys.flatMap(_.asRDDId).filter(_.rddId == rddId)
    blocks.foreach { blockId =>
      val bms: mutable.HashSet[BlockManagerId] = blockLocations.get(blockId)
      bms.foreach(bm => blockManagerInfo.get(bm).foreach(_.removeBlock(blockId)))
      blockLocations.remove(blockId)
    }

    // Ask the slaves to remove the RDD, and put the result in a sequence of Futures.
    // The dispatcher is used as an implicit argument into the Future sequence construction.
    val removeMsg = RemoveRdd(rddId)
    Future.sequence(
      blockManagerInfo.values.map { bm =>
        bm.slaveEndpoint.ask[Int](removeMsg)
      }.toSeq
    )
  }

  private def removeShuffle(shuffleId: Int): Future[Seq[Boolean]] = {
    // Nothing to do in the BlockManagerMasterEndpoint data structures
    val removeMsg = RemoveShuffle(shuffleId)
    Future.sequence(
      blockManagerInfo.values.map { bm =>
        bm.slaveEndpoint.ask[Boolean](removeMsg)
      }.toSeq
    )
  }

  /**
   * Delegate RemoveBroadcast messages to each BlockManager because the master may not notified
   * of all broadcast blocks. If removeFromDriver is false, broadcast blocks are only removed
   * from the executors, but not from the driver.
   */
  private def removeBroadcast(broadcastId: Long, removeFromDriver: Boolean): Future[Seq[Int]] = {
    val removeMsg = RemoveBroadcast(broadcastId, removeFromDriver)
    val requiredBlockManagers = blockManagerInfo.values.filter { info =>
      removeFromDriver || !info.blockManagerId.isDriver
    }
    Future.sequence(
      requiredBlockManagers.map { bm =>
        bm.slaveEndpoint.ask[Int](removeMsg)
      }.toSeq
    )
  }

  private def removeBlockManager(blockManagerId: BlockManagerId) {
    val info = blockManagerInfo(blockManagerId)

    // Remove the block manager from blockManagerIdByExecutor.
    blockManagerIdByExecutor -= blockManagerId.executorId

    // Remove it from blockManagerInfo and remove all the blocks.
    blockManagerInfo.remove(blockManagerId)

    val iterator = info.blocks.keySet.iterator
    while (iterator.hasNext) {
      val blockId = iterator.next
      val locations = blockLocations.get(blockId)
      locations -= blockManagerId
      // De-register the block if none of the block managers have it. Otherwise, if pro-active
      // replication is enabled, and a block is either an RDD or a test block (the latter is used
      // for unit testing), we send a message to a randomly chosen executor location to replicate
      // the given block. Note that we ignore other block types (such as broadcast/shuffle blocks
      // etc.) as replication doesn't make much sense in that context.
      if (locations.size == 0) {
        blockLocations.remove(blockId)
        logWarning(s"No more replicas available for $blockId !")
      } else if (proactivelyReplicate && (blockId.isRDD || blockId.isInstanceOf[TestBlockId])) {
        // As a heursitic, assume single executor failure to find out the number of replicas that
        // existed before failure
        val maxReplicas = locations.size + 1
        val i = (new Random(blockId.hashCode)).nextInt(locations.size)
        val blockLocations = locations.toSeq
        val candidateBMId = blockLocations(i)
        blockManagerInfo.get(candidateBMId).foreach { bm =>
          val remainingLocations = locations.toSeq.filter(bm => bm != candidateBMId)
          val replicateMsg = ReplicateBlock(blockId, remainingLocations, maxReplicas)
          bm.slaveEndpoint.ask[Boolean](replicateMsg)
        }
      }
    }

    listenerBus.post(SparkListenerBlockManagerRemoved(System.currentTimeMillis(), blockManagerId))
    logInfo(s"Removing block manager $blockManagerId")

  }

  private def removeExecutor(execId: String) {
    logInfo("Trying to remove executor " + execId + " from BlockManagerMaster.")
    blockManagerIdByExecutor.get(execId).foreach(removeBlockManager)
  }

  /**
   * Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   */
  private def heartbeatReceived(blockManagerId: BlockManagerId): Boolean = {
    if (!blockManagerInfo.contains(blockManagerId)) {
      blockManagerId.isDriver && !isLocal
    } else {
      blockManagerInfo(blockManagerId).updateLastSeenMs()
      true
    }
  }

  // Remove a block from the slaves that have it. This can only be used to remove
  // blocks that the master knows about.
  private def removeBlockFromWorkers(blockId: BlockId) {
    val locations = blockLocations.get(blockId)
    if (locations != null) {
      locations.foreach { blockManagerId: BlockManagerId =>
        val blockManager = blockManagerInfo.get(blockManagerId)
        if (blockManager.isDefined) {
          // Remove the block from the slave's BlockManager.
          // Doesn't actually wait for a confirmation and the message might get lost.
          // If message loss becomes frequent, we should add retry logic here.
          blockManager.get.slaveEndpoint.ask[Boolean](RemoveBlock(blockId))
        }
      }
    }
  }

  // Return a map from the block manager id to max memory and remaining memory.
  private def memoryStatus: Map[BlockManagerId, (Long, Long)] = {
    blockManagerInfo.map { case(blockManagerId, info) =>
      (blockManagerId, (info.maxMem, info.remainingMem))
    }.toMap
  }

  private def storageStatus: Array[StorageStatus] = {
    blockManagerInfo.map { case (blockManagerId, info) =>
      new StorageStatus(blockManagerId, info.maxMem, Some(info.maxOnHeapMem),
        Some(info.maxOffHeapMem), info.blocks.asScala)
    }.toArray
  }

  /**
   * Return the block's status for all block managers, if any. NOTE: This is a
   * potentially expensive operation and should only be used for testing.
   *
   * If askSlaves is true, the master queries each block manager for the most updated block
   * statuses. This is useful when the master is not informed of the given block by all block
   * managers.
   */
  private def blockStatus(
      blockId: BlockId,
      askSlaves: Boolean): Map[BlockManagerId, Future[Option[BlockStatus]]] = {
    val getBlockStatus = GetBlockStatus(blockId)
    /*
     * Rather than blocking on the block status query, master endpoint should simply return
     * Futures to avoid potential deadlocks. This can arise if there exists a block manager
     * that is also waiting for this master endpoint's response to a previous message.
     */
    blockManagerInfo.values.map { info =>
      val blockStatusFuture =
        if (askSlaves) {
          info.slaveEndpoint.ask[Option[BlockStatus]](getBlockStatus)
        } else {
          Future { info.getStatus(blockId) }
        }
      (info.blockManagerId, blockStatusFuture)
    }.toMap
  }

  /**
   * Return the ids of blocks present in all the block managers that match the given filter.
   * NOTE: This is a potentially expensive operation and should only be used for testing.
   *
   * If askSlaves is true, the master queries each block manager for the most updated block
   * statuses. This is useful when the master is not informed of the given block by all block
   * managers.
   */
  private def getMatchingBlockIds(
      filter: BlockId => Boolean,
      askSlaves: Boolean): Future[Seq[BlockId]] = {
    val getMatchingBlockIds = GetMatchingBlockIds(filter)
    Future.sequence(
      blockManagerInfo.values.map { info =>
        val future =
          if (askSlaves) {
            info.slaveEndpoint.ask[Seq[BlockId]](getMatchingBlockIds)
          } else {
            Future { info.blocks.asScala.keys.filter(filter).toSeq }
          }
        future
      }
    ).map(_.flatten.toSeq)
  }

  /**
   * Returns the BlockManagerId with topology information populated, if available.
   */
  private def register(
      idWithoutTopologyInfo: BlockManagerId,
      maxOnHeapMemSize: Long,
      maxOffHeapMemSize: Long,
      slaveEndpoint: RpcEndpointRef): BlockManagerId = {
    // the dummy id is not expected to contain the topology information.
    // we get that info here and respond back with a more fleshed out block manager id
    val id = BlockManagerId(
      idWithoutTopologyInfo.executorId,
      idWithoutTopologyInfo.host,
      idWithoutTopologyInfo.port,
      topologyMapper.getTopologyForHost(idWithoutTopologyInfo.host))

    val time = System.currentTimeMillis()
    if (!blockManagerInfo.contains(id)) {
      blockManagerIdByExecutor.get(id.executorId) match {
        case Some(oldId) =>
          // A block manager of the same executor already exists, so remove it (assumed dead)
          logError("Got two different block manager registrations on same executor - "
              + s" will replace old one $oldId with new one $id")
          removeExecutor(id.executorId)
        case None =>
      }
      logInfo("Registering block manager %s with %s RAM, %s".format(
        id.hostPort, Utils.bytesToString(maxOnHeapMemSize + maxOffHeapMemSize), id))

      blockManagerIdByExecutor(id.executorId) = id

      blockManagerInfo(id) = new BlockManagerInfo(
        id, System.currentTimeMillis(), maxOnHeapMemSize, maxOffHeapMemSize, slaveEndpoint)
    }
    listenerBus.post(SparkListenerBlockManagerAdded(time, id, maxOnHeapMemSize + maxOffHeapMemSize,
        Some(maxOnHeapMemSize), Some(maxOffHeapMemSize)))
    id
  }

  private def updateBlockInfo(
      blockManagerId: BlockManagerId,
      blockId: BlockId,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long,
      hash: String,
      hash_took: Long): Boolean = {

    if (!blockManagerInfo.contains(blockManagerId)) {
      if (blockManagerId.isDriver && !isLocal) {
        // We intentionally do not register the master (except in local mode),
        // so we should not indicate failure.
        return true
      } else {
        return false
      }
    }

    if (blockId == null) {
      blockManagerInfo(blockManagerId).updateLastSeenMs()
      return true
    }

    blockManagerInfo(blockManagerId).updateBlockInfo(blockId, storageLevel, memSize, diskSize, hash, hash_took)

    var locations: mutable.HashSet[BlockManagerId] = null
    if (blockLocations.containsKey(blockId)) {
      locations = blockLocations.get(blockId)
    } else {
      locations = new mutable.HashSet[BlockManagerId]
      blockLocations.put(blockId, locations)
    }

    if (storageLevel.isValid) {
      locations.add(blockManagerId)
    } else {
      locations.remove(blockManagerId)
    }

    // Remove the block from master tracking if it has been removed on all slaves.
    if (locations.size == 0) {
      blockLocations.remove(blockId)
    }
    true
  }

  private def canGetRemote(blockId: BlockId): Boolean = {
    /* Only allow rdd unique blockids to be read remotely 
     * any other blocks will have issues */
    blockId match {
      case RDDUniqueBlockId(_) => true
      //case RDDBlockId(_, _) => true // uncomment this to share non-uniaue RDD blocks - THIS IS VERY UNSAFE
      case _ => false
    }
  }

  private def getRemoteLocations(blockId: BlockId)
  : Option[Seq[BlockManagerId]]= {
    if (!canGetRemote(blockId)){
      return None
    }

    val r:Seq[Seq[BlockManagerId]] = remoteBlockManagerMasters.toSeq.map (
    { case (name, rbmm) => {
        val f = rbmm.ask[Seq[BlockManagerId]]( GetLocations(blockId, false) )
        //catch a failure and remove the rbmm from the list to stop further failures
        f onFailure { case f =>
          remoteBlockManagerMasters.synchronized {
            logInfo(s"getRemoteLocations failed, removing $rbmm")
            remoteBlockManagerMasters.remove(name)
          }
        }
        //return an empty sequence on failure
        f.fallbackTo( Future { Seq.empty } )
        RpcUtils.askRpcTimeout(conf).awaitResult(f)
      } } )

    if (r.length > 0){
      val ret:Seq[BlockManagerId] = r.reduce( (a,b) => {
        a ++ b
      } )

      Some(ret)
    } else {
      None
    }
  }

  private def getRemoteLocationsMultipleBlockIds(blockIds: Array[BlockId])
  : Option[IndexedSeq[Seq[BlockManagerId]]]= {
    //filter out anything that we can't ask for remotely
    //TODO since we don't ask for broadcast rdds using this method, we skip this for now
    // we need to make sure we return the locations int he right order
    val r:Seq[IndexedSeq[Seq[BlockManagerId]]] = remoteBlockManagerMasters.toSeq.map (
    { case (name, rbmm) => {
      val f = rbmm.ask[IndexedSeq[Seq[BlockManagerId]]]( GetLocationsMultipleBlockIds(blockIds, false) )
        //catch a failure and remove the rbmm from the list to stop further failures
      f onFailure { case f =>
        remoteBlockManagerMasters.synchronized {
          logInfo(s"getRemoteLocationsMultipleBlockIds failed, removing $rbmm")
          remoteBlockManagerMasters.remove(name)
        }
      }
      //return an empty sequence on failure
      f.fallbackTo( Future { blockIds.map( _ => Seq.empty ) } )
      RpcUtils.askRpcTimeout(conf).awaitResult(f)
    } } ) 

    if (r.length > 0){
      val ret:IndexedSeq[Seq[BlockManagerId]] = r.reduce( (a,b) => { //a,b are indexedseqs holding the ordered list of each rBMM's response
        a.zip(b).map(x => x._1 ++ x._2) //combine each result for the block id
      })
      Some(ret)
    } else {
      None
    }
  }

  private def getLocations(blockId: BlockId, remote: Boolean): Seq[BlockManagerId] = {
    val remoteLocs = if(remote) {
      getRemoteLocations(blockId) match {
          case Some(remoteLocs) => remoteLocs
          case None => Seq.empty
        }
      } else Seq.empty
    
     remoteLocs ++ (if (blockLocations.containsKey(blockId)) blockLocations.get(blockId).toSeq else Seq.empty)
  }

  private def getLocationsMultipleBlockIds(
      blockIds: Array[BlockId], remote: Boolean)
  : IndexedSeq[Seq[BlockManagerId]] = {
    val a = blockIds.map(blockId => getLocations(blockId, false))
    if (remote){
      val b = getRemoteLocationsMultipleBlockIds(blockIds)
      b match {
        case Some(r) => {
          val ret = a.zip(r).map(x => x._1 ++ x._2)
          ret
        }
        case None => a
      }
    } else {
      a
    }
  }

  // Track block IDs to RDDs
  private val blockIdToRDD
    = new HashMap[BlockId, scala.collection.mutable.Set[Int]] with MultiMap[BlockId, Int]

  private def getRDDsUsingBlockId(blockId: BlockId): Set[Int] = {
    blockIdToRDD.get(blockId).getOrElse( mutable.Set() ).toSet
  }
  private def registerRDDUsingBlockId(blockId: BlockId, rddId: Int): Boolean = {
    blockIdToRDD.addBinding( blockId, rddId )
    return true
  }
  private def deregisterRDDUsingBlockId(blockId: BlockId, rddId: Int): Boolean = {
    blockIdToRDD.removeBinding(blockId, rddId)
    return true
  }
  private def getAllRDDsUsingBlockIds = blockIdToRDD.toSeq

  /** Get the list of the peers of the given block manager */
  private def getPeers(blockManagerId: BlockManagerId): Seq[BlockManagerId] = {
    val blockManagerIds = blockManagerInfo.keySet
    if (blockManagerIds.contains(blockManagerId)) {
      blockManagerIds.filterNot { _.isDriver }.filterNot { _ == blockManagerId }.toSeq
    } else {
      Seq.empty
    }
  }

  /**
   * Returns an [[RpcEndpointRef]] of the [[BlockManagerSlaveEndpoint]] for sending RPC messages.
   */
  private def getExecutorEndpointRef(executorId: String): Option[RpcEndpointRef] = {
    for (
      blockManagerId <- blockManagerIdByExecutor.get(executorId);
      info <- blockManagerInfo.get(blockManagerId)
    ) yield {
      info.slaveEndpoint
    }
  }

  override def onStop(): Unit = {
    askThreadPool.shutdownNow()
  }
}

@DeveloperApi
case class BlockStatus(storageLevel: StorageLevel, memSize: Long, diskSize: Long, hash: String = "", hash_took: Long = 0L) {
  def isCached: Boolean = memSize + diskSize > 0
}

@DeveloperApi
object BlockStatus {
  def empty: BlockStatus = BlockStatus(StorageLevel.NONE, memSize = 0L, diskSize = 0L, hash = "", hash_took = 0)
}

private[spark] class BlockManagerInfo(
    val blockManagerId: BlockManagerId,
    timeMs: Long,
    val maxOnHeapMem: Long,
    val maxOffHeapMem: Long,
    val slaveEndpoint: RpcEndpointRef)
  extends Logging {

  val maxMem = maxOnHeapMem + maxOffHeapMem

  private var _lastSeenMs: Long = timeMs
  private var _remainingMem: Long = maxMem

  // Mapping from block id to its status.
  private val _blocks = new JHashMap[BlockId, BlockStatus]

  // Cached blocks held by this BlockManager. This does not include broadcast blocks.
  private val _cachedBlocks = new mutable.HashSet[BlockId]

  def getStatus(blockId: BlockId): Option[BlockStatus] = Option(_blocks.get(blockId))

  def updateLastSeenMs() {
    _lastSeenMs = System.currentTimeMillis()
  }

  def updateBlockInfo(
      blockId: BlockId,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long,
      hash: String,
      hash_took: Long) {

    updateLastSeenMs()

    val blockExists = _blocks.containsKey(blockId)
    var originalMemSize: Long = 0
    var originalDiskSize: Long = 0
    var originalLevel: StorageLevel = StorageLevel.NONE

    if (blockExists) {
      // The block exists on the slave already.
      val blockStatus: BlockStatus = _blocks.get(blockId)
      originalLevel = blockStatus.storageLevel
      originalMemSize = blockStatus.memSize
      originalDiskSize = blockStatus.diskSize

      if (originalLevel.useMemory) {
        _remainingMem += originalMemSize
      }
    }

    if (storageLevel.isValid) {
      /* isValid means it is either stored in-memory or on-disk.
       * The memSize here indicates the data size in or dropped from memory,
       * externalBlockStoreSize here indicates the data size in or dropped from externalBlockStore,
       * and the diskSize here indicates the data size in or dropped to disk.
       * They can be both larger than 0, when a block is dropped from memory to disk.
       * Therefore, a safe way to set BlockStatus is to set its info in accurate modes. */
      var blockStatus: BlockStatus = null
      if (storageLevel.useMemory) {
        blockStatus = BlockStatus(storageLevel, memSize = memSize, diskSize = 0, hash = hash, hash_took = hash_took)
        _blocks.put(blockId, blockStatus)
        _remainingMem -= memSize
        if (blockExists) {
          logInfo(s"Updated $blockId in memory on ${blockManagerId.hostPort}" +
            s" (current size: ${Utils.bytesToString(memSize)}," +
            s" original size: ${Utils.bytesToString(originalMemSize)}," +
            s" free: ${Utils.bytesToString(_remainingMem)}," +
            s" hash: ${hash} hash_took: ${hash_took})")
        } else {
          logInfo(s"Added $blockId in memory on ${blockManagerId.hostPort}" +
            s" (size: ${Utils.bytesToString(memSize)}," +
            s" free: ${Utils.bytesToString(_remainingMem)})" +
            s" hash: ${hash} hash_took: ${hash_took})")
        }
      }
      if (storageLevel.useDisk) {
        blockStatus = BlockStatus(storageLevel, memSize = 0, diskSize = diskSize, hash = hash, hash_took = hash_took)
        _blocks.put(blockId, blockStatus)
        if (blockExists) {
          logInfo(s"Updated $blockId on disk on ${blockManagerId.hostPort}" +
            s" (current size: ${Utils.bytesToString(diskSize)}," +
            s" original size: ${Utils.bytesToString(originalDiskSize)}," + 
            s" hash: ${hash})")
        } else {
          logInfo(s"Added $blockId on disk on ${blockManagerId.hostPort}" +
            s" (size: ${Utils.bytesToString(diskSize)}," +
            s" hash: ${hash})")
        }
      }
      if (!blockId.isBroadcast && blockStatus.isCached) {
        _cachedBlocks += blockId
      }
    } else if (blockExists) {
      // If isValid is not true, drop the block.
      _blocks.remove(blockId)
      _cachedBlocks -= blockId
      if (originalLevel.useMemory) {
        logInfo(s"Removed $blockId on ${blockManagerId.hostPort} in memory" +
          s" (size: ${Utils.bytesToString(originalMemSize)}," +
          s" free: ${Utils.bytesToString(_remainingMem)})")
      }
      if (originalLevel.useDisk) {
        logInfo(s"Removed $blockId on ${blockManagerId.hostPort} on disk" +
          s" (size: ${Utils.bytesToString(originalDiskSize)})")
      }
    }
  }

  def removeBlock(blockId: BlockId) {
    if (_blocks.containsKey(blockId)) {
      _remainingMem += _blocks.get(blockId).memSize
      _blocks.remove(blockId)
    }
    _cachedBlocks -= blockId
  }

  def remainingMem: Long = _remainingMem

  def lastSeenMs: Long = _lastSeenMs

  def blocks: JHashMap[BlockId, BlockStatus] = _blocks

  // This does not include broadcast blocks.
  def cachedBlocks: collection.Set[BlockId] = _cachedBlocks

  override def toString: String = "BlockManagerInfo " + timeMs + " " + _remainingMem

  def clear() {
    _blocks.clear()
  }
}
