package edu.berkeley.cs.amplab.sparkmem

import org.apache.spark.{TaskEndReason, Success => TaskEndSuccess}
import org.apache.spark.executor.{TaskMetrics, BlockAccess, BlockAccessType, DataReadMethod}
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.{SparkListenerApplicationStart,
                                   SparkListenerApplicationEnd,
                                   SparkListenerTaskEnd,
                                   SparkListenerBroadcastCreated}
import org.apache.spark.storage.{BlockId, BlockStatus, RDDBlockId, ShuffleBlockId, BroadcastBlockId}
 
import java.io.PrintWriter

import scala.collection.mutable

object BlockAccessListener {
  // Broadcasts smaller than this size are dropped entirely.
  val MINIMUM_BROADCAST_SIZE: Long = 128L * 1024L
}

class BlockAccessListener extends SparkListener with Logging {
  var skipProcessTasks = false
  var sortTasks = true
  var recordLogFile: PrintWriter = null
  var skipExtraStacks = false
  var skipRDDStack = false
  var consolidateRDDs = false

  private val pendingTasks = mutable.Buffer.empty[SparkListenerTaskEnd]
  private var didStart = false
  private val rddStack = new PriorityStack
  private val broadcastStack = new PriorityStack
  private val shuffleStack = new PriorityStack

  // TODO: Seperate in v. out memory sizes?
  // TODO: Make sure (Spark-side) that we don't double-count in Aggregator?
  private val topRddBlockSizes = new TopK
  private val topShuffleMemorySizes = new TopK
  private val topShuffleMemorySizesPairAdjust = new TopK
  private val topShuffleDiskBlockSizes = new TopK

  // 8 byte header + two 4-byte pointers
  private val PAIR_SIZE_ADJUSTMENT = { 8 + 4 + 4 }

  private var totalSpilledMemory = 0L
  private var totalSpilledDisk = 0L

  private var totalComputedDropped = 0L

  private var totalRecomputed = 0L
  private var totalRecomputedUnknown = 0L
  private var totalRecomputedZero = 0L

  // FIXME: Track block size sources
  private val blockSizes: mutable.Map[BlockId, Long] = mutable.Map.empty[BlockId, Long]

  var sizeIncrements = 0L
  def rddBlockCount: Int = blockSizes.keys.filter(_.isRDD).size

  def wasUnavailable(access: BlockAccess): Boolean =
    access.inputMetrics.map(_.readMethod == DataReadMethod.Unavailable).getOrElse(false)

  // To do this:
  //    Maintain taskId -> stageId map (for unended tasks)
  import BlockAccessListener._
  private val seenShuffleWritePieces = mutable.Set.empty[BlockId]
  private val shuffleWriteSize = mutable.Map.empty[Int, Long]
  private val seenShuffleReadPieces = mutable.Set.empty[BlockId]
  private val writtenBlocks = mutable.Set.empty[BlockId]
  private def nameShuffle(shuffleId: Int): String = "shuffle_" + shuffleId + "_0_0"
  private def shuffleReadIds(
      accessedBlocks: Option[Seq[(BlockId, BlockAccess)]]): Seq[ShuffleBlockId] = {
    accessedBlocks.getOrElse(Nil).filter { case (blockId, access) =>
      blockId.isShuffle && access.accessType == BlockAccessType.Read
    }.map(_._1.asInstanceOf[ShuffleBlockId])
  }
  private def shuffleWriteIds(
      accessedBlocks: Option[Seq[(BlockId, BlockAccess)]]): Seq[ShuffleBlockId] = {
    accessedBlocks.getOrElse(Nil).filter { case (blockId, access) =>
      blockId.isShuffle && access.accessType == BlockAccessType.Read
    }.map(_._1.asInstanceOf[ShuffleBlockId])
  }
  private def accumulateShuffle(taskId: Long, metrics: TaskMetrics) {
    val writeSize = metrics.shuffleWriteMetrics.map(_.shuffleBytesWritten).getOrElse(0L)
    // FIXME: This includes BOTH ends of the shuffle
    val memorySize = metrics.shuffleMemoryMetrics.map(_.shuffleOutputBytes).getOrElse(0L)
    val memoryGroups = metrics.shuffleMemoryMetrics.map(_.shuffleOutputGroups).getOrElse(0L)
    // TODO: When this isn't derived from hashtable size (shuffle.spill) we need to adjust it
    //       for hashtable overhead, too
    val memorySizeAdjusted = memorySize - memoryGroups * PAIR_SIZE_ADJUSTMENT
    var hadRead = false
    logDebug(s"Shuffle for TID $taskId: wrote $writeSize; produced $memorySize in memory")
    totalSpilledMemory += metrics.memoryBytesSpilled
    totalSpilledDisk += metrics.diskBytesSpilled
    shuffleReadIds(metrics.accessedBlocks).foreach { blockId =>
      if (!skipExtraStacks) {
        shuffleStack.read(nameShuffle(blockId.shuffleId), Some(shuffleWriteSize.getOrElse(blockId.shuffleId, 0L)),
                          shuffleWriteSize.getOrElse(blockId.shuffleId, 0L).toDouble)
      }
      if (!seenShuffleReadPieces.contains(blockId)) {
        seenShuffleReadPieces += blockId
      }
      topShuffleMemorySizes.insert(memorySize)
      topShuffleMemorySizesPairAdjust.insert(memorySizeAdjusted)
      hadRead = true
    }
    shuffleWriteIds(metrics.accessedBlocks).foreach { blockId => 
      val shuffleId = blockId.shuffleId
      if (!seenShuffleWritePieces.contains(blockId)) {
        seenShuffleWritePieces += blockId
        shuffleWriteSize(shuffleId) = shuffleWriteSize.getOrElse(shuffleId, 0L) + writeSize
        topShuffleDiskBlockSizes.insert(writeSize)
        if (!hadRead) {
          topShuffleMemorySizes.insert(memorySize)
          topShuffleMemorySizesPairAdjust.insert(memorySizeAdjusted)
        }
      }
      if (!skipExtraStacks) {
        shuffleStack.write(nameShuffle(shuffleId), shuffleWriteSize(shuffleId))
      }
    }
  }
  // As a simplification we assume that no task writes to multiple shuffles.

  def usageInfo: UsageInfo = UsageInfo(
    rddStack.getCostCurve,
    broadcastStack.getCostCurve,
    shuffleStack.getCostCurve,
    topRddBlockSizes.get,
    topShuffleMemorySizes.get,
    topShuffleMemorySizesPairAdjust.get,
    topShuffleDiskBlockSizes.get,
    totalSpilledMemory,
    totalSpilledDisk,
    rddBlockCount,
    sizeIncrements,
    totalRecomputed,
    totalRecomputedUnknown,
    totalRecomputedZero,
    totalComputedDropped
  )

  private def recordTopSizes() {
    for ((blockId, size) <- blockSizes) {
      blockId match {
        case RDDBlockId(_, -1) => {}
        case RDDBlockId(_, _) => topRddBlockSizes.insert(size)
        case ShuffleBlockId(_, _, _) => topShuffleDiskBlockSizes.insert(size)
        case _ => {}
      }
    }
  }

  private def recordSize(blockId: BlockId, observedSize: Long) {
    /* TODO: Coarsening for shuffle blocks here. */
    if (blockId.isRDD) {
      blockSizes.get(blockId).foreach { case oldSize => 
        sizeIncrements += math.max(0L, observedSize - oldSize)
      }
    }
    blockSizes(blockId) = math.max(blockSizes.getOrElse(blockId, 0L), observedSize)
  }

  private def costForBlock(blockId: BlockId): Double = {
    return blockSizes.getOrElse(blockId, 0L).toDouble
  }

  private def sizeForBlock(blockId: BlockId): Long = {
    val size = blockSizes.getOrElse(blockId, -1L)
    if (size < 0L) {
      if (consolidateRDDs && blockId.isRDD) {
        val blockIdRdd = blockId.asInstanceOf[RDDBlockId]
        assert(blockIdRdd.splitIndex == -1)
        var index = 0
        var newSize = 0L
        while (blockSizes.contains(RDDBlockId(blockIdRdd.rddId, index))) {
          newSize += blockSizes(RDDBlockId(blockIdRdd.rddId, index))
          index += 1
        }
        blockSizes(blockId) = newSize
        return newSize
      } else {
        logError("sizeForBlock unavailable for " + blockId)
      }
    }
    size
  }

  private def recordedBlockIdForBlock(blockId: BlockId): BlockId = blockId match {
    case ShuffleBlockId(shuffleId, _, _) => ShuffleBlockId(shuffleId, 0, 0)
    case RDDBlockId(rddId, splitIndex) =>
      if (consolidateRDDs)
        RDDBlockId(rddId, -1)
      else
        blockId
    case _ => blockId
  }

  private def recordStatusSize(blockId: BlockId, blockStatus: BlockStatus) {
    if (blockStatus.storageLevel.useMemory) {
      recordSize(blockId, blockStatus.memSize)
    } else if (blockStatus.storageLevel.useOffHeap) {
      recordSize(blockId, blockStatus.externalBlockStoreSize)
    } else if (blockStatus.storageLevel.useDisk) {
      recordSize(blockId, blockStatus.diskSize)
    } else if (blockStatus.memSize > 0) {
      totalComputedDropped += blockStatus.memSize
      recordSize(blockId, blockStatus.memSize)
    }
  }

  private def recordAccessSize(blockId: BlockId, blockAccess: BlockAccess) {
    if (!blockSizes.contains(blockId)) {
      blockAccess.inputMetrics match {
        case Some(metrics) => blockSizes(blockId) = metrics.bytesRead
        case None => {} /* FIXME: Make pending? */
      }
    }
  }

  private def nameBlock(blockId: BlockId): String = blockId match {
    case ShuffleBlockId(shuffleId, _, _) => nameShuffle(shuffleId)
    case _ => blockId.name
  }

  private def recordAccess(rawBlockId: BlockId, blockAccess: BlockAccess, taskId: Option[Long] = None) {
    val blockId = recordedBlockIdForBlock(rawBlockId)
    if (blockId.isBroadcast) {
      if (sizeForBlock(blockId) < BlockAccessListener.MINIMUM_BROADCAST_SIZE) {
        return
      }
    }
    val whichStack = blockId match {
      case BroadcastBlockId(_, _) => if (skipExtraStacks) None else Some(broadcastStack)
      // Shuffles are handled in accumulateShuffle().
      case ShuffleBlockId(_, _, _) => None
      case RDDBlockId(_, _) => {
        Option(recordLogFile).foreach { stream => 
          val accessType = if (blockAccess.accessType == BlockAccessType.Read) "READ" else "WRITE"
          val accessSize = sizeForBlock(blockId)
          val recordTaskId = taskId.getOrElse(-1L)
          recordLogFile.println(s"$accessType ${blockId.name} $accessSize $recordTaskId")
        }
        if (skipRDDStack) {
          None
        } else {
          Some(rddStack)
        }
      }
      case _ => {
        logError("Unknown block ID type for " + rawBlockId)
        None
      }
    }
    whichStack match {
      case Some(stack) =>
        if (blockAccess.accessType == BlockAccessType.Read) {
          assert(sizeForBlock(blockId) >= 0)
          assert(costForBlock(blockId) >= 0.0)
          stack.read(nameBlock(blockId), Some(sizeForBlock(blockId)), costForBlock(blockId))
        } else {
          assert(sizeForBlock(blockId) >= 0)
          assert(costForBlock(blockId) >= 0.0)
          stack.write(nameBlock(blockId), sizeForBlock(blockId))
        }
      case None => {}
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    if (skipProcessTasks) {
      recordSizes(taskEnd)
    } else {
      if (sortTasks) {
        pendingTasks += taskEnd
      } else {
        processTask(taskEnd)
      }
    }
  }

  private def recordSizes(taskEnd: SparkListenerTaskEnd) {
    val metrics = taskEnd.taskMetrics

    /* Infer block sizes from updated blocks. */
    for ((blockId, blockStatus) <- metrics.updatedBlocks.getOrElse(Nil)) {
      recordStatusSize(blockId, blockStatus)
    }

    /* Infer block sizes from read blocks. */
    for ((blockId, blockAccess) <- metrics.accessedBlocks.getOrElse(Nil)) {
      recordAccessSize(blockId, blockAccess)
    }
  }

  private def processTask(taskEnd: SparkListenerTaskEnd) {
    assert(didStart)
    recordSizes(taskEnd)
    if (taskEnd.reason == TaskEndSuccess) {
      val metrics = taskEnd.taskMetrics


      if (metrics.updatedBlocks.getOrElse(Nil).size > 0) {
        if (metrics.accessedBlocks.getOrElse(Nil).size == 0) {
          logError("updated blocks " + metrics.updatedBlocks + " but accessed " + metrics.accessedBlocks)
        }
      }

      accumulateShuffle(taskEnd.taskInfo.taskId, metrics)

      /* Infer block sizes from updated blocks. */
      for ((blockId, blockStatus) <- metrics.updatedBlocks.getOrElse(Nil)) {
        recordStatusSize(blockId, blockStatus)
      }

      /* Infer block sizes from read blocks. */
      for ((blockId, blockAccess) <- metrics.accessedBlocks.getOrElse(Nil)) {
        recordAccessSize(blockId, blockAccess)
      }

      if (metrics.updatedBlocks.getOrElse(Nil).size > 0) {
        if (metrics.accessedBlocks.getOrElse(Nil).size == 0) {
          logError("updated blocks " + metrics.updatedBlocks + " but accessed " + metrics.accessedBlocks)
        }
      }

      /* Actual record accesses in priority stack. */
      var misses = List.empty[BlockId]  // List of "active" misses
      if (metrics.accessedBlocks.size > 0) {
        logDebug("Start of access set")
      }
      val thisTaskBlocks = mutable.Set.empty[BlockId]
      for ((blockId, blockAccess) <- metrics.accessedBlocks.getOrElse(Nil)) {
        var rewrite = false
        var firstMiss = false
        if (blockAccess.accessType == BlockAccessType.Read) {
          if (!writtenBlocks.contains(blockId)) {
            firstMiss = true
          } else if (wasUnavailable(blockAccess)) {
            misses = blockId :: misses
          } else if (misses.headOption == Some(blockId)) {
            misses = misses.tail
          }
        } else if (blockAccess.accessType == BlockAccessType.Write) {
          if (thisTaskBlocks.contains(blockId)) {
            logError("Duplciate block")
          }
          thisTaskBlocks += blockId
          if (!writtenBlocks.contains(blockId)) {
            writtenBlocks += blockId
          } else {
            rewrite = true
            val theSize = sizeForBlock(blockId)
            if (theSize >= 0) {
              if (theSize == 0) {
                totalRecomputedZero += 1
              }
              logDebug(s"Found recomputed size of $theSize for $blockId")
              totalRecomputed += theSize
            } else {
              totalRecomputedUnknown += 1
            }
          }
          if (misses.headOption == Some(blockId)) {
            misses = misses.tail
          }
        }
        
        // Ignore block accesses taken to recover from a read miss.
        if (misses.isEmpty && !rewrite && !firstMiss) {
          logDebug("Within access set got " + blockAccess)
          recordAccess(blockId, blockAccess, Some(taskEnd.taskInfo.taskId))
        } else {
          logDebug("Ignoring access " + blockAccess)
        }
      }
      if (metrics.accessedBlocks.size > 0) {
        logDebug("End of access set")
      }
    }
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
    if (sortTasks && !skipProcessTasks) {
      pendingTasks.sortBy(taskEnd => taskEnd.taskInfo.taskId).foreach(processTask)
    }
    recordTopSizes()
    didStart = false
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart) {
    assert(!didStart) /* Don't support multiple applications yet. */
    didStart = true
  }

  override def onBroadcastCreated(broadcastCreated: SparkListenerBroadcastCreated) {
    assert(didStart)
    val blockId = BroadcastBlockId(broadcastCreated.broadcastId)
    // FIXME: use serialized size separately?
    assert(broadcastCreated.memorySize.getOrElse(0L) + broadcastCreated.serializedSize.getOrElse(0L) > 0)
    broadcastCreated.memorySize.foreach(recordSize(blockId, _))
    broadcastCreated.serializedSize.foreach(recordSize(blockId, _))
    if (sizeForBlock(blockId) >= BlockAccessListener.MINIMUM_BROADCAST_SIZE) {
      val dummyAccess = BlockAccess(BlockAccessType.Write)
      recordAccess(blockId, dummyAccess)
    }
  }

  /* FIXME: Use onUnpersistRDD events? */
}
