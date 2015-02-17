package edu.berkeley.cs.amplab.sparkmem

import org.apache.spark.{TaskEndReason, Success => TaskEndSuccess}
import org.apache.spark.executor.{TaskMetrics, BlockAccess, BlockAccessType}
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.{SparkListenerApplicationStart,
                                   SparkListenerApplicationEnd,
                                   SparkListenerTaskEnd,
                                   SparkListenerBroadcastCreated}
import org.apache.spark.storage.{BlockId, BlockStatus, RDDBlockId, ShuffleBlockId, BroadcastBlockId}

import java.io.PrintWriter

import scala.collection.mutable

object BlockAccessListener {
  case class ShuffleWritePiece(shuffleId: Int, piece: Int)
  // A reduce task might combine multiple shuffles, potentially with different partitioning.
  // We treat each case seperately for the purpose of figuring out the size
  case class ShuffleReadPiece(shuffleIds: List[Int], pieceStarts: List[Int], pieceEnds: List[Int])

  // Broadcasts smaller than this size are dropped entirely.
  val MINIMUM_BROADCAST_SIZE: Long = 128L * 1024L
}

class BlockAccessListener extends SparkListener with Logging {
  val sortTasks = true
  var recordLogFile: PrintWriter = null
  private val pendingTasks = mutable.Buffer.empty[SparkListenerTaskEnd]
  private var didStart = false
  private val rddStack = new PriorityStack
  private val broadcastStack = new PriorityStack
  private val shuffleStack = new PriorityStack

  private val topRddBlockSizes = new TopK
  private val topShuffleMemorySizes = new TopK
  private val topShuffleDiskBlockSizes = new TopK

  // FIXME: Track block size sources
  private val blockSizes: mutable.Map[BlockId, Long] = mutable.Map.empty[BlockId, Long]

  var sizeIncrements = 0L
  def rddBlockCount: Int = blockSizes.keys.filter(_.isRDD).size

  // To do this:
  //    Maintain taskId -> stageId map (for unended tasks)
  import BlockAccessListener._
  private val seenShuffleWritePieces = mutable.Set.empty[ShuffleWritePiece]
  private val shuffleWriteSize = mutable.Map.empty[Int, Long]
  private val seenShuffleReadPieces = mutable.Set.empty[ShuffleReadPiece]
  private val writtenBlocks = mutable.Set.empty[BlockId]
  private def nameShuffle(shuffleId: Int): String = "shuffle#" + shuffleId
  private def accumulateShuffle(metrics: TaskMetrics) {
    val writeSize = metrics.shuffleWriteMetrics.map(_.shuffleBytesWritten).getOrElse(0L)
    // FIXME: This includes BOTH ends of the shuffle
    val memorySize = metrics.shuffleMemoryMetrics.map(_.shuffleOutputBytes).getOrElse(0L)
    val haveRead = metrics.readShuffles.isDefined
    val haveWrite = metrics.writtenShuffles.isDefined
    metrics.readShuffles.map { shuffleParts =>
      assert(metrics.shuffleMemoryMetrics.isDefined)
      val shuffleIds = shuffleParts.map(_._1).toList
      val startPieces = shuffleParts.map(_._2).toList
      val endPieces = shuffleParts.map(_._3).toList
      shuffleIds.foreach { shuffleId => 
        shuffleStack.read(nameShuffle(shuffleId), Some(shuffleWriteSize(shuffleId)),
                          shuffleWriteSize(shuffleId).toDouble)
      }
      val part = ShuffleReadPiece(shuffleIds, startPieces, endPieces)
      if (!seenShuffleReadPieces.contains(part)) {
        seenShuffleReadPieces += part
      }
      topShuffleMemorySizes.insert(memorySize)
    }
    metrics.writtenShuffles.map { shuffleParts => 
      assert(shuffleParts.size == 1)
      shuffleParts.map { case (shuffleId, mapId) => 
        val writePiece = ShuffleWritePiece(shuffleId, mapId)
        if (!seenShuffleWritePieces.contains(writePiece)) {
          seenShuffleWritePieces += writePiece
          shuffleWriteSize(shuffleId) = shuffleWriteSize.getOrElse(shuffleId, 0L) + writeSize
          topShuffleDiskBlockSizes.insert(writeSize)
          if (!haveRead) {
            topShuffleMemorySizes.insert(memorySize)
          }
        }
        shuffleStack.write(nameShuffle(shuffleId), shuffleWriteSize(shuffleId))
      }
    }
    if (!haveRead && !haveWrite && memorySize > 0L) {
      logDebug("Task w/o read+write but with memorySize")
    }
    if (memorySize == 0L && haveRead) {
      logDebug("Task w/ read but no memorySize")
    }
    if (memorySize == 0L && haveWrite) {
      logDebug("Task w/ write but no memorySize")
    }
  }
  // As a simplification we assume that no task writes to multiple shuffles.

  def usageInfo: UsageInfo = UsageInfo(
    rddStack.getCostCurve,
    broadcastStack.getCostCurve,
    shuffleStack.getCostCurve,
    topRddBlockSizes.get,
    topShuffleMemorySizes.get,
    topShuffleDiskBlockSizes.get
  )

  private def recordTopSizes() {
    for ((blockId, size) <- blockSizes) {
      blockId match {
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
      logError("sizeForBlock unavailable for " + blockId)
    }
    size
  }

  private def recordedBlockIdForBlock(blockId: BlockId): BlockId = blockId

  private def recordStatusSize(blockId: BlockId, blockStatus: BlockStatus) {
    if (blockStatus.storageLevel.useMemory) {
      recordSize(blockId, blockStatus.memSize)
    } else if (blockStatus.storageLevel.useOffHeap) {
      recordSize(blockId, blockStatus.tachyonSize)
    } else if (blockStatus.storageLevel.useDisk) {
      recordSize(blockId, blockStatus.diskSize)
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

  private def nameBlock(blockId: BlockId): String = blockId.name

  private def recordAccess(rawBlockId: BlockId, blockAccess: BlockAccess, taskId: Option[Long] = None) {
    val blockId = recordedBlockIdForBlock(rawBlockId)
    val whichStack = blockId match {
      /* FIXME: Actually generate these in logs. */
      case BroadcastBlockId(_, _) => Some(broadcastStack)
      /* FIXME: Actually generate these in logs. */
      case ShuffleBlockId(_, _, _) => Some(shuffleStack)
      case RDDBlockId(_, _) => {
        Option(recordLogFile).foreach { stream => 
          val accessType = if (blockAccess.accessType == BlockAccessType.Read) "READ" else "WRITE"
          val accessSize = sizeForBlock(blockId)
          val recordTaskId = taskId.getOrElse(-1L)
          recordLogFile.println(s"$accessType ${blockId.name} $accessSize $recordTaskId")
        }
        Some(rddStack)
      }
      case _ => {
        logError("Unknown block ID type for " + rawBlockId)
        None
      }
    }
    whichStack match {
      case Some(stack) =>
        if (blockAccess.accessType == BlockAccessType.Read) {
          assert(sizeForBlock(blockId) > 0)
          assert(costForBlock(blockId) > 0.0)
          stack.read(nameBlock(blockId), Some(sizeForBlock(blockId)), costForBlock(blockId))
        } else {
          assert(sizeForBlock(blockId) > 0)
          assert(costForBlock(blockId) > 0.0)
          stack.write(nameBlock(blockId), sizeForBlock(blockId))
        }
      case None => {}
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    if (sortTasks) {
      pendingTasks += taskEnd
    } else {
      processTask(taskEnd)
    }
  }

  private def processTask(taskEnd: SparkListenerTaskEnd) {
    assert(didStart)
    if (taskEnd.reason == TaskEndSuccess) {
      val metrics = taskEnd.taskMetrics
      // TODO: Process shuffle{Read,Write,Memory}Metrics.

      accumulateShuffle(metrics)

      metrics.accessedBroadcasts.foreach { case broadcasts =>
        broadcasts.foreach { case broadcastId =>
          val broadcastSize = sizeForBlock(BroadcastBlockId(broadcastId))
          if (broadcastSize > BlockAccessListener.MINIMUM_BROADCAST_SIZE) {
            recordAccess(BroadcastBlockId(broadcastId), BlockAccess(BlockAccessType.Read))
          }
        }
      }

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
          } else if (!blockAccess.inputMetrics.isDefined) {
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
          logInfo("Ignoring access " + blockAccess)
        }
      }
      if (metrics.accessedBlocks.size > 0) {
        logDebug("End of access set")
      }
    }
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
    if (sortTasks) {
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
    val blockId = BroadcastBlockId(broadcastCreated.id)
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
