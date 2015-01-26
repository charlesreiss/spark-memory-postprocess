package edu.berkeley.cs.amplab.sparkmem

import org.apache.spark.{TaskEndReason, Success => TaskEndSuccess}
import org.apache.spark.executor.{TaskMetrics, BlockAccess, BlockAccessType}
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.{SparkListenerApplicationStart,
                                   SparkListenerApplicationEnd,
                                   SparkListenerTaskEnd}
import org.apache.spark.storage.{BlockId, BlockStatus, RDDBlockId, ShuffleBlockId, BroadcastBlockId}

import scala.collection.mutable

object BlockAccessListener {
  case class ShuffleWritePiece(shuffleId: Int, piece: Int)
  // A reduce task might combine multiple shuffles, potentially with different partitioning.
  // We treat each case seperately for the purpose of figuring out the size
  case class ShuffleReadPiece(shuffleIds: List[Int], pieceStarts: List[Int], pieceEnds: List[Int])
}

class BlockAccessListener extends SparkListener with Logging {
  private var didStart = false
  private val rddStack = new PriorityStack
  private val broadcastStack = new PriorityStack
  private val shuffleStack = new PriorityStack

  private val topRddBlockSizes = new TopK
  private val topShuffleMemorySizes = new TopK
  private val topShuffleDiskBlockSizes = new TopK

  // FIXME: Track block size sources
  private val blockSizes: mutable.Map[BlockId, Long] = mutable.Map.empty[BlockId, Long]
  // FIXME: Track shuffle in-memory sizes

  // FIXME: Track shuffle on-disk sizes
  // To do this:
  //    Maintain taskId -> stageId map (for unended tasks)
  import BlockAccessListener._
  private val seenShuffleWritePieces = mutable.Set.empty[ShuffleWritePiece]
  private val shuffleWriteSize = mutable.Map.empty[Int, Long]
  private val seenShuffleReadPieces = mutable.Set.empty[ShuffleReadPiece]
  private def nameShuffle(shuffleId: Int): String = "shuffle#" + shuffleId
  private def accumulateShuffle(metrics: TaskMetrics) {
    val writeSize = metrics.shuffleWriteMetrics.map(_.shuffleBytesWritten).getOrElse(0L)
    // FIXME: This includes BOTH ends of the shuffle
    val memorySize = metrics.shuffleMemoryMetrics.map(_.shuffleOutputBytes).getOrElse(0L)
    val haveRead = metrics.readShuffles.isDefined
    metrics.readShuffles.map { shuffleParts =>
      val shuffleIds = shuffleParts.map(_._1).toList
      val startPieces = shuffleParts.map(_._2).toList
      val endPieces = shuffleParts.map(_._3).toList
      shuffleIds.foreach { shuffleId => 
        shuffleStack.read(nameShuffle(shuffleId), Some(shuffleWriteSize(shuffleId)),
                          shuffleWriteSize(shuffleId).toDouble)
      }
      val part = ShuffleReadPiece(shuffleIds, startPieces, endPieces)
      topShuffleMemorySizes.insert(memorySize)
    }
    metrics.writtenShuffles.map { shuffleParts => 
      assert(shuffleParts.size == 1)
      shuffleParts.map { case (shuffleId, mapId) => 
        val writePiece = ShuffleWritePiece(shuffleId, mapId)
        if (!seenShuffleWritePieces.contains(writePiece)) {
          shuffleWriteSize(shuffleId) = shuffleWriteSize.getOrElse(shuffleId, 0L) + writeSize
          topShuffleDiskBlockSizes.insert(writeSize)
          if (!haveRead) {
            topShuffleMemorySizes.insert(memorySize)
          }
        }
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

  private def recordUpdate(blockId: BlockId, blockStatus: BlockStatus) {
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

  private def recordAccess(rawBlockId: BlockId, blockAccess: BlockAccess) {
    val blockId = recordedBlockIdForBlock(rawBlockId)
    val whichStack = blockId match {
      /* FIXME: Actually generate these in logs. */
      case BroadcastBlockId(_, _) => Some(broadcastStack)
      /* FIXME: Actually generate these in logs. */
      case ShuffleBlockId(_, _, _) => Some(shuffleStack)
      case RDDBlockId(_, _) => Some(rddStack)
      case _ => {
        logError("Unknown block ID type for " + rawBlockId)
        None
      }
    }
    whichStack match {
      case Some(stack) =>
        if (blockAccess.accessType == BlockAccessType.Read) {
          stack.read(nameBlock(blockId), Some(sizeForBlock(blockId)), costForBlock(blockId))
        } else {
          stack.write(nameBlock(blockId), sizeForBlock(blockId))
        }
      case None => {}
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    if (taskEnd.reason == TaskEndSuccess) {
      val metrics = taskEnd.taskMetrics
      // TODO: Process shuffle{Read,Write,Memory}Metrics.

      accumulateShuffle(metrics)

      /* Infer block sizes from updated blocks. */
      for ((blockId, blockStatus) <- metrics.updatedBlocks.getOrElse(Nil)) {
        recordUpdate(blockId, blockStatus)
      }

      /* Infer block sizes from read blocks. */
      for ((blockId, blockAccess) <- metrics.accessedBlocks.getOrElse(Nil)) {
        recordAccessSize(blockId, blockAccess)
      }

      if (metrics.updatedBlocks.getOrElse(Nil).size > 0) {
        assert(metrics.accessedBlocks.getOrElse(Nil).size > 0)
      }

      /* Actual record accesses in priority stack. */
      var misses = List.empty[BlockId]  // List of "active" misses
      for ((blockId, blockAccess) <- metrics.accessedBlocks.getOrElse(Nil)) {
        if (blockAccess.accessType == BlockAccessType.Read) {
          if (!blockAccess.inputMetrics.isDefined) {
            misses = blockId :: misses
          } else if (misses.headOption == Some(blockId)) {
            misses = misses.tail
          }
        }

        // Ignore block accesses taken to recover from a read miss.
        if (misses.isEmpty) {
          recordAccess(blockId, blockAccess)
        }
      }
    }
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
    recordTopSizes()
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart) {
    assert(!didStart) /* Don't support multiple applications yet. */
    didStart = true
  }

  /* FIXME: Use onUnpersistRDD events? */
}
