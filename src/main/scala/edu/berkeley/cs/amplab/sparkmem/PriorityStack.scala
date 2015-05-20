package edu.berkeley.cs.amplab.sparkmem

import scala.collection.mutable.Buffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

object PriorityStack {
  final case class Item(
    val key: String,
    var size: Long,
    var priority: Long,
    var next: Item,
    var prev: Item
  )
}

final class PriorityStack extends Logging {
  private var head: PriorityStack.Item = null
  private var contained = HashMap.empty[String, PriorityStack.Item]
  private var totalSize = 0L
  private var time: Long = 0
  private val costLog = Buffer[(Long, Double)](0L -> 0.0)
  private var maybeLastLookup: Option[PriorityStack.Item] = None
  private var lastCost: Long = 0L

  private def updateCacheForMove(causeKey: String, oldPriority: Long, newPriority: Long, newSize: Long, deltaSize: Long) {
    maybeLastLookup.foreach { case lastLookup =>
      if (newPriority > lastLookup.priority && oldPriority < lastLookup.priority) {
        lastCost += newSize
        logDebug(s"updating cost (moved) += $newSize to $lastCost for ${lastLookup.key} (caused by $causeKey moving from $oldPriority over ${lastLookup.priority} to $newPriority)")
      } else if (oldPriority == lastLookup.priority) {
        maybeLastLookup = None
        logDebug(s"uncaching ${lastLookup.key}")
      } else if (newPriority > lastLookup.priority && oldPriority > lastLookup.priority) {
        lastCost += deltaSize
        logDebug(s"updating cost (resized) += $deltaSize to $lastCost for ${lastLookup.key}")
      } else if (newPriority < lastLookup.priority && oldPriority < lastLookup.priority) {
        // do nothing
      } else {
        logError("Unhandled case")
      }
    }
  }

  private def maybeCacheForLookup(theNode: PriorityStack.Item, theCost: Long): Unit = {
    logDebug(s"caching ${theNode.key} at ${theCost}")
    if (!maybeLastLookup.isDefined || theCost < lastCost) {
      maybeLastLookup = Some(theNode)
      lastCost = theCost
    }
  }

  @inline private def findNodeCached(key: String, cacheFind: Boolean = false):
        (Option[PriorityStack.Item], Long, Option[PriorityStack.Item]) = {
    contained.get(key) match {
    case Some(resultItem) => 
      var size = resultItem.size
      val (startSize, startPoint) =
        maybeLastLookup.filter { last => last.priority > resultItem.priority }.
                        map { last => (lastCost, last) }.
                        getOrElse((0L -> head))
      logDebug(s"Using ($startSize @ ${startPoint.key})")
      var depth = 0
      if (head != resultItem) {
        size += startSize
        var current = startPoint
        var previous = current 
        while (current.next ne resultItem) {
          logDebug(s"Continue past ${current.key} adding ${current.size}")
          size += current.size
          previous = current
          current = current.next
          assert(previous.priority > current.priority)
          depth += 1
        }
        if (current != previous && cacheFind && depth > 20) {
          maybeCacheForLookup(current, size)
        }
        size += current.size
        return (Some(resultItem), size, Some(current))
      } else {
        return (Some(resultItem), size, None)
      }
    case None =>
      return (None, totalSize, None)
    }
  }

  @inline
  private def findNodeUncached(key: String, cacheFind: Boolean = false):
        (Option[PriorityStack.Item], Long, Option[PriorityStack.Item]) = {
    contained.get(key) match {
    case Some(resultItem) =>
      var size = resultItem.size
      if (head != resultItem) {
        var current = head
        var previous = current
        while (current.next != resultItem) {
          size += current.size
          previous = current
          current = current.next
          assert(previous.priority > current.priority, s"${previous.key} @ ${previous.priority} versus ${current.key} @ ${current.priority}")
        }
        if (current != previous && cacheFind) {
          maybeCacheForLookup(current, size)
        }
        size += current.size
        return (Some(resultItem), size, Some(current))
      } else {
        return (Some(resultItem), size, None)
      }
    case None =>
      return (None, totalSize, None)
    }
  }

  @inline def findNodeChecked(key: String):
        (Option[PriorityStack.Item], Long, Option[PriorityStack.Item]) = {
     val resultOne = findNodeCached(key, false)
     val resultTwo = findNodeUncached(key, true)
     assert(resultOne == resultTwo, s"$key: [cached] ${resultOne._3.map(_.key)} ${resultOne._2}/[uncached] ${resultTwo._2}")
     return resultOne
  }

  @inline def findNode(key: String): 
        (Option[PriorityStack.Item], Long, Option[PriorityStack.Item]) = findNodeCached(key, true)

  @inline
  private def insert(key: String, size: Long, priority: Long): Unit = {
    logDebug(s"insert($key, $size, $priority)")
    val maybeOldNode = contained.get(key)
    val oldSize = maybeOldNode.map(_.size).getOrElse(0L)
    logDebug(s"oldSize=$oldSize")
    totalSize += size - oldSize
    maybeOldNode.foreach { oldNode => 
      updateCacheForMove(key, oldNode.priority, priority, size, size - oldNode.size)
    }
    var node = maybeOldNode.getOrElse(
      new PriorityStack.Item(key, size, priority, null, null)
    )
    node.size = size // in case old node
    node.priority = priority
    /* remove old node */
    if (node.next != null) {
      node.next.prev = node.prev
    }
    if (node.prev != null) {
      node.prev.next = node.next
    }
    if (node == head) {
      head = node.next
    }

    /* find insertion spot (probably head) */
    var nextNode = head
    while (nextNode != null && nextNode.priority > priority) {
      nextNode = nextNode.next
    }
    
    /* insert */
    if (nextNode == head) {
      head = node
      assert(nextNode == null || nextNode.prev == null)
    }
    node.next = nextNode
    if (nextNode != null) {
      node.prev = nextNode.prev
      nextNode.prev = node
    } else {
      node.prev = null
    }
    contained.put(key, node)
    sanityCheck()
  }

  sealed abstract class AccessType
  case object Read extends AccessType
  case object Write extends AccessType

  def process(
    accessType: AccessType,
    key: String,
    maybeSize: Option[Long],
    maybeCost: Option[Double]
  ): Unit = {
    val cost = maybeCost.getOrElse(0.0)
    time = time + 1
    accessType match {
    case Read => 
      val (maybeTheNode, depth, prevNode) = findNode(key)
      var oldSize: Option[Long] = None
      maybeTheNode match {
      case Some(theNode) =>
        logDebug(s"Adding entry to cost log $depth->$cost")
        costLog += depth -> cost
        oldSize = Some(theNode.size)
      case None =>
        costLog += totalSize -> cost
      }
      maybeSize.orElse(oldSize) match {
      case Some(size) => 
        insert(key, size, time)
      case None => {}
        /* XXX */
      }
    case Write =>
      maybeSize match {
      case Some(size) => 
        insert(key, size, time)
      case None =>
        contained.get(key) match {
        case Some(node) => insert(key, node.size, time)
        case None => {}
        }
      }
    }
  }

  def read(blockId: String, size: Option[Long], cost: Double) {
    process(Read, blockId, size, Some(cost))
  }

  def write(blockId: String, size: Long) {
    process(Write, blockId, Some(size), None)
    process(Read, blockId, Some(size), None)
  }

  def getCostCurve: CostCurve = {
    sanityCheck()
    val sortedLog = costLog.clone.sortBy(-_._1)
    var prevSize = sortedLog.head._1
    var curSize = prevSize
    var totalCost = 0.0 
    val result = Buffer.empty[(Long, Double)]
    if (curSize > 0.0) {
      result += curSize -> 0.0
    }
    for ((size, cost) <- sortedLog) {
      if (size != prevSize) {
        /* output prevSize -> cost unless prevSize  == Long.MAX_VALUE */
        logDebug(s"Adding entry to curve $size -> $totalCost")
        result += size -> totalCost
        prevSize = curSize
        curSize = size
      }
      totalCost += cost
    }
    if (result.size == 0) {
      result += 0L -> totalCost
    }
    new CostCurve(result)
  }

  def sanityCheck(): Unit = {
    var numItems = 0L
    var computedTotalSize = 0L
    var current = head
    val seenItems = HashSet.empty[String]
    while (current != null) {
      assert(!seenItems.contains(current.key), "already seen " + current.key +
        " (seen so far " + seenItems + ")")
      seenItems += current.key
      numItems += 1
      computedTotalSize += current.size
      assert(contained.contains(current.key), "contains " + current.key)
      assert(contained(current.key) == current, "matches for " + current.key)
      current = current.next
    }
    assert(contained.size == numItems, "expected " + numItems + " vs " + contained.size)
    assert(computedTotalSize == totalSize, "expected size " + computedTotalSize + 
                                           " vs " + totalSize)
  }
}
