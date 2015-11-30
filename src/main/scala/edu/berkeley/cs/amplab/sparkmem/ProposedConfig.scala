package edu.berkeley.cs.amplab.sparkmem

case class ProposedConfig(
  val coresPerWorker: Int,
  val workers: Int,
  val shuffleSize: Long,
  val storageSize: Long,
  val storageUnrollSize: Long,
  val shuffleStorageSize: Long,
  val workerJvmSize: Long,
  val workerTotalSize: Long,

  // Exist for debugging/evaluation only
  val storageSizeActive: Option[Long] = None,
  val storageSizeCached: Option[Long] = None,
  val storageSizeBroadcast: Option[Long] = None,

  val shuffleSizeExplanation: String = "",
  val storageSizeExplanation: String = "",
  val storageUnrollSizeExplanation: String = ""
) {
  import Util.bytesToString

  private val workerJvmSizeStr = "%.0fm".format(workerJvmSize / 1024. / 1024.)

  val extraJvmSpace = workerJvmSize - storageSize - shuffleSize
  
  def storagePortion = storageSize.toDouble / workerJvmSize
  def storageUnrollPortion = storageUnrollSize.toDouble / storageSize
  def shufflePortion = shuffleSize.toDouble / workerJvmSize

  private def maybeRawSetting(name: String, value: Option[Long]): String =
    value match {
      case Some(x) => s"#$name=$x\n"
      case None => ""
    }

  def configFile: String = s"""
# Configuration for $workers workers each with $coresPerWorker tasks.
## Requires ${bytesToString(workerJvmSize)} (JVM) + ${bytesToString(shuffleStorageSize)} (page cache) per node

# ${bytesToString(storageSize)} = ${storageSizeExplanation}
spark.storage.memoryFraction $storagePortion

# ${bytesToString(shuffleSize)} = ${shuffleSizeExplanation}
spark.shuffle.memoryFraction $shufflePortion

# ${bytesToString(storageUnrollSize)} = ${storageUnrollSizeExplanation}
spark.storage.unrollFraction $storageUnrollPortion

## ${bytesToString(workerJvmSize)} including ${bytesToString(extraJvmSpace)} not for storage/shuffle
spark.executor.memory $workerJvmSizeStr

### Raw parameters represented by above config:
#coresPerWorker=${coresPerWorker}
#workers=$workers
#shuffleSize=$shuffleSize
#storageSize=$storageSize
#storageUnrollSize=$storageUnrollSize
#shuffleStorageSize=$shuffleStorageSize
#workerJvmSize=$workerJvmSize
#workerTotalSize=$workerTotalSize
""" + 
  maybeRawSetting("storageSizeCached", storageSizeCached) +
  maybeRawSetting("storageSizeActive", storageSizeActive) +
  maybeRawSetting("storageSizeBroadcast", storageSizeBroadcast)
}

object ProposedConfig {
  import Util.bytesToString

  def slackenConfig(
      orig: ProposedConfig,
      newWorkerTotalSize: Long,
      settings: ProposedConfigSettings
  ): ProposedConfig = {
    val extraSpace = newWorkerTotalSize - orig.workerTotalSize
    val extraStorage = (extraSpace * settings.portionExtraStorage).toLong
    val extraUnrollStorage = (extraSpace * settings.portionExtraUnrollStorage).toLong
    assert(extraUnrollStorage <= extraStorage)
    val extraShuffle = (extraSpace * settings.portionExtraShuffle).toLong
    val extraJvmSpace = extraStorage + extraShuffle + (extraSpace * settings.portionExtraUnassignedJvm).toLong
    val extraShuffleStorage = extraSpace - extraJvmSpace
    return ProposedConfig(
      coresPerWorker = orig.coresPerWorker,
      workers = orig.workers,
      shuffleSize = orig.shuffleSize + extraShuffle,
      storageSize = orig.storageSize + extraStorage,
      storageUnrollSize = orig.storageUnrollSize + extraUnrollStorage,
      shuffleStorageSize = orig.shuffleStorageSize + extraShuffleStorage,
      workerJvmSize = orig.workerJvmSize + extraJvmSpace,
      workerTotalSize = orig.workerTotalSize + extraSpace,
      
      storageSizeExplanation = s"${orig.storageSizeExplanation} + " +
                               s"${bytesToString(extraStorage)} slack",
      shuffleSizeExplanation = s"${orig.shuffleSizeExplanation} + " +
                               s"${bytesToString(extraShuffle)} slack",
      storageUnrollSizeExplanation = s"${orig.storageUnrollSizeExplanation} + " +
                                     s"${bytesToString(extraShuffle)} slack"

    )
  }

  def forWorkerSize(
      usageInfo: UsageInfo, cores: Int, targetWorkerSize: Long,
      settings: ProposedConfigSettings = ProposedConfigSettings.DEFAULT,
      noSlacken: Boolean = false): ProposedConfig = {
    def forCount(workers: Int) = forWorkerCount(usageInfo, cores, workers, settings)
    var lower = 1
    var upper = 1024 * 32
    while (lower < upper) {
      val middle = (lower + upper) / 2
      val middleConfig = forCount(middle)
      if (middleConfig.workerTotalSize > targetWorkerSize) {
        lower = middle + 1
      } else {
        upper = middle
      }
    }
    assert(forCount(lower).workerTotalSize < targetWorkerSize)
    assert(lower == 1 || forCount(lower - 1).workerTotalSize > targetWorkerSize)
    if (noSlacken)
      return forCount(lower)
    else
      return slackenConfig(forCount(lower), targetWorkerSize, settings)
  }

  def forWorkerCount(
      usageInfo: UsageInfo, cores: Int, workers: Int,
      settings: ProposedConfigSettings = ProposedConfigSettings.DEFAULT): ProposedConfig = {
    val assumedSlack = settings.assumedSlack
    val gcBonus = settings.gcBonus
    val explainSlack = f"${1.0 / assumedSlack * 100.0 - 100.0}%.0f%% safetyFraction"

    val partitionScale  = settings.scalePartitionsFactorFor(cores * workers)

    val storageSizeBroadcast = usageInfo.broadcastAllSize
    val storageSizeActive = usageInfo.rddActiveSize(cores) * partitionScale
    val storageSizeCached = usageInfo.rddAllSize / workers
    val storageSize = math.max(
      (usageInfo.broadcastAllSize + usageInfo.rddActiveSize(cores) * partitionScale +
       usageInfo.rddAllSize / workers) / assumedSlack,
      cores * settings.minStorageSizePerCore
    )
    val storageSizeExplanation =
      s"${bytesToString(usageInfo.broadcastAllSize)} broadcast + " +
      s"${bytesToString(usageInfo.rddActiveSize(cores))} active RDDs + " +
      s"${bytesToString(usageInfo.rddAllSize / workers)} hot cached RDDs + " +
      explainSlack

    // FIXME(charles): Probably need a special case for very small storage sizes
    val storageUnrollSize = math.max(
      usageInfo.rddActiveSize(cores) * partitionScale / assumedSlack,
      cores * settings.minUnrollStorageSizePerCore
    )
    val storageUnrollSizeExplanation = 
      s"${bytesToString(usageInfo.rddActiveSize(cores))} active RDDs + " +
      explainSlack

    val shuffleSize = math.max(
      usageInfo.shuffleActiveSize(cores) * partitionScale / assumedSlack,
      cores * settings.minShuffleSizePerCore
    )
    val shuffleSizeExplanation =
      s"${bytesToString(usageInfo.shuffleActiveSizeWithAdjust(cores))} + " +
      explainSlack

    val shuffleStorageSize = 
      if (settings.ignoreNonJvmSpace)
        0L
      else
        usageInfo.shuffleAllSize / workers

    val workerJvmSize = (storageSize + shuffleSize) * gcBonus

    return ProposedConfig(
      coresPerWorker = cores,
      workers = workers,
      shuffleSize = shuffleSize.toLong,
      storageSize = storageSize.toLong,
      storageUnrollSize = storageUnrollSize.toLong,
      workerJvmSize = workerJvmSize.toLong,
      shuffleStorageSize = shuffleStorageSize,
      workerTotalSize = workerJvmSize.toLong + shuffleStorageSize,

      storageSizeCached = Some(storageSizeCached.toLong),
      storageSizeActive = Some(storageSizeActive.toLong),
      storageSizeBroadcast = Some(storageSizeBroadcast.toLong),

      storageSizeExplanation = storageSizeExplanation,
      storageUnrollSizeExplanation = storageUnrollSizeExplanation,
      shuffleSizeExplanation = shuffleSizeExplanation
    )
  }
}
