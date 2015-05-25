package edu.berkeley.cs.amplab.sparkmem

case class ProposedConfig(
  val coresPerWorker: Int,
  val workers: Int,
  val shuffleSize: Long,
  val storageSize: Long,
  val storageUnrollSize: Long,
  val shuffleStorageSize: Long,
  val workerJvmSize: Long,
  val workerTotalSize: Long
) {
  import Util.bytesToString

  private val workerJvmSizeStr = "%.0fm".format(workerJvmSize / 1024. / 1024.)

  val extraJvmSpace = workerJvmSize - storageSize - shuffleSize
  
  def storagePortion = storageSize.toDouble / workerJvmSize
  def storageUnrollPortion = storageUnrollSize.toDouble / storageSize
  def shufflePortion = shuffleSize.toDouble / workerJvmSize

  def configFile: String = s"""
# Configuration for $workers workers each with $coresPerWorker tasks.
## Requires ${bytesToString(workerJvmSize)} (JVM) + ${bytesToString(shuffleStorageSize)} (page cache) per node
St
# ${bytesToString(storageSize)}
spark.storage.memoryFraction $storagePortion

# ${bytesToString(shuffleSize)}
spark.shuffle.memoryFraction $shufflePortion

# ${bytesToString(storageUnrollSize)}
spark.storage.unrollFraction $storageUnrollPortion

## ${bytesToString(workerJvmSize)} including ${bytesToString(extraJvmSpace)} not for storage/shuffle
spark.executor.memory $workerJvmSizeStr
"""
}

object ProposedConfig {
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
      workerTotalSize = orig.workerTotalSize + extraSpace
    )
  }

  def forWorkerSize(
      usageInfo: UsageInfo, cores: Int, targetWorkerSize: Long,
      settings: ProposedConfigSettings = ProposedConfigSettings.DEFAULT): ProposedConfig = {
    def forCount(workers: Int) = forWorkerCount(usageInfo, cores, workers, settings)
    var lower = 1
    var upper = 1024 * 32
    while (lower < upper) {
      val middle = (lower + upper) / 2
      val middleConfig = forCount(middle)
      if (middleConfig.workerTotalSize > targetWorkerSize) {
        upper = middle
      } else {
        lower = middle
      }
    }
    return slackenConfig(forCount(lower), targetWorkerSize, settings)
  }

  def forWorkerCount(
      usageInfo: UsageInfo, cores: Int, workers: Int,
      settings: ProposedConfigSettings = ProposedConfigSettings.DEFAULT): ProposedConfig = {
    val assumedSlack = settings.assumedSlack
    val gcBonus = settings.gcBonus

    val storageSize = math.max(
      (usageInfo.broadcastAllSize + usageInfo.rddActiveSize(cores) +
       usageInfo.rddAllSize / workers) / assumedSlack,
      cores * settings.minStorageSizePerCore
    )
    val storageUnrollSize = math.max(
      usageInfo.rddActiveSize(cores) / assumedSlack,
      cores * settings.minUnrollStorageSizePerCore
    )
    val shuffleSize = math.max(
      usageInfo.shuffleActiveSizeWithAdjust(cores) / assumedSlack,
      cores * settings.minShuffleSizePerCore
    )

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
      workerTotalSize = workerJvmSize.toLong + shuffleStorageSize
    )
  }
}
