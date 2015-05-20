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

  def configFile: String = """
## Configuration for $workers workers each with $cores tasks.
## Requires ${bytesToString(workerJvmSize)} (JVM) + ${bytesToString(shuffleStorageSize)} (page cache) per node
St
# ${bytesToString(storageSize)}
spark.storage.memoryFraction $storagePortion

# ${bytesToString(shuffleSize)}
spark.storage.shuffleFraction $shufflePortion

# ${bytesToString(storageUnrollSize)}
spark.storage.unrollFraction $storageUnrollPortion

## ${bytesToString(workerJvmSize)} including ${bytesToString(extraJvmSpace)} not for storage/shuffle
spark.executor.memory $workerJvmSizeStr
"""
}

case class ProposedConfigSettings(
  val gcBonus: Double = 1.2,
  val assumedSlack: Double = 0.9,

  val portionExtraStorage: Double = 0.45,
  val portionExtraUnrollStorage: Double = 0.1,
  val portionExtraShuffle: Double = 0.25,
  val portionExtraUnassignedJvm: Double = 0.05
)

object ProposedConfigSettings {
  val DEFAULT = ProposedConfigSettings()
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

    val storageSize = (usageInfo.broadcastAllSize + usageInfo.rddActiveSize(cores) +
                       usageInfo.rddAllSize / workers) / assumedSlack
    val storageUnrollSize = usageInfo.rddActiveSize(cores) / assumedSlack
    val shuffleSize = usageInfo.shuffleActiveSizeWithAdjust(cores) / assumedSlack

    val shuffleStorageSize = usageInfo.shuffleAllSize / workers

    val workerJvmSize = (storageSize + shuffleSize) * gcBonus

    val storagePortion = storageSize.toDouble / workerJvmSize
    val storageUnrollPortion = storageUnrollSize / storageSize

    val shufflePortion = shuffleSize.toDouble / workerJvmSize

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
