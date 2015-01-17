package edu.berkeley.cs.amplab.sparkmem

private[sparkmem] final class TopK {
  private val K = 100
  private val values = new Array[Long](K * 2)
  private var extent = 0

  private def fixup() {
    if (extent > K) {
      java.util.Arrays.sort(values, 0, extent)
      java.lang.System.arraycopy(values, extent - K, values, 0, K)
      extent = K
    }
  }

  def insert(item: Long) {
    if (extent >= values.size) {
      fixup()
    }
    values(extent) = item
    extent += 1
  }

  def get: Seq[Long] = {
    fixup()
    return values.take(extent).sortBy(_ * -1L)
  }
}
