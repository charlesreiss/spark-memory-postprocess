package edu.berkeley.cs.amplab.sparkmem

private[sparkmem] object Util {
  def bytesToString(bytes: Long): String = "%.1f GB".format(bytes / 1024. / 1024. / 1024.)
}
