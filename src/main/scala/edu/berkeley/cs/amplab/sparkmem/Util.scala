package edu.berkeley.cs.amplab.sparkmem

private[sparkmem] object Util {
  def bytesToString(bytes: Long): String = {
    if (bytes > 1024L * 1024L * 1024L)
      "%.1f GB".format(bytes / 1024. / 1024. / 1024.)
    else if (bytes > 1024L * 1024L)
      "%.1f MB".format(bytes / 1024. / 1024.)
    else
      "%.1f KB".format(bytes / 1024.)
  }
}
