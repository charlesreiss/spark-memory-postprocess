# Memory Configuration Recommendation Tools For Apache Spark

This package contains the log-processing and configuration generation part of some memory
configuration tools for Apache Spark. These tools are intended to take statistics from an
instrumented run and produce a recommended configuration based on sizes measured during
that run. The goal of the recommended configuration is to keep the entire computation
in memory, including not recomputing any datasets the user/application requested Spark
persist in memory.

*STABILITY NOTE*: These tools are very much undertested, especially with respect to
performance at analyzing largish logs, producing multi-worker configurations,
and the reasonableness of shuffle storage (non-JVM memory) recommendations.
Use with caution. Please contact me (charles@eecs.berkeley.edu) with any
questions, concerns, bug reports, weird behavior, etc.

# Basic Usage
- Get binaries of a version of Spark with instrumention. For the patched Spark, build from
  source using the the
  [extra-monitoring branch](https://github.com/woggle/spark/tree/extra-monitoring)
  of my fork on Spark on github, or use a prebuilt binary distribution from
  [here](https://www.eecs.berkeley.edu/~charles/spark-1.4.0-memanalysis-SNAPSHOT-0521-hadoop2.2.tar.gz).

- Build these tools using `sbt/sbt assembly`.

- Run the program under a version of Spark patched with extra monitoring. This
  patch is available from   When running the program, set the Spark configuration option `spark.extraMetrics.enabled` to `true`
  and enable event log writing to somewhere. Compressing the event log is probably preferable.

- The run the tool in this repository on the event log to produce a JSON file containing a summary
  of important metrics from the event log:

    ./run --jsonFile program-summary.json --logFile the-event-log \
          --consolidateRDDs --tasksInOrder

  I recommend using the option `--consolidateRDDs`, which should increase processing speed for
  runs with high partition counts by considering all partitions of RDD as a unit for some analyses.

  I recommend trying the option `--tasksInOrder`, which always processes tasks in order by their
  TID (which is roughly in order by when they start, instead of roughly in order by when they
  finish). A downside, however, is that this requires more memory (task end events are buffered in
  memory to be sorted).

  In my testing, the main factor in processing speed seems to be event log parsing time, especially
  for large event logs. The main reason to have a event log that I have seen are executions which
  run many jobs with deep dependencies, which results in large JobStart, StageStart, etc. records
  being written. The current analyses processes the JSON of all log entries (it is reusing
  Spark's event log playing code that's used by the job history server), even though, for example,
  lineage information is not used.


- Using the resulting JSON file, produce a configuration either by passing --targetWorkers:

    ./run --jsonFile program-summary.json --makeConfig --targetWorkers 1

  or by passing --targetMemoryPerWorker:

    ./run --jsonFile program-summary.json --makeConfig --targetMemoryPerWorker 64g

  The tool will indicate desired worker count with a comment. Note that the tool tries to set aside
  space for page cache for stored shuffle data, so you don't need to include that in
  targetMemoryPerWorker.

  Note that the resulting configuration will include a setting for executor memory but not driver memory;
  if you intend to run the Spark program in local mode, you should adjust this.

  The configuration tool makes some assumptions which are configurable using a configuration
  file like the one in `conf/make-config.properties.template`, specified using the command-
  line option --makeConfigProperties. See the comments in that file.

# A Note on Compressed OOPS

Since we reuse the size estimates Spark uses internally,
the measurements of sizes will likely depend heavily on whether your JVM is configured to use
compressed object pointers. In (64-bit) OpenJDK or Oracle JDK, this is enabled by default for heap sizes
less than 64GB and unsupported otherwise. You can explicitly disable compressed object pointers
in OpenJDK using `-XX:-UseCompressedOops`.

# Partition Counts

This tool assumes that partitioning will not change between the example run and the run
for the targetted configuration. If your partitioning will depend on the number of active
tasks or you want to estimate the effect of adjusting the partition count on input, you can
supply `scalePartitions*` settings in --makeCOnfigProperties file as shown in the template.
Note that these adjustments don't try to do anything smart about uneven partitioning.

# Page Cache

For shuffles, Spark generally writes the data to be sent to reducers to files on the filesystem,
and reads it whenever the reducers try to access it. To keep this in memory, one would need to rely
on the page cache. Therefore, by default, this tools

# No shuffle space

Some event logs have no measurements of sizes during the shuffle, leading to a very small allocation
of space to the shuffle. I believe this is generally because no shuffle requiring in-memory
aggregation of keys takes place (using the codepaths that allocate `shuffle' memory),
in which case this allocation is correct, but I suspect I may have missed some case.

# Things we don't handle well, probably

## Load balance assumptions

This tool assumes that cached blocks are approximately evenly distributed across workers (but
also adds space for the largest partitions). Likely there should be some explicit adjustment for
load misbalance based on the normal cached partition size.

## Largest partitions from different datasets

When computing worst-case sizes, this tool computes the size for the largest K partitions to
reside on a worker that can run K tasks. This is more pessimistic than it needs to be since
some partitions can't be computed simulatenously.

## Miscellaenous temporary space

I've heard speculation that a small but sometimes significant amount of extra space
is used:

* to buffer shuffle partitions being sent over the network before they are merged;
* for buffers to support serialization and deserialization;

and that these memory requirements aren't accounted for the shuffle or storage regions of Spark.
If so, we should account for this extra space (especially in cases where would
assume that almost no space is required for tasks).

## Explicit unpersisting

Currently, this tool ignores explicitly unpersisted RDDs, which may cause overestimates
in cases when the RDD would not be dropped by an LRU-like policy.
