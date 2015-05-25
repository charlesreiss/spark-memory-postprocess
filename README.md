# Memory Configuration Recommendation Tools For Apache Spark

This package contains the log-processing and configuration generation part of some memory
configuration tools for Apache Spark. These tools are intended to take statistics from an
instrumented run and produce a recommended configuration based on sizes measured during
that run.

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
          --consolidateRDDs

  I recommend using the option `--consolidateRDDs`, which should substantially increase log processing
  speed. (This option groups RDD partitions together for some analyses, which may hurt accuracy
  if your program often accesses subsets of RDDs, but should substantially improve performance because
  many heuristics are missing to make dealing with large block counts reasonably efficient.)

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

# Likely problems

## A note on compressed OOPS

Since we reuse the size estimates Spark uses internally,
the measurements of sizes will likely depend heavily on whether your JVM is configured to use
compressed object pointers. In (64-bit) OpenJDK, this is enabled by default for heap sizes
less than 64GB and unsupported otherwise. You can explicitly disable compressed object pointers
in OpenJDK using `-XX:-UseCompressedOops`.
