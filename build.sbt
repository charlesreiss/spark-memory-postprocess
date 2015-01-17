import AssemblyKeys._

assemblySettings

name := "spark-memanalysis"

version := "0.1"

organization := "edu.berkeley.cs.amplab"

scalaVersion := "2.10.4"

mainClass := Some("edu.berkeley.cs.amplab.sparkmem.ParseLogs")

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.3.0-memanalysis-SNAPSHOT"
)

{
  val defaultHadoopVersion = "1.0.4"
  val hadoopVersion = scala.util.Properties.envOrElse("SPARK_HADOOP_VERSION", defaultHadoopVersion)
  libraryDependencies += "org.apache.hadoop" % "hadoop-client" % hadoopVersion
}

resolvers += Resolver.mavenLocal

fork in run := true

javaOptions in run += "-Xmx8g"
