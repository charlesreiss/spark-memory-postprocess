name := "spark-memanalysis"

version := "0.2-SNAPSHOT"

organization := "edu.berkeley.cs.amplab"

scalaVersion := "2.10.4"

mainClass := Some("edu.berkeley.cs.amplab.sparkmem.ParseLogs")

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.4.0-memanalysis-SNAPSHOT" from
    "https://www.eecs.berkeley.edu/~charles/spark-core_2.10-1.4.0-memanalysis-SNAPSHOT-0521.jar",
  "org.json4s" %% "json4s-jackson" % "3.2.11"
)

{
  val defaultHadoopVersion = "1.0.4"
  val hadoopVersion = scala.util.Properties.envOrElse("SPARK_HADOOP_VERSION", defaultHadoopVersion)
  libraryDependencies += "org.apache.hadoop" % "hadoop-client" % hadoopVersion
}

resolvers := Seq(Resolver.mavenLocal, DefaultMavenRepository)

fork in run := true

javaOptions in run += "-Xmx8g"

scalacOptions ++= Seq("-optimize")

assemblyMergeStrategy in assembly := {
  case x @ PathList("META-INF", _*) => {
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
  }
  case _ => MergeStrategy.first
}
