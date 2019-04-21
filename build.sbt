name := "hp"

version := "0.1"

scalaVersion := "2.11.12"

resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"

assemblyJarName in assembly := "hp-spark-submit-retail-debug.jar"

mainClass in assembly := Some("com.scienaptic.jobs.App")

libraryDependencies ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-core" % "2.1.1",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.1.1",
  "org.apache.spark" %% "spark-sql" % "2.3.0"
)
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.0"
libraryDependencies += "com.crealytics" %% "spark-excel" % "0.11.0"
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}