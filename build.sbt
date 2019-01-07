name := "hp"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-core" % "2.1.1",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.1.1",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.1.1",
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.1.1",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.7",
  "commons-cli" % "commons-cli" % "1.4",
  "org.apache.spark" %% "spark-sql" % "2.3.0"
)
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.0"