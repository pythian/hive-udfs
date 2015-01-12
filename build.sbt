name := "pythian-hive-udfs"

version := "0.1"

organization := "com.pythian"

scalaVersion := "2.11.4"

test in assembly := {}

resolvers += "cloudera"  at "https://repository.cloudera.com/artifactory/cloudera-repos/"

libraryDependencies += "org.apache.hive" % "hive-exec" % "0.12.0" % "provided"

libraryDependencies ++= Seq(
  "org.apache.hive" % "hive-common" % "0.12.0" % "provided",
  "org.apache.hive" % "hive-serde" % "0.12.0" % "provided",
  "org.apache.hadoop" % "hadoop-core" % "1.1.1" % "provided",
  "org.apache.hbase" % "hbase-common" % "0.98.+" % "provided",
  "org.scalatest" % "scalatest_2.11" % "2.1.6" % "test"
)
