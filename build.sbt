name := "untitled1"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "joda-time" % "joda-time" % "2.10.5",
  "org.twitter4j" % "twitter4j-stream" % "4.0.7",
  "org.twitter4j" % "twitter4j-core" % "4.0.7"
)