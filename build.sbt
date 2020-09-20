name := "spark-basics"

version := "0.1"

scalaVersion := "2.12.10"
val sparkVersion = "3.0.1"
val logVersion = "2.4.1"

libraryDependencies ++= Seq (
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.logging.log4j" % "log4j-api" % logVersion,
  "org.apache.logging.log4j" % "log4j-core" % logVersion
)