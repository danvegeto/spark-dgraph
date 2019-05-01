name := "spark-dgraph"
version := "0.1"
scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "io.dgraph" % "dgraph4j" % "1.7.1",
  "com.google.protobuf" % "protobuf-java" % "3.7.1",
  "org.apache.spark" %% "spark-core" % "2.4.2",
  "org.apache.spark" %% "spark-sql" % "2.4.2"
)