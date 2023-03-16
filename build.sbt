ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.11"

lazy val root = (project in file("."))
  .settings(
    name := "spark_work"
  )
val sparkVersion = "3.2.1"
val kafka_clients = "2.8.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.kafka" % "kafka-clients" % "2.8.2"

)


fork in run := true
javaOptions in run ++= Seq(

    "-Dlog4j.debug=true",
    "-Dlog4j.configuration=log4j.properties"
                          )
outputStrategy := Some(StdoutOutput)