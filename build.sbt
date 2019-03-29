name := "spark-app"

version := "0.1"

scalaVersion := "2.11.11"
val sparkVersion = "2.3.0"


resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/",
  "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven",
  "confluent" at "https://mvnrepository.com/artifact/"
)



libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.1",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.kafka" % "kafka-clients" % "2.2.0",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.6.3"



)