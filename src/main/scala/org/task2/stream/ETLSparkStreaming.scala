package org.task2.stream

import org.apache.spark.sql._

object ETLSparkStreaming {
  def main(args: Array[String]) {
    val filePath = "file:///home/ikjotkaur/On_Time_On_Time_Performance_1988_1.csv" // Persisted on file system
    val accessKeyId = System.getenv("AWS_ACCESS_KEY_ID")
    val secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY")
    //val spark = new org.apache.spark.SparkContext()
    val sqlContext = SparkSession.builder.appName("ETL Application").getOrCreate().sqlContext
    val df = sqlContext.read.format("csv").option("header",true).load(filePath)
    val topic = "cleansed-data"
    val df2 = df.select("FlightDate","Carrier","Flights",
      "Origin","Dest","AirlineId","DepTime","DepDelayMinutes",
      "ArrTime","ArrDelayMinutes","DayOfWeek","FlightNum","UniqueCarrier", "ArrDelay");
    val md = df2.toJSON
    md.selectExpr("CAST(value as STRING) as value")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("topic", topic)
      .save()
  }
}
