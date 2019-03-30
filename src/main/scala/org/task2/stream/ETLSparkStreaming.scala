package org.task2.stream

import org.apache.spark.sql._

object ETLSparkStreaming {
  def main(args: Array[String]) {
    val filePath = "hdfs:///root/airline_on_time_performance_cleaned_data" // Persisted on file system
    val spark = new org.apache.spark.SparkContext()
    val sqlContext = SparkSession.builder.appName("ETL Application").getOrCreate().sqlContext
    val df = sqlContext.read.format("csv").option("header",true).load(filePath)
    val topic = "cleansed-data"
    val sparkSession = SparkSession.builder.appName("Tasks Application").getOrCreate()
    case class Schema (
    Year:String,FlightDate:String,
    Carrier:String, Flights:String,
    Origin:String, Dest:String,
    AirlineId:String,DepTime:String,
    DepDelayMinutesString:String, ArrTime:String,
    ArrDelayMinutes:String,DayOfWeek:String,
    FlightNum:String,UniqueCarrier:String,
    ArrDelay:String)

    val df2 = df.select("Year","FlightDate","Carrier","Flights",
      "Origin","Dest","AirlineId","DepTime","DepDelayMinutes",
      "ArrTime","ArrDelayMinutes","DayOfWeek","FlightNum","UniqueCarrier", "ArrDelay");

   df2.selectExpr("CAST(Year as STRING) as key","to_json(Struct(*)) as value")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "3.91.59.193:9092")
      .option("topic", topic)
      .save()
  }
}
