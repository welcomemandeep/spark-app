package org.task2.stream

import org.apache.spark.sql._

object ETLSparkStreaming {
  def main(args: Array[String]) {
    //val filePath = "hdfs:///root/airline_on_time_performance_cleaned_data" // Persisted on file system
    val filePath = "hdfs:///root/airline_on_time_performance_cleaned_data"
    val sqlContext = SparkSession.builder.appName("ETL Application").getOrCreate().sqlContext
    val df = sqlContext.read.format("csv").option("header",true).load(filePath)
    val topic = "cleansed-data-new"
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
      "ArrTime","ArrDelayMinutes","DayOfWeek","FlightNum","UniqueCarrier", "ArrDelay")

   df2.selectExpr("CAST(Year as STRING) as key","to_json(Struct(*)) as value")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", sparkSession.sparkContext.getConf.getOption("spark.kafka.broker").getOrElse("ec2-3-91-59-193.compute-1.amazonaws.com"))
      .option("topic", topic)
      .option("enable.auto.commit",true)
      .save()
  }
}
