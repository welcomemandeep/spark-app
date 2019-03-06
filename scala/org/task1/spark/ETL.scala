


package org.task1.spark

import org.apache.spark.sql._


object ETL {
  def main(args: Array[String]) {
    val filePath = "s3://aviationontime/test/*/*.csv" // Should be some file on your system
    //val spark = new org.apache.spark.SparkContext()
    val sqlContext = SparkSession.builder.appName("ETL Application").getOrCreate().sqlContext
    val df = sqlContext.read.format("csv").option("header",true).load(filePath)
    df.select("FlightDate","Carrier","Flights",
      "Origin","Dest","AirlineId","DepTime","DepDelayMinutes",
      "ArrTime","ArrDelayMinutes","DayOfWeek","FlightNum","UniqueCarrier", "ArrDelay").write.format("csv")
      .option("header",true).save("hdfs:///root/airline_on_time_performance_cleaned_data")
    
    
    
  }
}
