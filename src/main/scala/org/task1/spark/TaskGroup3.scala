package org.task1.spark


import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._


object TaskGroup3 {
  def main(args: Array[String]) {

    val filePath = "s3://aviationontime/test/2008/*.csv" // Should be some file on your system
    val sqlContext = SparkSession.builder.appName("Task Group 3 Application").getOrCreate().sqlContext
    val df = sqlContext.read.format("csv").option("header",true).load(filePath)
    val sparkSession = SparkSession.builder.appName("Tasks Application").getOrCreate()

    import sqlContext.implicits._


    val conf = new SparkConf(true)
    sparkSession.setCassandraConf(CassandraConnectorConf.KeepAliveMillisParam.option(10000))

    val distinct_dataframe = df.select($"Origin", $"Dest", $"DepTime", to_date($"FlightDate","yyyy-MM-dd").as("FlightDate"), $"DepTime",$"FlightNum" , $"UniqueCarrier",$"ArrDelay").distinct()
    val x_y_df = distinct_dataframe.filter($"DepTime" <= 1200)
    x_y_df.printSchema()
    x_y_df.count()

    val y_z_df = distinct_dataframe.filter($"DepTime" >= 1200)
    y_z_df.printSchema()
    y_z_df.count()

    x_y_df.createOrReplaceTempView("x_y_view")
    y_z_df.createOrReplaceTempView("y_z_view")
    val final_df = sqlContext.sql("select *  from (SELECT distinct x_y_view.Origin as x, x_y_view.Dest as y, y_z_view.Dest as z, x_y_view.FlightDate as dt,concat(x_y_view.UniqueCarrier,x_y_view.FlightNum) as x_y_flight,concat(y_z_view.UniqueCarrier,y_z_view.FlightNum) as y_z_flight,(x_y_view.ArrDelay +  y_z_view.ArrDelay )as arr_delay,rank() over ( PARTITION BY x_y_view.Origin,x_y_view.Dest,y_z_view.Dest,x_y_view.FlightDate ORDER BY (x_y_view.ArrDelay +  y_z_view.ArrDelay ) ASC ) AS rank  FROM x_y_view JOIN y_z_view on x_y_view.Dest = y_z_view.Origin WHERE datediff(y_z_view.FlightDate, x_y_view.FlightDate)=2 ) s where rank=1")
    final_df.select("x","y","z","dt","arr_delay","x_y_flight","y_z_flight").write.cassandraFormat("x_y_z", "aviation_online").mode(SaveMode.Append).save()

    }
}
