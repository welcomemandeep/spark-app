package org.task2.stream

import com.datastax.spark.connector.cql.CassandraConnectorConf
import kafka.serializer.{Decoder, StringDecoder}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.reflect.ClassTag;
//import org.apache.spark.streaming.
import org.apache.spark.streaming.{Seconds, StreamingContext}



object StreamingTasks {
  def main(args: Array[String]) {
    val filePath = "file:///home/ikjotkaur/On_Time_On_Time_Performance_1988_1.csv" // Should be some file on your system
    val sc = new SparkContext();
    val sparkSession = SparkSession.builder.appName("Tasks Application").getOrCreate()
    val sqlContext = sparkSession.sqlContext
//    val df = sqlContext.read.format("csv").option("header", true).load(filePath).cache()
    import sqlContext.implicits._
    val topic =  Map("cleansed-data"->1)
    //val conf = new SparkConf().setAppName("Simple Streaming Application")
    val ssc = new StreamingContext(sc, Seconds(1))
    val topicsSet = "cleansed-data".split(",").toSet

    val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")




      val directKafkaStream  = (1 to 2) map { _ =>
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, topicsSet).map(_._2)
    }

    val unifiedStream = ssc.union(directKafkaStream) // Merge the "per-partition" DStreams
    val sparkProcessingParallelism = 1 // You'd probably pick a higher value than 1 in production.
    // Repartition distributes the received batches of data across specified number of machines in the cluster
    // before further processing.  Essentially, what we are doing here is to decouple processing parallelism from
    // reading parallelism (limited by #partitions).
    unifiedStream.repartition(sparkProcessingParallelism)
    unifiedStream.print()

    //jsonDf.show(10, false)

    val struct = new StructType()
      .add("FlightDate", DataTypes.StringType)
      .add("Carrier", DataTypes.StringType)
      .add("Flights", DataTypes.StringType)
      .add("Origin", DataTypes.StringType)
      .add("Dest", DataTypes.StringType)
      .add("AirlineId", DataTypes.StringType)
      .add("DepTime", DataTypes.StringType)
      .add("DepDelayMinutes", DataTypes.StringType)
      .add("ArrTime", DataTypes.StringType)
      .add("ArrDelayMinutes", DataTypes.StringType)
      .add("DayOfWeek", DataTypes.StringType)
      .add("FlightNum", DataTypes.StringType)
      .add("UniqueCarrier", DataTypes.StringType)
      .add("ArrDelay", DataTypes.StringType)

   /* val nestedDf = jsonDf.select(from_json($"value", struct).as("data"))
    val df = nestedDf.selectExpr("data.FlightDate","data.Carrier","data.Flights","data.Origin","data.Dest","data.AirlineId","data.DepTime","data.DepDelayMinutes","data.ArrTime","data.ArrDelayMinutes","data.DayOfWeek","data.FlightNum","data.UniqueCarrier","data.ArrDelay")

    //     Group 1 queries
    //     1.2 Rank the top 10 airlines by on-time arrival performance.

    val result_ques_1_2 = df.select("AirlineID", "ArrDelayMinutes").groupBy("AirlineId").agg(count("ArrDelayMinutes"))
   // val result_ques_1_2 = df.select("AirlineID", "ArrDelayMinutes").filter($"ArrDelayMinutes" === "0.00").groupBy("AirlineId").agg(count("ArrDelayMinutes"))
    result_ques_1_2.sort("count(ArrDelayMinutes)").orderBy(desc("count(ArrDelayMinutes)")).show(10)

     System.exit(1)
    //   Rank the days of the week by on-time arrival performance
    val result_ques_1_3 = df.select("DayOfWeek", "ArrDelayMinutes").filter($"ArrDelayMinutes" === "0.00").groupBy("DayOfWeek").agg(count("ArrDelayMinutes"))
    result_ques_1_3.sort("count(ArrDelayMinutes)").orderBy(desc("count(ArrDelayMinutes)")).show(10)


    val conf = new SparkConf(true)
    sparkSession.setCassandraConf(CassandraConnectorConf.KeepAliveMillisParam.option(10000))


    //    For each airport X, rank the top-10 carriers in decreasing order of on-time departure performance from X
    val result_ques_2_1 = df.select("Origin", "Carrier", "DepDelayMinutes").filter($"DepDelayMinutes" === "0.00").groupBy("Origin", "Carrier").agg(count("DepDelayMinutes"))
    val airports = result_ques_2_1.select("Origin").distinct().collect().map(_ (0).toString).toList
    airports.foreach { x =>
      val list = result_ques_2_1.filter($"Origin" === x).
        orderBy(desc("count(DepDelayMinutes)")).limit(10).
        withColumnRenamed("count(DepDelayMinutes)", "Count_Dept_Ontime").
        select("Carrier").map(r => r.getString(0)).collect.toList

      val sf = List((x, list))
      sf.toDF("airport", "top10carriers").write.cassandraFormat("otdperf", "aviation_online").mode(SaveMode.Append).save()
    }

    val result_ques_2_2 = df.select("Origin", "Dest", "DepDelayMinutes").filter($"DepDelayMinutes" === "0.00").groupBy("Origin", "Dest").agg(count("DepDelayMinutes"))
    val origin = result_ques_2_2.select("Origin").distinct().collect().map(_ (0).toString).toList
    origin.foreach { x =>
      val list = result_ques_2_2.filter($"Origin" === x).
        orderBy(desc("count(DepDelayMinutes)")).limit(10).
        withColumnRenamed("count(DepDelayMinutes)", "Count_Dept_Ontime").
        select("Dest").map(r => r.getString(0)).collect.toList
      val sf = List((x, list))

      sf.toDF("airport", "top10ontimedest").write.cassandraFormat("otdestperf", "aviation_online").mode(SaveMode.Append).save()
    }

    //For each source-destination pair X-Y, rank the top-10 carriers in decreasing order of on-time arrival performance at Y from X.

    val result_ques_2_3 = df.select("Origin", "Dest", "Carrier", "ArrDelayMinutes").
      filter($"ArrDelayMinutes" === "0.00").
      groupBy("Origin", "Dest", "Carrier").
      agg(count("ArrDelayMinutes"))


    val origin_dest = result_ques_2_3.select("Origin", "Dest").
      distinct().collect().map(row =>
      (row(0).toString, row(1).toString)).toList

    origin_dest.foreach {
      case (x, y) =>
        val list = result_ques_2_3.filter($"Origin" === x && $"Dest" === y).
          orderBy(desc("count(ArrDelayMinutes)")).limit(10).
          withColumnRenamed("count(ArrDelayMinutes)", "count_arr_ontime").
          select("Carrier").map(r => r.getString(0)).collect.toList

        val sf = List((x, y, list))
        sf.toDF("origin", "dest", "carriers")

        sf.toDF("origin", "dest", "carriers").write.
          cassandraFormat("source_destination_airline2", "aviation_online").mode(SaveMode.Append).save()
    }*/
  }
}