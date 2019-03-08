
package org.task1.spark

import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._


object Tasks {
  def main(args: Array[String]) {
    val filePath = "hdfs:///root/airline_on_time_performance_cleaned_data" // Should be some file on your system
    val sparkSession = SparkSession.builder.appName("Tasks Application").getOrCreate()
    val sqlContext = sparkSession.sqlContext
    val df = sqlContext.read.format("csv").option("header", true).load(filePath).cache()
    import sqlContext.implicits._

    //     Group 1 queries
    //     1.2 Rank the top 10 airlines by on-time arrival performance.

    val result_ques_1_2 = df.select("AirlineID", "ArrDelayMinutes").filter($"ArrDelayMinutes" === "0.00").groupBy("AirlineId").agg(count("ArrDelayMinutes"))
    result_ques_1_2.sort("count(ArrDelayMinutes)").orderBy(desc("count(ArrDelayMinutes)")).show(10)


    //    # Rank the days of the week by on-time arrival performance
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

      sf.toDF("airport", "top10ontimedest").write.cassandraFormat("otdestperf2", "aviation_online").mode(SaveMode.Append).save()
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
    }
  }
}
