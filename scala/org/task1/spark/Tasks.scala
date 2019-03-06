
package org.task1.spark

import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._


object Tasks {
  def main(args: Array[String]) {
    val filePath = "hdfs:///root/airline_on_time_performance_cleaned_data" // Should be some file on your system
    //val filePath = "file:///Users/mgandhi/Downloads/23.csv" // Should be some file on your system
    val sparkSession = SparkSession.builder.appName("Tasks Application").getOrCreate()
    val sqlContext = sparkSession.sqlContext
    val df = sqlContext.read.format("csv").option("header", true).load(filePath).cache()
    import sqlContext.implicits._
    //     Group 1 queries
    //     1.2 Rank the top 10 airlines by on-time arrival performance.

    //    val result_ques_1_2 = df.select("AirlineID","ArrDelayMinutes").filter($"ArrDelayMinutes"==="0.00").groupBy("AirlineId").agg(count("ArrDelayMinutes"))
    //    result_ques_1_2.sort("count(ArrDelayMinutes)").orderBy(desc("count(ArrDelayMinutes)")).show(10)


    //    # Rank the days of the week by on-time arrival performance
    //    val result_ques_1_3 = df.select("DayOfWeek","ArrDelayMinutes").filter($"ArrDelayMinutes"==="0.00").groupBy("DayOfWeek").agg(count("ArrDelayMinutes"))
    //    result_ques_1_3.sort("count(ArrDelayMinutes)").orderBy(desc("count(ArrDelayMinutes)")).show(10)


    val conf = new SparkConf(true)
    //      .set("spark.cassandra.connection.host", "54.210.203.189")
    sparkSession.setCassandraConf(CassandraConnectorConf.KeepAliveMillisParam.option(10000))

    // set params for the particular cluster
    //    sparkSession.setCassandraConf("Cluster1", CassandraConnectorConf.ConnectionHostParam.option("172.31.92.159") ++ CassandraConnectorConf.ConnectionPortParam.option(9042))

    //    CassandraConnector(conf).withSessionDo { session =>
    //      session.execute("CREATE KEYSPACE aviation_online WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    //      session.execute("CREATE TABLE aviation_online.otdperf (airport text PRIMARY KEY,  top10carriers list<text>)")
    //    }
    //df.write.cassandraFormat("tasks", "aviation_online").save()

    //    For each airport X, rank the top-10 carriers in decreasing order of on-time departure performance from X
    //    val result_ques_2_1 = df.select("Origin","Carrier","DepDelayMinutes").filter($"DepDelayMinutes"==="0.00").groupBy("Origin","Carrier").agg(count("DepDelayMinutes"))
    //    val airports = result_ques_2_1.select("Origin").distinct().collect().map(_(0).toString).toList
    //    airports.foreach{ x=>
    //      val list = result_ques_2_1.filter($"Origin" === x ).
    //        orderBy(desc("count(DepDelayMinutes)")).limit(10).
    //        withColumnRenamed("count(DepDelayMinutes)","Count_Dept_Ontime").
    //        select("Carrier").map(r => r.getString(0)).collect.toList
    //
    //      val sf = List((x,list))
    //      sf.toDF("airport","top10carriers").write.cassandraFormat("otdperf", "aviation_online").mode(SaveMode.Append).save()
    //    }

    //    val result_ques_2_2 = df.select("Origin","Dest","DepDelayMinutes").filter($"DepDelayMinutes"==="0.00").groupBy("Origin","Dest").agg(count("DepDelayMinutes"))
    //    val origin = result_ques_2_2.select("Origin").distinct().collect().map(_(0).toString).toList
    //    origin.foreach{ x=>
    //      val list = result_ques_2_2.filter($"Origin" === x ).
    //        orderBy(desc("count(DepDelayMinutes)")).limit(10).
    //        withColumnRenamed("count(DepDelayMinutes)","Count_Dept_Ontime").
    //        select("Dest").map(r => r.getString(0)).collect.toList
    //
    //      val sf = List((x,list))
    //
    //      sf.toDF("airport","top10ontimedest").write.cassandraFormat("otdestperf2", "aviation_online").mode(SaveMode.Append).save()
    //    }

    //or each source-destination pair X-Y, rank the top-10 carriers in decreasing order of on-time arrival performance at Y from X.

    val result_ques_2_3 = df.select("Origin", "Dest", "Carrier", "ArrDelayMinutes").
      filter($"ArrDelayMinutes" === "0.00").
      groupBy("Origin", "Dest", "Carrier").
      agg(count("ArrDelayMinutes")).cache()


    //    val origin_dest =  Map("CMI"  -> "ORD", "IND" -> "CMH", "DFW" -> "IAH", "LAX" -> "SFO", "JFK" ->  "LAX", "ATL" ->"PHX")

    //    val origin_dest = result_ques_2_3.select("Origin","Dest").
    //      distinct().collect().map( row =>
    //      (row(0).toString, row(1).toString)).toList
    //
    //    origin_dest.foreach{
    //      case (x,y)=>
    //      val list = result_ques_2_3.filter($"Origin" === x  && $"Dest" === y).
    //        orderBy(desc("count(ArrDelayMinutes)")).limit(10).
    //        withColumnRenamed("count(ArrDelayMinutes)","count_arr_ontime").
    //        select("Carrier").map(r => r.getString(0)).collect.toList
    //
    //        val sf = List((x, y,list))
    //        sf.toDF("origin", "dest", "carriers")
    //
    //        println(origin_dest.toString())
    //
    //        sf.toDF("origin", "dest", "carriers").write.
    //          cassandraFormat("source_destination_airline2", "aviation_online").mode(SaveMode.Append).save()
    //    }


    //  select origin, dest , count(carrier), carrier   origin_dest join  result
    // on o.origin = r.origin and r.est = o.dest
    //    groupby(origin , dest, carrier)


    // Group 3

    //    requirements

    //    a.  Leave from X before 12
    //    b.  Leave from Y after 12
    //    c.  Flight from Y should be 2 days after
    val distinct_dataframe = df.select("Origin", "Dest", "DepTime", "FlightDate", "DepTime","FlightNum" , "UniqueCarrier","ArrDelay").distinct()
    val x_y_df = distinct_dataframe.filter($"DepTime" <= 1200)
//    x_y_df.show(10000)
    val y_z_df = distinct_dataframe.filter($"DepTime" >= 1200)
//    y_z_df.show(10000)
    //x_y_df.createOrReplaceTempView("x_y_view")
    //y_z_df.createOrReplaceTempView("y_z_view")
//    sqlContext.sql("SELECT distinct x_y_view.Origin as X, x_y_view.Dest as Y, y_z_view.Dest as Z, x_y_view.FlightDate as dt,concat(x_y_view.UniqueCarrier,x_y_view.FlightNum) as x_y_flight,concat(y_z_view.UniqueCarrier,y_z_view.FlightNum) as y_z_flight,(x_y_view.ArrDelay +  y_z_view.ArrDelay )as arr_delay,rank() over ( PARTITION BY x_y_view.Origin,x_y_view.Dest,y_z_view.Dest,x_y_view.FlightDate ORDER BY (x_y_view.ArrDelay +  y_z_view.ArrDelay ) ASC ) AS rank  FROM x_y_view JOIN y_z_view on x_y_view.Dest = y_z_view.Origin WHERE datediff(y_z_view.FlightDate, x_y_view.FlightDate)=2").show(20000)    //  CMI → ORD → LAX, 04/03/2008
//    sqlContext.sql("select *  from (SELECT distinct x_y_view.Origin as X, x_y_view.Dest as Y, y_z_view.Dest as Z, x_y_view.FlightDate as dt,concat(x_y_view.UniqueCarrier,x_y_view.FlightNum) as x_y_flight,concat(y_z_view.UniqueCarrier,y_z_view.FlightNum) as y_z_flight,(x_y_view.ArrDelay +  y_z_view.ArrDelay )as arr_delay,rank() over ( PARTITION BY x_y_view.Origin,x_y_view.Dest,y_z_view.Dest,x_y_view.FlightDate ORDER BY (x_y_view.ArrDelay +  y_z_view.ArrDelay ) ASC ) AS rank  FROM x_y_view JOIN y_z_view on x_y_view.Dest = y_z_view.Origin WHERE datediff(y_z_view.FlightDate, x_y_view.FlightDate)=2 ) s where rank=1").
    val final_df = x_y_df.as("a").join(y_z_df.as("b"),$"b.Origin" === $"a.Dest","inner").select($"a.Origin".as("x"),$"a.Dest".as("y"),$"b.Dest".as("z"),date_format($"a.FlightDate","dd/MM/yyyy").as("flight_date"),concat($"a.UniqueCarrier",$"a.FlightNum").as("x_y_flight"),concat($"b.UniqueCarrier",$"b.FlightNum").as("y_z_flight"),($"a.ArrDelay"+$"b.ArrDelay").as("total_arrival_delay")).filter(datediff($"b.FlightDate",$"a.FlightDate")===2)
    val distinct_comb = final_df.select("x","y","z","flight_date").distinct().collect().map(row => (row(0).toString,row(1).toString,row(2).toString, row(3).toString)).toList

//    airports.foreach{ x=>
    //      val list = result_ques_2_1.filter($"Origin" === x ).
    //        orderBy(desc("count(DepDelayMinutes)")).limit(10).
    //        withColumnRenamed("count(DepDelayMinutes)","Count_Dept_Ontime").
    //        select("Carrier").map(r => r.getString(0)).collect.toList
    //
    //      val sf = List((x,list))
    //      sf.toDF("airport","top10carriers").write.cassandraFormat("otdperf", "aviation_online").mode(SaveMode.Append).save()
    //    }
    
 //   sqlContext.sql("select x ,y,z,from_unixtime(unix_timestamp(dt, 'yyyy-MM-dd'), 'dd/MM/yyyy') as flight_date,x_y_flight," +
  //    " y_z_flight,arr_delay as total_arrival_delay from (SELECT distinct x_y_view.Origin as x, " +
   //   "x_y_view.Dest as y, y_z_view.Dest as z, x_y_view.FlightDate as dt,x_y_view.FlightDate," +
    //  "concat(x_y_view.UniqueCarrier,x_y_view.FlightNum) as x_y_flight," +
     // "concat(y_z_view.UniqueCarrier,y_z_view.FlightNum) as y_z_flight," +
     // "(x_y_view.ArrDelay +  y_z_view.ArrDelay )as arr_delay," +
     // "rank() over ( PARTITION BY x_y_view.Origin,x_y_view.Dest,y_z_view.Dest," +
     // "x_y_view.FlightDate ORDER BY (x_y_view.ArrDelay +  y_z_view.ArrDelay ) ASC ) AS rank " +
      //"" +
      //" FROM x_y_view JOIN y_z_view on x_y_view.Dest = y_z_view.Origin " +
      //"WHERE datediff(y_z_view.FlightDate, x_y_view.FlightDate)=2 ) s where rank=1").
      //write.cassandraFormat("x_y_z", "aviation_online").mode(SaveMode.Append).save()
    distinct_comb.foreach{ case (p,q,r,s)=>val list = final_df.filter($"x" === p &&  $"y" === q && $"z" === r && $"flight_date" === s).orderBy("total_arrival_delay").limit(1) ; list.write.cassandraFormat("x_y_z", "aviation_online").mode(SaveMode.Append).save()}

  }
}
