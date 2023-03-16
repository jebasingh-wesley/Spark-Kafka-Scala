import configurations.{Base, SparkAppConfig}
import org.apache.hadoop.shaded.com.squareup.okhttp.Route
import org.apache.spark.sql.catalyst.dsl.expressions.DslSymbol
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions.{from_unixtime, unix_timestamp, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import sun.util.calendar.CalendarDate
object inceptez_logic extends App with SparkAppConfig with Base{

//  val dfdt=dfcsvdiffdtfmt.withColumn("dt",from_unixtime(unix_timestamp(col("dt"),"MM-dd-yyyy"),"yyyy-MM-dd"))
//  // string(MM-dd-yyyy) ->  unix_timestamp -> 1634841000 ebsidic value -> from_unixtime -> "yyyy-MM-dd"
//  dfdt.show()

  // To connect with jdbc in REPL
  // spark-shell --jars /home/hduser/install/mysql-connector-java.jar --conf spark.sql.hive.metastore.version=2.3.0 --conf spark.sql.hive.metastore.jars=/usr/local/hive/lib/*
//https://sparkbyexamples.com/spark/spark-with-sql-server-read-and-write-table/


//  route_id, service_id, trip_id, trip_headsign, direction_id, shape_id, wheelchair_accessible, note_fr, note_en
//  2, 21 J -GLOBAUX - 04 - S, 21 J -GLOBAUX - 04 - S_2_0_227923930, STATION HENRI -BOURASSA, 0, 20239, 1,,

  val tripstDF = spark.read.option("header", "true").option("inferschema", "true").option("sep", ",").csv("/home/ubuntu/workspace/data/trips.txt")
//  tripstDF.printSchema()
//  tripstDF.show(10)
  tripstDF.createOrReplaceTempView("tblTrip")
  //tripstDF.registerTempTable("tblTrip")


  val routesDF = spark.read.option("header", "true").option("inferschema", "true").option("sep", ",").csv("/home/ubuntu/workspace/data/routes.txt")
//  routesDF.printSchema()
//  routesDF.show(10)
  routesDF.createOrReplaceTempView("tblRoute")

  val calendarDatesDF = spark.read.option("header", "true").option("inferschema", "true").option("sep", ",").csv("/home/ubuntu/workspace/data/calendar_dates.txt")
//  calendarDatesDF.printSchema()
//  calendarDatesDF.show(10)
  calendarDatesDF.createOrReplaceTempView("tblCalDate")

//  val joinedTable = tripstDF.join(routesDF, Seq("route_id")).show(10)
//  tblTrip
//  tblRoute
//  tblCalDate
val enrichedTripsDF= spark.sql("""SELECT tblTrip.route_id
                                          , tblTrip.service_id
                                          , tblRoute.route_short_name
                                            FROM tblTrip
                                            FULL JOIN tblRoute
                                            ON tblTrip.service_id = tblRoute.route_short_name;""")
  val enrich1 = spark.sql(
    """SELECT
                    |
                    |tblTrip.trip_id,
                    |tblTrip.service_id,
                    |tblTrip.route_id,
                    |tblTrip.trip_headsign,
                    |tblTrip.wheelchair_accessible,
                    |tblCalDate.date,
                    |tblCalDate.exception_type,
                    |tblRoute.route_long_name,
                    |tblRoute.route_color
                    |FROM tblTrip
                    |LEFT JOIN tblRoute
                    |ON tblTrip.route_id = tblRoute.route_id
                    |LEFT JOIN tblCalDate
                    |ON tblTrip.service_id = tblCalDate.service_id""".stripMargin)

//  val demo = spark.sql("""SELECT * FROM tblTrip;""").show(10)

    println("----------------------SQL INNER JOIN Example-------------------------------------------")

  var demo1 =spark.sql("""SELECT Trip.route_id, route.route_id FROM tblTrip Trip INNER JOIN tblRoute route ON Trip.route_id = route.route_id;""")

  println("----------------------SQL LEFT JOIN Syntax Example---------------------------------------")

  var demo2 = spark.sql("""SELECT trip.route_id,route.route_id FROM tblTrip trip LEFT JOIN tblRoute route ON trip.route_id = route.route_id;""")







}
