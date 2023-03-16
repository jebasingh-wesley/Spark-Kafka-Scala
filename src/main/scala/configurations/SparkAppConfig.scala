package configurations

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

trait SparkAppConfig {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Spark Streaming Project")
    .master("local[*]")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  val ssc: StreamingContext = new StreamingContext(sc, Seconds(10))
}