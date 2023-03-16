import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.apache.log4j.{Level, Logger}
import org.apache.hadoop.fs.{FileSystem,Path}


object Datamozart extends App {
  // turn off logger messages in INFO level
  // and only show messages in WARN level
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Spark Word Count")
    .getOrCreate()

//  val myFile = spark.read.parquet("hdfs://192.168.0.50:54310/usr/paxel_dw/prod/shipments")
//  myFile.printSchema()
//  val numPartitions = 20
//  val partitionedData = myFile.repartition(numPartitions)
//  val path = "/home/ubuntu/Datamozart/datamozart_backup/shipments"
//  partitionedData.write.option("header", "true").option("delimiter", ",").csv(path)


//  val myFile = spark.read.option("header", "true").option("inferschema", "true").option("sep", ",").csv("/home/ubuntu/Datamozart/datamozart_backup/Datamozart_Data/shipments")
//  myFile.printSchema()
//  val numPartitions = 100
//  val partitionedData = myFile.repartition(numPartitions)
//  val path = "/home/ubuntu/Datamozart/datamozart_backup/shipments"
//  partitionedData.write.option("header", "true").option("inferschema", "true").option("delimiter", ",").csv(path)











}



//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.{FileSystem, Path}
//val hadoopConf = new Configuration()
//val fs = FileSystem.get(hadoopConf)
//val path = new Path("hdfs://192.168.0.50:54310/usr/paxel_dw/prod/shipment_logs/")
//val directorySize = fs.getContentSummary(path).getLength
//println(s"The size of directory $path is $directorySize bytes.")