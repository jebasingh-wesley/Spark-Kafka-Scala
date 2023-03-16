import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger


object FileStreamDemo extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("File Streaming Demo")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.streaming.schemaInference", "true")
      .getOrCreate()

    val rawDF = spark.readStream
      .format("csv")
      .option("path", "/home/ubuntu/workspace/data/airports.csv")
      .option("maxFilesPerTrigger", 1)
      .load()

    val invoiceWriterQuery = rawDF.writeStream
      .format("csv")
      .queryName("Flattened Invoice Writer")
      .outputMode("append")
      .option("path", "/home/ubuntu/workspace/data")
      .option("checkpointLocation", "/home/ubuntu/workspace/data/checkpoint/")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    logger.info("Flattened Invoice Writer started")
    invoiceWriterQuery.awaitTermination()
  }
}