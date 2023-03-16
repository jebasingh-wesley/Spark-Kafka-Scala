import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.types._
object spark_depth_example extends App {

  // turn off logger messages in INFO level
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)


//  val conf = new SparkConf().setAppName("Banking_Domain").setMaster("local[*]")
//  val sc = new SparkContext(conf)
//  val sqlc = new SQLContext(sc)
  //val hqlc = new org.apache.spark.sql.hive.HiveContext(sc)
  //sc.setLogLevel("ERROR");
  val spark = SparkSession.builder.appName("spark_depth_example").master("local[*]").getOrCreate



  //read csv with options
  //  val df = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true"))
  //    .csv("/home/ubuntu/data_engin/spark-scala-examples/src/main/resources/zipcodes.csv")

  println("--------------------------------------------------------------------cache-----------------------------------------------------------------------------------------")
  //it is used for store data in memory for random use offen we dont want to get from scratch
  //  val df2 = df.where(col("State") === "PR").cache()
  //  df2.show(false)
  //  println(df2.count())
  //  val df3 = df2.where(col("Zipcode") === 704)
  //  println(df2.count())
  println("--------------------------------------------------------------------Cast_String_To_Int------------------------------------------------------------------------------")
  //  change the string to int in a dataframe are any rdd,DS
  //  https://github.com/spark-examples/spark-scala-examples/blob/master/src/main/scala/com/sparkbyexamples/spark/dataframe/examples/CastStringToInt.scala
  //  it is used for sql change string to double multiple use

  println("--------------------------------------------------------------------Collect_Example_scala_List_getstring----------------------------------------------------------")
  //  https://github.com/spark-examples/spark-scala-examples/blob/master/src/main/scala/com/sparkbyexamples/spark/dataframe/examples/CollectExample.scala
  //  index start from Zero convert row into list for structer

  println("--------------------------------------------------------------------Data_Frame_Complex_scala_MAP_ARRAY_ROW----------------------------------------------------------")
  //  https://github.com/spark-examples/spark-scala-examples/blob/master/src/main/scala/com/sparkbyexamples/spark/dataframe/examples/DataFrameComplex.scala

  println("--------------------------------------------------------------------Data_Frame_DropColumn_scala---------------------------------------------------------------------")
  //  https://github.com/spark-examples/spark-scala-examples/blob/master/src/main/scala/com/sparkbyexamples/spark/dataframe/examples/DropColumn.scala

  println("--------------------------------------------------------------------Rename_DeleteFile_scala_---------------------------------------------------------------------")
  //  https://github.com/spark-examples/spark-scala-examples/blob/master/src/main/scala/com/sparkbyexamples/spark/dataframe/examples/RenameDeleteFile.scala
  // Rename a File  //Alternatively, you can also create Hadoop configuration  //Delete a File | delete from hadoop or rename from hadoop

  println("--------------------------------------------------------------------Save_Single_File_hadoop_from_local---------------------------------------------------------------------")
  //https://github.com/spark-examples/spark-scala-examples/blob/master/src/main/scala/com/sparkbyexamples/spark/dataframe/examples/SaveSingleFile.scala


  println("--------------------------------------------------------------------SelectExamples---------------------------------------------------------------------")
 // Show columns from start and end index |   //Show columns by index or position | //Show columns by regular expression   //Show columns from start and end index

  println("--------------------------------------------------------------------array_contains,ArrayContainsExample---------------------------------------------------------------------")
  //https://github.com/spark-examples/spark-scala-examples/blob/master/src/main/scala/com/sparkbyexamples/spark/dataframe/functions/collection/ArrayContainsExample.scala

  println("--------------------------------------------------------------------ArrayOfString.scala,concat_ws,array to string------------------------------------------------------------")
//  https://github.com/spark-examples/spark-scala-examples/blob/master/src/main/scala/com/sparkbyexamples/spark/dataframe/functions/collection/ArrayOfString.scala

  println("--------------------------------------------------------------------Current_DateAnd_Time_scala------------------------------------------------------------")

// https://github.com/spark-examples/spark-scala-examples/blob/master/src/main/scala/com/sparkbyexamples/spark/dataframe/functions/datetime/CurrentDateAndTime.scala

//  https://github.com/spark-examples/spark-scala-examples/blob/master/src/main/scala/com/sparkbyexamples/spark/dataframe/functions/datetime/DateAddMonths.scala

  println("--------------------------------------------------------------------Concat_Example_scala------------------------------------------------------------")
//  https://github.com/spark-examples/spark-scala-examples/blob/master/src/main/scala/com/sparkbyexamples/spark/dataframe/functions/string/ConcatExample.scala
// join thee colume into single colume by

  println("--------------------------------------------------------------------Split_Example_scala------------------------------------------------------------")
  //https://github.com/spark-examples/spark-scala-examples/blob/master/src/main/scala/com/sparkbyexamples/spark/dataframe/functions/string/SplitExample.scala
  // split a array into colume in a table |



}