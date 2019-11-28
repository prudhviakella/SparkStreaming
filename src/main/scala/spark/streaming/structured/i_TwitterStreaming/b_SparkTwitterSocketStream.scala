package spark.streaming.structured.i_TwitterStreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, split}

/*
* Author : Prudhvi Akella.
* Description : This Application connects to the socket that is exposed by TwitterStreaming application and
* process the twitter data that is captured by twitter4j streaming library
*/
object b_SparkTwitterSocketStream extends App {
  if(args.length != 2) {
    println("Either host name or port is missing")
    System.exit(1)
  }
  val host:String=args(0).toString;
  val port:String=args(1).toString;
  println(host+" "+port)
  System.setProperty("hadoop.home.dir", "D:\\spark")
  //Creating spark session object to create socket stream
  val spark = SparkSession.builder()
    .appName("SparkTwitterSocketStreaming")
    .master("local")
    .getOrCreate();
  //Creating socket stream
  val lines = spark.readStream
    .format("socket")
    .option("host",host)
    .option("port",port)
    .option("includeTimestamp",true)
    .load()
  //
  import spark.implicits._
  val words = lines.select(explode(split($"value"," ")).as("word"),$"timestamp")

  val query = words.writeStream
    .format("console")
    .outputMode("append")
    .start()

  query.awaitTermination()
}
