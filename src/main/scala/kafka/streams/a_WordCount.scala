package kafka.streams


import java.lang
import java.time.Duration
import java.util.Properties

import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.ImplicitConversions._

/*
* Compiler : Scala 2.11.8 with -xExperimental Flag enabled.
* Producer console command : kafka-console-producer.bat --broker-list localhost:9092 --topic word-count-input
* Consumer console command : kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic word-count-output --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
 */
object a_WordCount extends App{
  //Preparing the properties that are required for kafka stream
  //APPLICATION_ID plays an important role streams engine uses it will  like consumer-id,default-clinet-id prefix,
  // prefix-to internal logs.Once stream is created don't change if you change it stream engine consider it at as new
  //stream and it will start from beginning
  val config: Properties = {
    val properties = new Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application")
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")//consumer property
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    properties
  }

  val builder = new StreamsBuilder()
  // 1 - stream from Kafka (Source Processor)
  val textLines: KStream[String, String] = builder.stream[String, String]("word-count-input")
  //
  val wordCounts:KTable[String,Long] = textLines
  // 2 - map values to lowercase
    .mapValues(Value => Value.toLowerCase)
  // 3 - flatmap values split by space
    .flatMapValues(textLine => textLine.toLowerCase.split("\\W+"))
  // 4 - select key to apply a key (As key is null we applying key. As of now key is nothing but value)
    .selectKey((key,value)=>value)
  // 5 - group by key before aggregation
    .groupByKey
  // 6 - count occurences
    .count()
  //word-count-output to kafka topic (Sink Processor)
  wordCounts.toStream.to("word-count-output")
  //builder.build() returns a topology and KafkaStreams creates a stream with Topology and config
  val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
  streams.cleanUp()
  streams.start()
  // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
  sys.ShutdownHookThread {
    streams.close(Duration.ofSeconds(10))
  }
}
