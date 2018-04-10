package oscuroweb.javacafe.sparkdemo

import org.apache.spark.streaming.{Duration, Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object D02_SparkStreaming {

  def main(args: Array[String]): Unit = {
    val conf =  new SparkConf().setAppName("SparkStreaming WordCount").setMaster("local[2]")
    val streamingContext = new StreamingContext(conf, Durations.seconds(1))

    val lines  = streamingContext.socketTextStream("localhost", 9999)

    val wordCount = lines.flatMap(_.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    wordCount.print()

    streamingContext.start()
    streamingContext.awaitTermination()

    streamingContext.stop()
  }
}
