package oscuroweb.javacafe.sparkdemo

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}

object D03_SparkStreamingWindow {

  def main(args: Array[String]): Unit = {

    val conf =  new SparkConf().setAppName("SparkStreaming WordCount").setMaster("local[2]")
    val streamingContext = new StreamingContext(conf, Durations.seconds(2))

    val lines = streamingContext.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    val wordCount = lines.flatMap(_.split(" "))
      .map(word => (word, 1))
      .reduceByKeyAndWindow((value1:Int, value2:Int) => (value1 + value2),
        Seconds(10),
        Seconds(4))

    wordCount.print()

    streamingContext.start()
    streamingContext.awaitTermination()

    streamingContext.stop()

  }

}
