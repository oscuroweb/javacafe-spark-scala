package oscuroweb.javacafe.sparkdemo

import org.apache.spark.{SparkConf, SparkContext}

object D01_WordCount {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkWordCount")
    val spark = new SparkContext(sparkConf)

    val lines = spark.textFile("spark-warehouse/01_word_count_in.txt")

    val wordCountPair = lines
      .flatMap(line => line.split(" ").iterator)
      .map(word => (word, 1))
      .reduceByKey((value1, value2) => value1 + value2)
      .map(tuple => (tuple.swap))
      .sortByKey(ascending = false)

    wordCountPair.foreach(tuple => println(tuple.toString()))

    // wordCountPair.saveAsTextFile(path = "spark-warehouse/02_word_count_out_scala")

    spark.stop()
  }
}
