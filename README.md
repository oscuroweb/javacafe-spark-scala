# Spark Demos in Scala

### Simple Apache Spark Examples in Scala.

* [D01_WordCount](https://github.com/oscuroweb/javacafe-spark-scala/blob/master/src/main/scala/oscuroweb/javacafe/sparkdemo/D01_WordCount.scala): Words count example
* [D02_SparkStreaming](https://github.com/oscuroweb/javacafe-spark-scala/blob/master/src/main/scala/oscuroweb/javacafe/sparkdemo/D02_SparkStreaming.scala): Words count with Spark Streaming 
* [D03_SparkStreamingWindow](https://github.com/oscuroweb/javacafe-spark-scala/blob/master/src/main/scala/oscuroweb/javacafe/sparkdemo/D03_SparkStreamingWindow.scala): Words count with Spark Streaming using windows
* [D04_SparkSQL](https://github.com/oscuroweb/javacafe-spark-scala/blob/master/src/main/scala/oscuroweb/javacafe/sparkdemo/D04_SparkSQL.scala): Spark SQL example

### Requeriments
* [Apache Spark 2.3.0](https://www.apache.org/dyn/closer.lua/spark/spark-2.3.0/spark-2.3.0-bin-hadoop2.7.tgz)
* [Scala 2.11.12](https://www.scala-lang.org/download/2.11.12.html)
* [sbt 1.1.4](https://www.scala-sbt.org/download.html)
* [Java SDK 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)

### How to run

1. Package project
```
$ sbt package
```

2. Execute with spark-submit
```
$ spark-submit --class "oscuroweb.javacafe.sparkdemo.[CLASS_NAME]" --master local[*] target/scala-2.11/javacafe-spark-scala_2.11-0.1.jar [INPUT_PARAMETER]
```
Where [CLASS_NAME] is the name of the class to execute and [INPUT_PARAMETER] are the arguments.

Also, we can use included scripts
* run-d01.bat or run-d01.sh
* run-d02.bat or run-d02.sh
* run-d03.bat or run-d03.sh
* run-d04.bat or run-d04.sh
