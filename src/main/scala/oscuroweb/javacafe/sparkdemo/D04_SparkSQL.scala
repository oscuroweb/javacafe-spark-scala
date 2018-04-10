package oscuroweb.javacafe.sparkdemo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.{SparkConf, SparkContext}

object D04_SparkSQL {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .appName("Spark SQL Demo")
      .config("master", "local[2]")
      .getOrCreate()

    // Get dataset from csv file
    val dataset = spark
      .read
      .option("ignoreLeadingWhiteSpace", true)
      .csv(args(0))

    dataset.show()

    // Rename dataset columns
    val renamedDataset = dataset.withColumnRenamed("_C0", "age")
        .withColumnRenamed("_C1", "workclass")
        .withColumnRenamed("_C2", "fnlwgt")
        .withColumnRenamed("_C3", "education")
        .withColumnRenamed("_C4", "education-num")
        .withColumnRenamed("_C5", "marital-status")
        .withColumnRenamed("_C6", "occupation")
        .withColumnRenamed("_C7", "relationship")
        .withColumnRenamed("_C8", "race")
        .withColumnRenamed("_C9", "sex")
        .withColumnRenamed("_C10", "capital-gain")
        .withColumnRenamed("_C11", "capital-loss")
        .withColumnRenamed("_C12", "hours-per-week")
        .withColumnRenamed("_C13", "native-country")
        .withColumnRenamed("_C14", "income")

    dataset.show()

    // Binarize income column.
    // 1 => >50K
    // 0 => <=50K
    val binarizeIncomeDataset = renamedDataset
      .withColumn("income_bin",
        renamedDataset.col("income").equalTo(">50K").cast(DataTypes.IntegerType))

    binarizeIncomeDataset.show()

    // Average age for each income value
    val binarizeIncomeDatasetCast = binarizeIncomeDataset
      .withColumn("age",
        binarizeIncomeDataset.col("age").cast(DataTypes.IntegerType))

    val avgAgeDataset = binarizeIncomeDatasetCast
      .groupBy("income_bin")
      .avg("age")

    avgAgeDataset.show()

    // Average age for each income value SQL
    binarizeIncomeDatasetCast.createOrReplaceTempView("dataset")

    val avgAgeSqlDataset = spark.sql("SELECT income_bin, AVG(age) " +
      "FROM dataset " +
      "GROUP BY income_bin")

    avgAgeSqlDataset.show()

    spark.close()



  }
}
