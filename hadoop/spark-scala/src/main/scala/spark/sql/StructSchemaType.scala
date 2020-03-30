package spark.sql

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object StructSchemaType {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("Name").master("local[2]").getOrCreate()

    val sc = spark.sparkContext

    val rdd = sc.textFile("./doc/person.txt").map(_.split(","))

    val personRDD = rdd.map(x => Row(x(0), x(1), x(2).toInt))

    val schema = StructType(
      StructField("id", StringType, nullable = true) ::
        StructField("name", StringType) ::
        StructField("age", IntegerType) :: Nil
    )

    val dateFrame: DataFrame = spark.createDataFrame(personRDD, schema)

    dateFrame.show()

  }
}
