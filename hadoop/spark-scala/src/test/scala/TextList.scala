import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Rand
import org.spark_project.jetty.util.thread.Scheduler.Task

object TextList {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[1]").getOrCreate()
    //需要手动导入隐式转换
    import spark.implicits._
    List(1, 2, 3).toDF().show()


    List(scala.collection.immutable.Range.inclusive(1, 10)).toDF().show()

    scala.collection.immutable.Range.inclusive(1, 10).toDF().show()

    SparkSession
    SparkContext
  }
}
