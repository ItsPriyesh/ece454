import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 2")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(args(0))

    // modify this code
    val count = lines
      .flatMap(_.split(",", -1).tail)
      .filter(!_.isEmpty)
      .count()

    // write count to a single file
    sc.makeRDD(Array(count))
      .coalesce(1)
      .saveAsTextFile(args(1))
  }
}
