import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task3 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 3")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    def parseText(line: String): Array[(Int, Int)] = {
      line.split(",", -1)
        .zipWithIndex
        .drop(1)
        .filter(!_._1.isEmpty)
        .map { case(_, idx) => (idx, 1) }
    }

    val countRatings = textFile
      .flatMap(parseText)
      .reduceByKey(_ + _)
      .map(v => v._1 + "," + v._2)

    countRatings.saveAsTextFile(args(1))
  }
}

