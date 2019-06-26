import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 1")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(args(0))

    // Convert input RDD[String] to RDD[(movie (rating, userIndex))]
    val rows = lines.flatMap(line => {
      val cols = line.split(",", -1)
      val movie = cols.head
      val ratings = cols.tail
      ratings
        .zipWithIndex
        .filter(!_._1.isEmpty)
        .map { case (rating, idx) => movie -> (rating.toInt, List(idx + 1)) }
    });

    // reduces rows to (move, rating, List(userIndex))
    val reduced = rows.reduceByKey((acc, i) =>
      if (i._1 > acc._1) i
      else if (i._1 == acc._1) (acc._1, acc._2 ++ i._2)
      else acc
    )

    // map to desired output (movie, users)
    val output = reduced.map { case (movie, (_, users)) => movie + "," + users.mkString(",") }

    output.saveAsTextFile(args(1))
  }
}
