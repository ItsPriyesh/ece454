import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task4 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

      //    val local = sc.textFile("/Users/priyesh/Desktop/4A/ECE_454/assignments/a2/sample_input/smalldata.txt")
    val local = sc.textFile(args(0))
      .map(line => {
        val tokens = line.split(",", -1)
        (tokens.head, tokens.tail)
      })

    val broadcast = sc.broadcast(local)

    def countSimilarities(sim: Int, users: (Array[String], Array[String])): Int =
      users.zipped.foldLeft(0) { case (acc, (a, b)) => if (!a.isEmpty && !b.isEmpty && a == b) acc + 1 else acc }

    val output = local
      .cartesian(broadcast.value)
      .filter { case ((titleA, _), (titleB, _)) => titleA < titleB }
      .map { case ((titleA, usersA), (titleB, usersB)) => s"$titleA,$titleB" -> (usersA, usersB) }
      .aggregateByKey(0)(countSimilarities, _ + _)

    output.saveAsTextFile(args(1))
  }
}
