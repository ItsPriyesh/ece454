import org.apache.spark.{SparkConf, SparkContext}

// please don't change the object name
object Task4 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

    val local = sc
      .textFile(args(0))
      .map(line => {
        val tokens = line.split(",", -1)
        (tokens.head, tokens.tail)
      })

    val broadcast = sc.broadcast(local.collect())

    def countSimilarities(sim: Int, users: (Array[String], Array[String])): Int =
      users.zipped.foldLeft(0) { case (acc, (a, b)) => if (!a.isEmpty && !b.isEmpty && a == b) acc + 1 else acc }

    val output = local
      .flatMap { case (titleA, usersA) =>
        broadcast.value.map { case (titleB, usersB) => (titleA, usersA, titleB, usersB) }
      }
      .filter { case (titleA, _, titleB, _) => titleA < titleB }
      .map { case (titleA, usersA, titleB, usersB) => s"$titleA,$titleB" -> (usersA, usersB) }
      .aggregateByKey(0)(countSimilarities, _ + _) // TODO: check if this is faster than nested aggregation within the previous map step
      .map { case (k, v) => s"$k,$v" }

    output.saveAsTextFile(args(1))
  }
}
