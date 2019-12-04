import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._

object WordCount {
  def main (args: Array[String]): Unit = {
    if (args.length != 2) {
      throw new IllegalArgumentException("At exactly 2 arguments are required: <inputPath> <outputPath>")
    }
    val inputPath = args(0)
    val outputPath = args(1)
    val sc = new SparkContext(new SparkConf().setAppName("WordCount"))

    val lines = sc.textFile(inputPath)
    val output = lines.flatMap(l => l.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    output.saveAsTextFile(outputPath)

  }
}
