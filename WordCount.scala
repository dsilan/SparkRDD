
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "WordCount")

    val data = sc.textFile("data/book.txt")
    val words = data.flatMap(x => x.split(' '))
    //counts the occurrences of each word
    val countByWord = words.countByValue()
    countByWord.foreach(println)
  }
}
