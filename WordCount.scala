
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "WordCount")

    val data = sc.textFile("data/book.txt")
    val words = data.flatMap(x => x.split("\\W+"))
    val lowerCase = words.map(x=> x.toLowerCase())
    //counts the occurrences of each word . First way:
    //val countByWord = lowerCase.countByValue()
    //countByWord.foreach(println)

    val wordCounts = lowerCase.map(x=>(x,1)).reduceByKey((x,y) => x+y)

    //To be able to sort by key we need to swap places
    val wordCountsSorted=wordCounts.map(x=> (x._2,x._1)).sortByKey()
    for(results<-wordCountsSorted){
      val word = results._2
      val count= results._1

      println(s"$word:$count")
    }
  }
}
