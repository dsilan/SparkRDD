import org.apache.log4j.{Level, Logger}
import org.apache.spark._

object FriendsByAge{
  //csv data->age,numFriends tuples
  def parseLine(line: String):(Int,Int)={
    val fields = line.split(',')
    val age = fields(2).toInt
    val numFriends = fields(3).toInt

    (age, numFriends)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "HelloWorld")

    val data = sc.textFile("data/fakefriends-noheader.csv")
    val rdd = data.map(parseLine)

    //how many people exist for that age
    val totalsByAge = rdd.mapValues(x => (x,1)).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2 ))
    val avgByAge = totalsByAge.mapValues(x => x._1/x._2)

    val results = avgByAge.collect()

    results.sorted.foreach(println)

    sc.stop()
  }
}
