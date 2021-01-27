import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object CustomerOrders {

  def parseLine(line: String):(String,Float)={
    val fields = line.split(',')
    val age = fields(0)
    val amount = fields(2).toFloat

    (age, amount)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "WordCount")

    val rdd = sc.textFile("data/customer-orders.csv")
    val tuples = rdd.map(parseLine) //key,value => customer,amount

    val reducedAmounts = tuples.reduceByKey(_+_)

    reducedAmounts.collect().sorted.foreach(println)
  }
}
