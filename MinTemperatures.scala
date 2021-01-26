import org.apache.spark._
import org.apache.log4j._

object MinTemperatures {

  def parseLine(line:String):(String, String, Float) ={
    val fields = line.split(',')
    val temperature = fields(3).toFloat
    val stationID= fields(0)
    val entryType= fields(2) //max or min

    (stationID, entryType, temperature)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "MinTemperatures")

    val data = sc.textFile("data/1800.csv")
    val rdd = data.map(parseLine)

    val minTemperatures = rdd.filter(x => x._2 == "TMIN")
    //all min temparatatures collected no need entry type anymore
    val stationTemps = minTemperatures.map(x => (x._1,x._3))
    val results = stationTemps.collect()
    results.sorted.foreach(println)

    sc.stop()
  }
}
