import org.apache.spark.SparkContext
import org.apache.log4j.{Level,Logger}

object WordCountInOneLine extends App{
  Logger.getLogger("org")setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "FirstDemo")
  val rdd1 = sc.textFile(path=("C:\\Users\\ITC_Dev\\Documents\\warehouse\\wordcount_scala.txt"))
  // val rdd2 = rdd1.flatMap(x => x.split( " ")).map(x=>x.toLowerCase()).map(x=> (x,1)).reduceByKey((x,y) => x+y)
  //If we want to sort by key
  //val rdd2 = rdd1.flatMap(x => x.split( " ")).map(x=>x.toLowerCase()).map(x=> (x,1)).reduceByKey((x,y) => x+y).sortByKey()
  //If we want to sort by values
  //val rdd2 = rdd1.flatMap(x => x.split( " ")).map(x=>x.toLowerCase()).map(x=> (x,1)).reduceByKey((x,y) => x+y).sortBy(_._2)
  //If we want to sort by key value
  //val rdd2 = rdd1.flatMap(x => x.split( " ")).map(x=>x.toLowerCase()).map(x=> (x,1)).reduceByKey((x,y) => x+y)
  //val rdd3 = rdd2.map(x => (x._2,x._1)).sortByKey()

  //collect() return value is RDD
  // rdd3.collect().foreach(println)
  //rdd2.collect().foreach(println)
  //countByKey-The return value is a Map
  val rdd2 = rdd1.flatMap(x => x.split( " ")).map(x=>x.toLowerCase()).map(x=> (x,1)).reduceByKey((x,y) => x+y).countByKey()
  println(rdd2)
}