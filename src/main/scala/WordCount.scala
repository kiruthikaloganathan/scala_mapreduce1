
import org.apache.spark.SparkContext
import org.apache.log4j.{Level,Logger}

object WordCount extends App{
  Logger.getLogger("org") setLevel (Level.ERROR)

  val sc = new SparkContext("local[*]", "FirstDemo")
  //readinputfileandreturnRDD(Resilientdistributeddataset)
  val rdd1 = sc.textFile(path="C:\\Users\\ITC_Dev\\Documents\\warehouse\\wordcount_scala.txt")
  rdd1.collect.foreach(println)

  //oneinputmultipleoutput--givesyourwords
  val rdd2 = rdd1.flatMap(x => x.split(""))
  rdd2.collect.foreach(println)

  //returnkey-valuepairwiththewordsreceived
  val rdd3 = rdd2.map(x => (x, 1))
  rdd3.collect.foreach(println)

  //do sum for same keys
  val rdd4 = rdd3.reduceByKey((x, y) => x + y)

  rdd4.collect().foreach(println) // Collect is a method of RDD and only with that  if a  return value is rdd we can print

}