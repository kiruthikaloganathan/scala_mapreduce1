import org.apache.spark.SparkContext
import org.apache.log4j.{Level,Logger}

object Main {
  def main(args: Array[String]): Unit = {
    println("Hello world!")

    //if we want to see only the error message and not information about execution
    Logger.getLogger("org")setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "FirstDemo")
    //read input file and return RDD (Resilient distributed dataset)
    val rdd1 = sc.textFile(path=("C:\\Users\\kiruthika\\kiruthika_mapreduce.txt"))

    //one input multiple output--gives your words
    val rdd2 = rdd1.flatMap(x => x.split( " "))

    //return key-value pair with the words received
    val rdd3 = rdd2.map(x =>(x,1))
    rdd3.collect.foreach(println)

    //do sum for same keys
    val rdd4 = rdd3.reduceByKey((x,y) => x+y)

    rdd4.collect.foreach(println)

  }
}

