import org.apache.spark.SparkContext
import org.apache.log4j.{Level,Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.SaveMode
object batch extends App{
  Logger.getLogger("org") setLevel (Level.ERROR)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "DFDemo")
    sparkconf.set("spark.master", "local[2]")

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
  //reading table and converting into dataframe
    val orderDF = spark.read.option("header", true)
      .csv("C:\\Users\\ITC_Dev\\Documents\\warehouse\\products.csv")
  //orderDF.printSchema() //to check the datatype
  //Transformations
  val orderDFRes = orderDF.select("product_number", "product_category","length") //narrow transformation
  .where("length>2000")  //wide transformation-shuffling involved
   orderDFRes.show()
    //.count() //transformation//reduceByKey


}
