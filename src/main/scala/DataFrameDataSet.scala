//Dataframe and Dataset example
//convert Dataframe to Dataset

import org.apache.log4j.{Level,Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DataFrameDataSet extends App{
  Logger.getLogger("org")setLevel(Level.ERROR)
  val sparkconf = new SparkConf()
  sparkconf.set("spark.app.name", "DFDemo")
  sparkconf.set("spark.master", "local[2]")

  val spark=SparkSession.builder().config(sparkconf).getOrCreate()

  val orderDF = spark.read.option("header", true) //skip header line
    // .option("inferschema", true) //infer or create the schema//
    // .schema(orderSchemaDDL)
    .csv("C:\\Users\\ITC_Dev\\Documents\\warehouse\\orders_buvana.csv")

  //schema to convert DF to DS-Need to provide in a case class format

  case class ordersData(order_id:String,order_date:String,order_customer_id:String,order_Status:String)
  import spark.implicits._
  val orderDS=orderDF.as[ordersData]
  orderDS.filter("order_id<10").show()
  //orderDS.show(numRows=5)

}