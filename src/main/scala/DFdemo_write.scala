import org.apache.hadoop.util.StringUtils.format
import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.SaveMode

import scala.reflect.internal.util.NoPosition.source

object DFdemo_write extends App{

  Logger.getLogger("org") setLevel (Level.ERROR)
  val sparkconf = new SparkConf()
  sparkconf.set("spark.app.name", "DFDemo")
  sparkconf.set("spark.master", "local[2]")

  val spark = SparkSession.builder().config(sparkconf).getOrCreate()

  val orderSchemaDDL = "orderid Int, orderdate String,custid Int,orderStatus String"


  val orderSchema = StructType(List(
    StructField("orderid", IntegerType, true),
    StructField("orderdate", StringType, true),
    StructField("custid", IntegerType, true),
    StructField("orderstatus", StringType, true)

  ))

  val orderDF = spark.read.option("header", true) //skip header line
    .option("inferschema", true) //infer or create the schema//
    //.schema(orderSchemaDDL)
    .csv("C:\\Users\\ITC_Dev\\Documents\\warehouse\\orders_buvana.csv")

  //processing

  val orderDFRes = orderDF.where(conditionExpr = "order_customer_id > 10000") //narrow transformation
    .select("order_id", "order_customer_id") //narrow transformation
    .groupBy("order_customer_id") //wide transformation-shuffling involved
    .count() //transformation//reduceByKey

  //writing into a file
  orderDFRes.write.format("csv").mode(SaveMode.Overwrite).option("path","C:\\Users\\ITC_Dev\\Documents\\warehouse\\new").save()

  //writing into a file with partition

  //orderDFRes.repartition(1).write.format(source="csv").mode(SaveMode.Overwrite).option("path","C:\\Users\\devij\\OneDrive\\Desktop\\Big Data\\SPARK\\Output1").save()

  //writing into a file with Coalesce
  //orderDFRes.coalesce(2).write.format("csv").mode(SaveMode.Overwrite).option("path","C:\\Users\\devij\\OneDrive\\Desktop\\Big Data\\SPARK\\Output18").save()
}
