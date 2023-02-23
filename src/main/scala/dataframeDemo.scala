import org.apache.spark.SparkContext
import org.apache.log4j.{Level,Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object dataframeDemo extends App{
  Logger.getLogger("org")setLevel(Level.ERROR)
  val sparkconf=new SparkConf()
  sparkconf.set("spark.app.name","DFDemo")
  sparkconf.set("spark.master","local[2]")

  val spark=SparkSession.builder().config(sparkconf).getOrCreate()

  val orderSchemaDDL="orderid Int, orderdate String,custid Int,orderStatus String"


  val orderSchema=StructType(List(
    StructField("orderid",IntegerType,true),
    StructField("orderdate", StringType, true),
    StructField("custid", IntegerType, true),
    StructField("orderstatus", StringType, true)

  ))

  val orderDF=spark.read.option("header",true)//skip header line
    .option("inferschema",true)//infer or create the schema//
    //.schema(orderSchemaDDL)
    .csv("C:\\Users\\devij\\OneDrive\\Desktop\\Big Data\\SPARK\\orders.csv")

  //processing

  val orderDFRes = orderDF.where(conditionExpr = "order_customer_id > 10000")//narrow transformation
    .select("order_id", "order_customer_id")//narrow transformation
    .groupBy("order_customer_id")//wide transformation-shuffling involved
    .count()//transformation//reduceByKey

  orderDFRes.show()


}
