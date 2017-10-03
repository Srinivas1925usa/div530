import org.apache.spark.SparkContext, org.apache.spark.SparkConf

import com.typesafe.config.ConfigFactory

import org.apache.hadoop.fs._

object AvgRevenueDaily{
  def main(args:Array[String]){
    val appConf = ConfigFactory.load()
    val conf = new SparkConf().
  setAppName("AvgRevenue - Daily").
  setMaster(appConf.getConfig(args(2)).getString("deploymentmaster"))
  
  val sc = new SparkContext(conf)
  
  val inputPath = args(0)
  val outputPath = args(1)
 /* 
  val fs = FileSystem.get(sc.hadoopConfiguration)
  
  val inputPathExists = fs.exists(new Path(inputPath))
  val outputPathExists = fs.exists(new Path(outputPath))
  
  if(!inputPathExists)
  {
    println("inputPath does not exists")
    return
  }
  
   if(outputPathExists)
  {
   fs.delete(new Path(outputPath),true)
    return
  }
   */
   val ordersRDD = sc.textFile(inputPath + "/orders")
   val orderItemsRDD = sc.textFile(inputPath + "/order_items")
   val ordersCompleted = ordersRDD.filter (rec => (rec.split(",")(3) == "COMPLETE"))
   val orders = ordersCompleted.map(rec => (rec.split(",")(0).toInt, rec.split(",")(1)))
   val orderItemsMap = orderItemsRDD.map(rec => (rec.split(",")(1).toInt, rec.split(",")(4).toFloat))
   val orderItems = orderItemsMap.reduceByKey((acc, value) => acc+value)
   val ordersJoin = orders.join(orderItems)
   val ordersJoinMap = ordersJoin.map(rec => (rec._2._1, rec._2._2))
   val revenuePerDay = ordersJoinMap.aggregateByKey((0.0,0))(
       (acc,value) => (acc._1 +value, acc._2 + 1), (total1, total2) => (total1._1 + total2._2, total1._2 + total2._2))
       
       val averageRevenuePerDay = revenuePerDay.map(rec => (rec._1, BigDecimal(rec._2._1 /rec._2._2).
           setScale(2,BigDecimal.RoundingMode.HALF_UP).toFloat))
           
        val avarageRevenuePerDaySorted = averageRevenuePerDay.sortByKey()
        
        avarageRevenuePerDaySorted.map(rec => rec._1 + ","+ rec._2).saveAsTextFile(outputPath)
  
         
 /*
  val wc = sc.textFile(inputPath).
  flatMap(rec => rec.split(" "))
  map(rec => (rec,1)).
  reduceByKey((acc, value) => acc+value)
     wc.saveAsTextFile(outputPath)
     * 
     */
}
}
