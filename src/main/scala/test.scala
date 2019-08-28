import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf

object test{
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf();
    conf.setMaster("local")
    conf.setAppName("Demo")
    val sc = new SparkContext(conf);

    val rdd = sc.textFile("/Users/jinsong/GitProject/MyDemoScala/data/input/GradeTable")
    rdd.foreach(println)
    val rdd2 = rdd.map(x=>(x.split(" ")(0),x.split(" ")(1),x.split(" ")(2)))

    rdd2.foreach(println)

    rdd2.groupBy(x=>x._1).foreach(println)

    rdd2.sortBy(x=>x._3)  .foreach(println)
  }
}