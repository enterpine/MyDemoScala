import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object TopStudent2 {
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf();
    conf.setAppName("TopStudent2").setMaster("local")
    val sc = new SparkContext(conf)

    var rdd = sc.textFile("/Users/jinsong/GitProject/MyDemoScala/data/input/GradeTable")

    //rdd.aggregate()

//    rdd.map(s=>{
//      (s.split(" ")(0),s)
//    }).groupByKey()



//    rdd.sortBy(s=>s.split(" ")(2),false).groupBy(s=>s.split(" ")(0)).foreach(s=>{
//      println(s._1)
//
//      val it = s._2.iterator
//      while(it.hasNext){
//        println(it.next())
//      }
//    })

    rdd.sortBy(s=>s.split(" ")(2),false).groupBy(s=>s.split(" ")(0)).take(2)
      .foreach(s=>{
        println(s._1)
        for(a <- s._2.toArray){
          print(a)
        }
      })







  }
}
