
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import com.alibaba.fastjson.JSONObject

object test{
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf();
    conf.setMaster("local")
    conf.setAppName("Demo")
    val sc = new SparkContext(conf);

    val rdd = sc.textFile("/Users/jinsong/GitProject/MyDemoScala/data/input/GradeTable")

    rdd.foreach(println)

    val rdd2 = rdd.map(x=>(x.split(" ")(0),x.split(" ")(1),x.split(" ")(2)))

    val rdd3 = rdd.map(x=>(x.split(" ")(1),x.split(" ")(2)))

    val rdd4 = rdd.map(x=>(x.split(" ")(0),x.split(" ")(2)))

    val func = (x:Tuple2[String,String])=>x._2.toInt>90

    val func2 = (x:Tuple2[String,String]) => println(x._1+" "+x._2)

    //rdd3.filter(func).foreach(func2)

    //每个班级平均值
    //rdd4.persist(StorageLevel.MEMORY_ONLY)

    rdd4.map(x=>(x._1,(x._2.toInt,1)))
      .reduceByKey((x,y)=>((x._1+y._1).toInt,x._2+y._2))
      .map(a=>{
        (a._1,(a._2._1)/(a._2._2))
      })
    .foreach(x=>{
      println(x._1.toString+" "+x._2.toString)
    })

//    rdd4.combineByKey(
//      v=>(v,1)
//      ,(a:(Int,Int),v)=>(a._1+v,a._2+1)
//      ,(a:(Int,Int),b:(Int,Int))=>(a._1+b._1,b._2+b._2)
//    )
    //单数值rdd平均值
    val list = List(1,2,3,4)
    val rdd5 = sc.parallelize(list)
    rdd5.map(x=>(x,1))
      .fold(0,0)((x,y)=>{(x._1+y._1,x._2+y._2)})

    rdd5.aggregate((0,0))(
      (a,b)=>(a._1+b,a._2+1)
      ,(a,b)=>(a._1+a._1,a._2+a._2)
    )

    val list2 = List(("a",1),("b",2),("c",3),("d",4))
    val rdd6 = sc.parallelize(list2)

    var jsonobject = new JSONObject()

    val a = rdd6.aggregate[JSONObject](jsonobject)(
      (a:JSONObject,b)=>{
            a.put(b._1.toString,b._2.toString)
            a
          }
      ,(c,d)=>{
        val keyset =  d.keySet()
        import collection.JavaConversions._
        for(key<-keyset){
          c.put(key,d.getString(key))
        }
        c
      }
    )
    println(a)





  }
}