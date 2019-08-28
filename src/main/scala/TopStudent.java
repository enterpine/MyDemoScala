import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class TopStudent {
    public static void main(String[] args){

        PairFunction<String,String,String> pairFunction = new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2(s.split(" ")[0],s);
            }
        };


        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("Demo");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        //JavaRDD<String> lines = sc.textFile("/input/GradeTable");
        JavaRDD<String> lines = sc.textFile("/Users/jinsong/GitProject/MyDemoScala/data/input/GradeTable");


        JavaPairRDD<String,String> pr = lines.mapToPair(pairFunction);

        List<Tuple2<String,ArrayList<Tuple2<String,Integer>>>> l = pr.groupByKey().mapValues(x->{

            ArrayList<Tuple2<String,Integer>> al = new ArrayList<>();

            Integer g1 = 0;
            String s1 = new String("");
            Integer g2 = 0;
            String s2 = new String("");
            Integer g3 = 0;
            String s3 = new String("");

            Iterator<String> it = x.iterator();
            while(it.hasNext()){
                String line = it.next();
                Integer grade = Integer.parseInt(line.split(" ")[2]);
                String student = line.split(" ")[1];
                if(grade > g1){
                    g1 = grade;
                    s1 = student;
                }else if(grade>g2){
                    g2=grade;
                    s2 = student;
                }
                else if(grade>g3){
                    g3=grade;
                    s3 = student;
                }
            }

            al.add(new Tuple2<String,Integer>(s1,g1));
            al.add(new Tuple2<String,Integer>(s2,g2));
            al.add(new Tuple2<String,Integer>(s3,g3));


            return al;
        }).collect();


        //List<Tuple2<String,Iterable<String>>> l = pr.groupByKey().collect();

        Iterator<Tuple2<String,ArrayList<Tuple2<String,Integer>>>> it = l.iterator();

        while (it.hasNext()){
            System.out.println(it.next());
        }

    }


}
