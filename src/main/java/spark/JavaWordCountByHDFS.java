package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class JavaWordCountByHDFS {

    public static void main(String[] args){
        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount")
                .setMaster("spark://node1:7077");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = jsc.textFile("/dataset/wikidata", 1);
        JavaRDD<String> words = lines.flatMap(str -> {
            return Arrays.asList(str.split(" ")).iterator();
        });

        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

        counts.saveAsTextFile("/tmp/WordCount");

//        List<Tuple2<String, Integer>> output = counts.collect();
//
//        output.forEach(tuple2 -> {
//            System.out.println("tuple2: " + tuple2);
//        });

        jsc.stop();
    }
}
