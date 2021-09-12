package spark;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class JavaReadLineByHDFS {

    public static void main(String[] args){
        SparkConf sparkConf = new SparkConf().setAppName("JavaReadLineByHDFS")
                .setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        long count = jsc.hadoopFile("file:///home/wangzhe/Documents/dataservice-web.log", TextInputFormat.class, LongWritable.class,
                Text.class)
                .count();
        System.out.println("count: " + count);
        jsc.stop();
    }
}
