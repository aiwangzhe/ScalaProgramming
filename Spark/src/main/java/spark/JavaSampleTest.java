package spark;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class JavaSampleTest {

    public static void main(String[] args){
        SparkConf sparkConf = new SparkConf().setAppName("JavaReadLineByHDFS")
                .setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        List<Integer> list = Arrays.asList(1, 2, 2,2,2,2,2,2,2,2,2,2,2,2,2,2,2, 4, 5);

        jsc.parallelize(list).sample(true, 0.5)
                .foreach(i -> System.out.println(i));
    }
}
