import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

public class SimpleApp {
    public static void main(String[] args) {
        String logFile = "file:///Users/css/data/test.txt"; // Should be some file on your system
        SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile).cache();

        long numHello = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                return s.contains("hello");
            }
        }).count();

        long numWorld = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                return s.contains("world");
            }
        }).count();

        System.out.println("Lines with hello: " + numHello + ", lines with world: " + numWorld);

        sc.stop();
    }
}
