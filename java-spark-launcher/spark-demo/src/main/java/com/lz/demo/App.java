package com.lz.demo;

import com.lz.demo.entity.Person;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class App {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName(App.class.getSimpleName())
                .getOrCreate();

        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());

        System.out.println(("接收到参数：" + args[0]));

        List<String> array = Arrays.asList("tom,20", "jerry,30", "spike,40");

        JavaRDD<String> rdd = javaSparkContext.parallelize(array);
        rdd.collect().forEach(System.out::println);

        JavaRDD<Person> personRdd = rdd.map(v -> new Person(v.split(",")[0], Integer.parseInt(v.split(",")[1].trim())));
        personRdd.collect().forEach(System.out::println);

        Dataset<Row> personDf = spark.createDataFrame(personRdd, Person.class);
        personDf.show();

        spark.stop();
    }
}
