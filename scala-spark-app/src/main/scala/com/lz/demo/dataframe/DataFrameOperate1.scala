package com.lz.demo.dataframe

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
 * 删除列操作
 */
object DataFrameOperate1 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().master("local[*]").appName("SparkSQL")
            .config("spark.sql.shuffle.partitions", "2").getOrCreate()

        val structureData = Seq(
            Row("James", "Smith", "36636", "NewYork", 3100, "M"),
            Row("Michael", "Rose", "40288", "California", 4300, "M"),
            Row("Robert", "Williams", "42114", "Florida", 1400, "M"),
            Row("Maria", "Jones", "39192", "Florida", 5500, "F"),
            Row("Jen", "Brown", "34561", "NewYork", 3000, "F")
        )

        val structureSchema = new StructType()
            .add("first_name", StringType)
            .add("last_name", StringType)
            .add("id", StringType)
            .add("location", StringType)
            .add("salary", IntegerType)
            .add("gender", StringType)

        val df = spark.createDataFrame(spark.sparkContext.parallelize(structureData), structureSchema)
        df.show()

        // 删除列
        df.drop("first_name").show()
        df.drop("first_name", "last_name").show()
        val cols = Seq("first_name", "last_name", "location")
        df.drop(cols: _*).show()
    }
}
