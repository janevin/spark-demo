package com.lz.demo.dataframe

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
 * case when 表达式
 */
object DataFrameOperate2 {
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

        // case when 表达方法
        df.withColumn("new_gender", when(col("gender") === "M", "Male")
            .when(col("gender") === "F", "Female")
            .otherwise("Unknown")).show()
        df.withColumn("new_gender",
            expr("case when gender = 'M' then 'Male' " +
                "when gender = 'F' then 'Female' " +
                "else 'Unknown' end")).show()
        df.select(col("*"),
            expr("case when gender = 'M' then 'Male' " +
                "when gender = 'F' then 'Female' " +
                "else 'Unknown' end").alias("new_gender")).show()
    }
}
