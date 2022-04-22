package com.lz.demo.learn

import com.amazon.deequ.profiles.ColumnProfilerRunner
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}

object Profile {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().master("local[*]").appName("SparkSQL")
            .getOrCreate()

        val schema = new StructType()
            .add("PassengerId", IntegerType)
            .add("Survived", IntegerType)
            .add("Pclass", IntegerType)
            .add("Name", StringType)
            .add("Sex", StringType)
            .add("Age", IntegerType)
            .add("SibSp", IntegerType)
            .add("Parch", IntegerType)
            .add("Ticket", StringType)
            .add("Fare", DoubleType)
            .add("Cabin", StringType)
            .add("Embarked", StringType)

        val df = spark.read
            .format("csv")
            .schema(schema)
            .option("header", "true")
            .load("data/titanic.csv")

        val result = ColumnProfilerRunner()
            .onData(df)
            .run()

        result.profiles.foreach(v => println(v._1 + " | " + v._2))

        spark.stop()
    }
}
