package com.lz.demo.dataframe

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
 * collect 和 collectAsList
 */
object DataFrameOperate3 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().master("local[*]").appName("SparkSQL")
            .config("spark.sql.shuffle.partitions", "2").getOrCreate()

        val structureData = Seq(
            Row(Row("James", "Smith"), 3100, "M"),
            Row(Row("Michael", "Rose"), 4300, "M"),
            Row(Row("Robert", "Williams"), 1400, "M"),
            Row(Row("Maria", "Jones"), 5500, "F"),
            Row(Row("Jen", "Brown"), 3000, "F")
        )

        val structureSchema = new StructType()
            .add("name", new StructType()
                .add("first_name", StringType)
                .add("last_name", StringType))
            .add("salary", IntegerType)
            .add("gender", StringType)

        val df = spark.createDataFrame(spark.sparkContext.parallelize(structureData), structureSchema)
        df.show()

        // collect 和 collectAsList
        val colList = df.collectAsList()
        colList.forEach(row => {
            val fullName: Row = row.getStruct(0)
            val firstName = fullName.getString(0)
            val lastName = fullName.getAs[String]("last_name")
            val salary = row.getInt(1)
            val gender = row.getString(2)
            println(firstName + ", " + lastName + ", " + salary + ", " + gender)
        })

        println()

        val colData = df.collect()
        colData.foreach(row => {
            val fullName: Row = row.getStruct(0)
            val firstName = fullName.getString(0)
            val lastName = fullName.getAs[String]("last_name")
            val salary = row.getInt(1)
            val gender = row.getString(2)
            println(firstName + ", " + lastName + ", " + salary + ", " + gender)
        })
    }
}
