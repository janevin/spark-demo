package com.lz.demo.dataframe

import org.apache.spark.sql.SparkSession

/**
 * 数据去重
 */
object DataFrameOperate4 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().master("local[*]").appName("SparkSQL")
            .config("spark.sql.shuffle.partitions", "2").getOrCreate()

        import spark.implicits._

        val simpleData = Seq(("James", "Sales", 3000),
            ("Michael", "Sales", 4600),
            ("Robert", "Sales", 4100),
            ("Maria", "Finance", 3000),
            ("James", "Sales", 3000),
            ("Scott", "Finance", 3300),
            ("Jen", "Finance", 3900),
            ("Jeff", "Marketing", 3000),
            ("Kumar", "Marketing", 2000),
            ("Saif", "Sales", 4100)
        )
        val df = simpleData.toDF("employee_name", "department", "salary")
        df.show()

        // 根据所有字段去重
        val distinctDF = df.distinct()
        println("Distinct count: " + distinctDF.count())
        distinctDF.show()

        val distinctDF2 = df.dropDuplicates()
        println("Distinct count: " + distinctDF2.count())
        distinctDF2.show()

        // 根据指定字段去重
        val dropDisDF = df.dropDuplicates("department", "salary")
        println("Distinct count of department & salary : " + dropDisDF.count())
        dropDisDF.show()

        spark.stop()
    }
}
