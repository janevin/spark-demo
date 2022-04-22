package com.lz.demo.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * 数据排序
 */
object DataFrameOperate8 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().master("local[*]").appName("SparkSQL")
            .config("spark.sql.shuffle.partitions", "2").getOrCreate()

        import spark.implicits._

        val simpleData = Seq(("James", "Sales", "NY", 90000, 34, 10000),
            ("Michael", "Sales", "NY", 86000, 56, 20000),
            ("Robert", "Sales", "CA", 81000, 30, 23000),
            ("Maria", "Finance", "CA", 90000, 24, 23000),
            ("Raman", "Finance", "CA", 99000, 40, 24000),
            ("Scott", "Finance", "NY", 83000, 36, 19000),
            ("Jen", "Finance", "NY", 79000, 53, 15000),
            ("Jeff", "Marketing", "CA", 80000, 25, 18000),
            ("Kumar", "Marketing", "NY", 91000, 50, 21000)
        )
        val df = simpleData.toDF("employee_name", "department", "state", "salary", "age", "bonus")
        df.printSchema()
        df.show()

        // 使用字符串或 Column 类型排序，等价
        df.sort("department", "state").show(false)
        df.sort(col("department"), col("state")).show(false)

        // orderBy 和 sort 的效果基本上都是一样的
        df.orderBy("department", "state").show(false)
        df.orderBy(col("department"), col("state")).show(false)

        // 控制排序方式，没有指定相应的排序顺序默认都是升序的方式
        df.sort(col("department").asc, col("state").asc).show(false)
        df.orderBy(col("department").asc, col("state").asc).show(false)

        spark.stop()
    }
}
