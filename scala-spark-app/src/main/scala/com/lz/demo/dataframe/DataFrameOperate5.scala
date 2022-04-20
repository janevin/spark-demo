package com.lz.demo.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

/**
 * 行列转换
 */
object DataFrameOperate5 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().master("local[*]").appName("SparkSQL")
            .config("spark.sql.shuffle.partitions", "2").getOrCreate()

        val data = Seq(("Banana", 1000, "USA"), ("Carrots", 1500, "USA"), ("Beans", 1600, "USA"),
            ("Orange", 2000, "USA"), ("Orange", 2000, "USA"), ("Banana", 400, "China"),
            ("Carrots", 1200, "China"), ("Beans", 1500, "China"), ("Orange", 4000, "China"),
            ("Banana", 2000, "Canada"), ("Carrots", 2000, "Canada"), ("Beans", 2000, "Mexico"))

        import spark.sqlContext.implicits._
        val df = data.toDF("Product", "Amount", "Country")
        df.show()

        val pivotDF1 = df.groupBy("Product").pivot("Country").sum("Amount")
        pivotDF1.show()

        val pivotDF2 = df.groupBy("Product", "Country")
            .sum("Amount")
            .groupBy("Product")
            .pivot("Country")
            .sum("sum(Amount)")
        pivotDF2.show()

        val unPivotDF = pivotDF2.select($"Product",
            expr("stack(3, 'Canada', Canada, 'China', China, 'Mexico', Mexico) as (Country, Total)"))
            .where("Total is not null")
        unPivotDF.show()
    }
}
