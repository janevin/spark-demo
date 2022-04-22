package com.lz.demo.learn

import com.amazon.deequ.analyzers._
import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Analyzer {
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

        val analysisResult = AnalysisRunner
            .onData(df)
            .addAnalyzer(ApproxQuantiles("Age", quantiles = Seq(0.1, 0.5, 0.9))) // Age字段分布的近似分位数
            .addAnalyzer(Completeness("Age")) // Age字段非空的比例
            .addAnalyzer(Compliance("young", "Age <= 18")) // Age字段符合给定约束的比例
            .addAnalyzer(CountDistinct("Age")) // 唯一值的数量
            .addAnalyzer(Maximum("Fare")) // 最大值
            .addAnalyzer(Minimum("Fare")) // 最小值
            .addAnalyzer(Mean("Fare")) // 均值
            .addAnalyzer(Sum("Fare")) // 求和
            .addAnalyzer(Size()) // 记录数
            .addAnalyzer(PatternMatch("Name", pattern = raw".*Anna.*".r)) // 符合正则表达式的记录比例
            .run()

        val analysisResultDf = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
        analysisResultDf.show(false)

        spark.stop()
    }
}
