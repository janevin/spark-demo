package com.lz.demo.learn

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.analyzers.Completeness
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.repository.fs.FileSystemMetricsRepository
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import com.google.common.io.Files
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}

import java.io.File

object Repository {
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

        // 将指标计算结果保存在本地json文件中，也可以保存到hdfs和s3
        val metricsFile = new File(Files.createTempDir(), "metrics.json")
        val repository: MetricsRepository = FileSystemMetricsRepository(spark, metricsFile.getAbsolutePath)

        // 结果键需要使用时间戳
        val resultKey = ResultKey(System.currentTimeMillis(), Map("tag" -> "repositoryExample"))

        VerificationSuite()
            .onData(df)
            .addCheck(
                Check(CheckLevel.Error, "Review Check")
                    .hasSize(_ <= 1000) // 不超过1000条记录
                    .hasMin("Fare", _ == 0) // 最小值为0
                    .hasMax("Fare", _ == 512.3292) // 最大值为93.5
                    .isComplete("Name") // 非空
                    .isUnique("PassengerId") // 唯一值
                    .isContainedIn("Pclass", Array("1", "2", "3"))
                    .isNonNegative("Age")) // 非负数)
            // 保存结果
            .useRepository(repository)
            .saveOrAppendResult(resultKey)
            .run()

        // 加载存储在结果键下的特定分析器的指标
        val completenessOfProductName = repository
            .loadByKey(resultKey).get
            .metric(Completeness("Name")).get
        println(s"The completeness of the productName column is: $completenessOfProductName")

        // 查询过去10秒钟的所有指标为json
        val json = repository.load()
            .after(System.currentTimeMillis() - 10000)
            .getSuccessMetricsAsJson()
        println(s"Metrics from the last 10 seconds:\n$json")

        // 通过标签值查询，并以数据框的形式显示检索结果
        repository.load()
            .withTagValues(Map("tag" -> "repositoryExample"))
            .getSuccessMetricsAsDataFrame(spark)
            .show()

        spark.stop()
    }
}
