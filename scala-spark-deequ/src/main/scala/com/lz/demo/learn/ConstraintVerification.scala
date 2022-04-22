package com.lz.demo.learn

import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object ConstraintVerification {
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

        val verificationResult: VerificationResult = {
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
                        .isNonNegative("Age")) // 非负数
                .run()
        }

        checkResultsAsDataFrame(spark, verificationResult).show(false)
        VerificationResult.successMetricsAsDataFrame(spark, verificationResult).show(false)

        spark.stop()
    }
}
