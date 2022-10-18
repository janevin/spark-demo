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

val customSchema = StructType(Array(
    StructField("PassengerId", IntegerType, true),
    StructField("Survived", IntegerType, true),
    StructField("Pclass", IntegerType, true),
    StructField("Name", StringType, true),
    StructField("Sex", StringType, true),
    StructField("Age", IntegerType, true),
    StructField("SibSp", IntegerType, true),
    StructField("Parch", IntegerType, true),
    StructField("Ticket", StringType, true),
    StructField("Fare", DoubleType, true),
    StructField("Cabin", StringType, true),
    StructField("Embarked", StringType, true)))


        val df = spark.read
            .format("csv")
            .schema(customSchema)
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
// +-------+-------------------------+------------+---------+
// |entity |instance                 |name        |value    |
// +-------+-------------------------+------------+---------+
// |Column |Name                     |Completeness|1.0      |
// |Dataset|*                        |Size        |891.0    |
// |Column |Pclass contained in 1,2,3|Compliance  |1.0      |
// |Column |Age is non-negative      |Compliance  |1.0      |
// |Column |Fare                     |Minimum     |693.0    |
// |Column |Fare                     |Maximum     |3101298.0|
// |Column |PassengerId              |Uniqueness  |1.0      |
// +-------+-------------------------+------------+---------+
        spark.stop()
    }
}
