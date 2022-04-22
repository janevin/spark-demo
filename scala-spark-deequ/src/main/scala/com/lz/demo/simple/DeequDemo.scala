package com.lz.demo.simple

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus
import org.apache.spark.sql.SparkSession

object DeequDemo {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().master("local[*]").appName("SparkSQL")
            .config("spark.sql.shuffle.partitions", "2").getOrCreate()

        val rdd = spark.sparkContext.parallelize(Seq(
            Item(1, "Thingy A", "awesome thing.", "high", -1),
            Item(2, "Thingy B", "available at http://thingb.com", null, 0),
            Item(3, null, null, "low", 5),
            Item(4, "Thingy D", "checkout https://thingd.ca", "Low", -10),
            Item(5, "Thingy E", "https://google.com", "low", 10),
            Item(5, "Thingy F", null, "high", 12)))

        val data = spark.createDataFrame(rdd)

        val verificationResult = VerificationSuite()
            .onData(data)
            .addCheck(
                Check(CheckLevel.Error, "testing1")
                    .hasSize(_ == 5) // 判断数据量是否是5条
                    .isComplete("id") // 判断该列是否全部不为空
                    .isUnique("id") // 判断该字段是否是唯一
                    .isContainedIn("priority", Array("high", "low")) // 该字段值是否在枚举范围中
                    .isNonNegative("numViews")) //该字段不能为负数
            .addCheck(
                Check(CheckLevel.Warning, "testing2")
                    .containsURL("description", _ >= 0.6) // 包含url的记录比例是否达到一半
                    .hasApproxQuantile("numViews", 0.5, _ <= 8) // 有一半的值不超过10
                    .satisfies("abs(numViews) <= 8", "MyConstrain")) // 自定义策略
            .run()

        if (verificationResult.status == CheckStatus.Success) {
            println("The data passed the test, everything is fine!")
        } else {
            println("We found errors in the data:\n")

            val resultsForAllConstraints = verificationResult.checkResults
                .flatMap { case (_, checkResult) => checkResult.constraintResults }

            resultsForAllConstraints
                .filter(_.status != ConstraintStatus.Success)
                .foreach(result => println(s"${result.constraint}: ${result.message.get}"))
        }

        spark.stop()
    }
}
