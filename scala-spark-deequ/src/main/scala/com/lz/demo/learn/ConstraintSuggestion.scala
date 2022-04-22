package com.lz.demo.learn

import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}

object ConstraintSuggestion {
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

        val suggestionResult = {
            ConstraintSuggestionRunner()
                .onData(df)
                // 约束建议的默认规则集
                .addConstraintRules(Rules.DEFAULT)
                .run()
        }

        import spark.implicits._

        val suggestionDataFrame = suggestionResult.constraintSuggestions.flatMap {
            case (column, suggestions) =>
                suggestions.map { constraint =>
                    (column, constraint.description, constraint.codeForConstraint)
                }
        }.toSeq.toDS()
        suggestionDataFrame.show(numRows = 100, truncate = 100)

        spark.stop()
    }
}
