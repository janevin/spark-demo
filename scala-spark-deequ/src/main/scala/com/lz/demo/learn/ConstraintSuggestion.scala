package com.lz.demo.learn

import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}

object ConstraintSuggestion {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().master("local[*]").appName("SparkSQL")
            .getOrCreate()

        val schema = StructType(Array(
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
// +-----------+----------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------+
// |         _1|                                                                                                  _2|                                                                                                  _3|
// +-----------+----------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------+
// |PassengerId|                                                                           'PassengerId' is not null|                                                                          .isComplete("PassengerId")|
// |PassengerId|                                                                'PassengerId' has no negative values|                                                                       .isNonNegative("PassengerId")|
// |       Name|                                                                                  'Name' is not null|                                                                                 .isComplete("Name")|
// |     Ticket|                                                                                'Ticket' is not null|                                                                               .isComplete("Ticket")|
// |     Ticket|                                                                          'Ticket' has type Integral|                                             .hasDataType("Ticket", ConstrainableDataTypes.Integral)|
// |     Ticket|                                 'Ticket' has value range '0', '1', '2' for at least 97.0% of values|         .isContainedIn("Ticket", Array("0", "1", "2"), _ >= 0.97, Some("It should be above 0.97!"))|
// |     Ticket|                                                                     'Ticket' has no negative values|                                                                            .isNonNegative("Ticket")|
// |     Pclass|                                                                                'Pclass' is not null|                                                                               .isComplete("Pclass")|
// |     Pclass|                                                              'Pclass' has value range '3', '1', '2'|                                                      .isContainedIn("Pclass", Array("3", "1", "2"))|
// |     Pclass|                                                                     'Pclass' has no negative values|                                                                            .isNonNegative("Pclass")|
// |      Parch|                                                                                 'Parch' is not null|                                                                                .isComplete("Parch")|
// |      Parch|                                           'Parch' has value range '0', '1', '2', '4', '3', '8', '5'|                                   .isContainedIn("Parch", Array("0", "1", "2", "4", "3", "8", "5"))|
// |      Parch|                                       'Parch' has value range '0', '1' for at least 89.0% of values|               .isContainedIn("Parch", Array("0", "1"), _ >= 0.89, Some("It should be above 0.89!"))|
// |      Parch|                                                                      'Parch' has no negative values|                                                                             .isNonNegative("Parch")|
// |   Embarked|                                                         'Embarked' has less than 80% missing values|                             .hasCompleteness("Embarked", _ >= 0.2, Some("It should be above 0.2!"))|
// |        Age|                                                                            'Age' has value range ''|                                                                    .isContainedIn("Age", Array(""))|
// |      Cabin|                                                                                 'Cabin' is not null|                                                                                .isComplete("Cabin")|
// |      Cabin|                                                                         'Cabin' has type Fractional|                                            .hasDataType("Cabin", ConstrainableDataTypes.Fractional)|
// |      Cabin|                                                                      'Cabin' has no negative values|                                                                             .isNonNegative("Cabin")|
// |       Fare|                                                             'Fare' has less than 29% missing values|                               .hasCompleteness("Fare", _ >= 0.71, Some("It should be above 0.71!"))|
// |       Fare|                                                                       'Fare' has no negative values|                                                                              .isNonNegative("Fare")|
// |      SibSp|                                                            'SibSp' has less than 26% missing values|                              .hasCompleteness("SibSp", _ >= 0.74, Some("It should be above 0.74!"))|
// |      SibSp|'SibSp' has value range '24', '22', '18', '28', '30', '19', '21', '25', '36', '29', '27', '32', '...|.isContainedIn("SibSp", Array("24", "22", "18", "28", "30", "19", "21", "25", "36", "29", "27", "...|
// |      SibSp|'SibSp' has value range '24', '22', '18', '28', '30', '19', '21', '25', '36', '29', '27', '32', '...|.isContainedIn("SibSp", Array("24", "22", "18", "28", "30", "19", "21", "25", "36", "29", "27", "...|
// |      SibSp|                                                                      'SibSp' has no negative values|                                                                             .isNonNegative("SibSp")|
// |   Survived|                                                                              'Survived' is not null|                                                                             .isComplete("Survived")|
// |   Survived|                                                                 'Survived' has value range '0', '1'|                                                         .isContainedIn("Survived", Array("0", "1"))|
// |   Survived|                                                                   'Survived' has no negative values|                                                                          .isNonNegative("Survived")|
// |        Sex|                                                                                   'Sex' is not null|                                                                                  .isComplete("Sex")|
// +-----------+----------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------+
        spark.stop()
    }
}
