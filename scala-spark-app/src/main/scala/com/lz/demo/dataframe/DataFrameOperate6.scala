package com.lz.demo.dataframe

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
 * 数据类型
 */
object DataFrameOperate6 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().master("local[*]").appName("SparkSQL")
            .config("spark.sql.shuffle.partitions", "2").getOrCreate()

        val typeFromJson = DataType.fromJson(
            """{"type":"array",
              |"elementType":"string","containsNull":false}""".stripMargin)
        println(typeFromJson.getClass)
        val typeFromJson2 = DataType.fromJson("\"string\"")
        println(typeFromJson2.getClass)

        println("--------------------------------------------------------")
        val ddlSchemaStr = "`fullName` STRUCT<`first`: STRING, `last`: STRING," +
            "`middle`: STRING>,`age` INT,`gender` STRING"
        val ddlSchema = DataType.fromDDL(ddlSchemaStr)
        println(ddlSchema.getClass)

        println("--------------------------------------------------------")
        val strType = DataTypes.StringType
        println("json : " + strType.json)
        println("prettyJson : " + strType.prettyJson)
        println("simpleString : " + strType.simpleString)
        println("sql : " + strType.sql)
        println("typeName : " + strType.typeName)
        println("catalogString : " + strType.catalogString)
        println("defaultSize : " + strType.defaultSize)

        println("--------------------------------------------------------")
        val arrayType1 = ArrayType(IntegerType, containsNull = false)
        val arrayType = DataTypes.createArrayType(StringType, true)
        println("containsNull : " + arrayType.containsNull)
        println("elementType : " + arrayType.elementType)
        println("productElement : " + arrayType.productElement(0))
        println("json() : " + arrayType.json) // Represents json string of datatype
        println("prettyJson() : " + arrayType.prettyJson) // Gets json in pretty format
        println("simpleString() : " + arrayType.simpleString) // simple string
        println("sql() : " + arrayType.sql) // SQL format
        println("typeName() : " + arrayType.typeName) // type name
        println("catalogString() : " + arrayType.catalogString) // catalog string
        println("defaultSize() : " + arrayType.defaultSize) // default size

        println("--------------------------------------------------------")
        val mapType1 = MapType(StringType, IntegerType)
        val mapType = DataTypes.createMapType(StringType, IntegerType)
        println("keyType() : " + mapType.keyType)
        println("valueType() : " + mapType.valueType)
        println("valueContainsNull() : " + mapType.valueContainsNull)
        println("productElement(1) : " + mapType.productElement(1))

        println("--------------------------------------------------------")
        val structureData = Seq(
            Row(Row("James", "Smith"), 3100, "M"),
            Row(Row("Michael", "Rose"), 4300, "M"),
            Row(Row("Robert", "Williams"), 1400, "M"),
            Row(Row("Maria", "Jones"), 5500, "F"),
            Row(Row("Jen", "Brown"), 3000, "F")
        )

        val structureSchema = new StructType()
            .add("name", new StructType()
                .add("first_name", StringType)
                .add("last_name", StringType))
            .add("salary", IntegerType)
            .add("gender", StringType)

        val df = spark.createDataFrame(spark.sparkContext.parallelize(structureData), structureSchema)
        df.printSchema()

        spark.stop()
    }
}
