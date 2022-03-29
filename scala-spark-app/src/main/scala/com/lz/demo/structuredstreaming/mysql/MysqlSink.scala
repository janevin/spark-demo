package com.lz.demo.structuredstreaming.mysql

import org.apache.spark.sql.{ForeachWriter, Row}

import java.sql._

class MysqlSink(url: String, userName: String, password: String) extends ForeachWriter[Row] {
    var statement: Statement = _
    var resultSet: ResultSet = _
    var connection: Connection = _

    override def open(partitionId: Long, version: Long): Boolean = {
        Class.forName("com.mysql.cj.jdbc.Driver")
        connection = DriverManager.getConnection(url, userName, password)
        statement = connection.createStatement()
        true
    }

    override def process(value: Row): Unit = {
        val word = value.getAs[String]("word")
        val count = value.getAs[Long]("count")

        // 当表中有主键时，replace into才会更新数据
        val sql = "replace into word_count(word, count) values('" + word + "'," + count + ")"
        statement.execute(sql)
    }

    override def close(errorOrNull: Throwable): Unit = {
        connection.close()
    }
}
