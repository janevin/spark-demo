package com.lz.demo.rdd.mysql

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

object MysqlOperate {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
        val sc = new SparkContext(conf)

        val personRdd = sc.makeRDD(List(("Tom", 20), ("Jerry", 30), ("Spike", 40)))
        personRdd.foreachPartition(iter => {
            val conn: Connection = DriverManager.getConnection("jdbc:mysql://192.168.56.3:3306/demo?useSSL=false", "root", "123456")
            val sql = "INSERT INTO person (name, age) VALUES (?, ?)"
            val ps: PreparedStatement = conn.prepareStatement(sql)
            iter.foreach(line => {
                ps.setString(1, line._1)
                ps.setInt(2, line._2)
                ps.addBatch()
            })
            ps.executeBatch()
            if (null != conn) conn.close()
            if (null != ps) ps.close()
        })

        val getConn = () => DriverManager.getConnection("jdbc:mysql://192.168.56.3:3306/demo?useSSL=false", "root", "123456")
        val sql = "SELECT name, age FROM person where age >= ? and age <= ?"
        val mapRow = (r: ResultSet) => {
            val name = r.getString("name")
            val age = r.getInt("age")
            (name, age)
        }
        val readPersonRdd = new JdbcRDD[(String, Int)](
            sc,
            getConn,
            sql,
            25,
            35,
            1,
            mapRow
        )
        readPersonRdd.foreach(println)
    }

}
