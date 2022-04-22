name := "spark-deequ"
version := "0.1"
scalaVersion := "2.12.15"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.3"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.3"

libraryDependencies += "com.amazon.deequ" % "deequ" % "1.2.2-spark-3.0"
