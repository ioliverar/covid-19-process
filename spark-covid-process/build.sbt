name := "spark-covid-process"
version := "0.1"
scalaVersion := "2.11.12"
organization := "Orb"

//Scala Libraries
libraryDependencies += "org.scala-lang" % "scala-library" % "2.11.12"

//Spark Libraries
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.4.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.0" % "provided"

//Util Libraries
libraryDependencies += "com.typesafe" % "config" % "1.3.2"
libraryDependencies += "commons-io" % "commons-io" % "2.6"
