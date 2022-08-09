package io.orb.spark.covid

import com.typesafe.config.ConfigFactory
import io.orb.spark.covid.storage.DAOStorage._
import org.apache.spark.sql.functions.{col, date_format}

object SparkBaseApplication extends App with SparkJob {

  //Declare Properties
  val conf = ConfigFactory.load()
  val envProps = conf.getConfig("dev") //getConfig(args(0))

  //Read covid files from csv
  val cleanCovidData = readCSVFile(spark, envProps.getString("src.input.path"))

  //Building columns from covid file
  val covidDF = cleanCovidData
    .select(
      col("Province/State").as("state"),
      col("Country/Region").as("country"),
      date_format(col("Date"), "dd/MM/yyyy").as("reported_date"),
      col("Confirmed").cast("int").as("confirmed"),
      col("Deaths").cast("int").as("deaths"),
      col("Recovered").cast("int").as("recovered"),
      col("Active").cast("int").as("active"),
      col("WHO Region").as("region")
    )

  // Read from Parquet
  val covidParquetDf = readParquetFile(spark, envProps.getString("src.parquet.path"))

  //Filtering new records
  val CovidUniqueDf = covidDF.as("c")
    .join(covidParquetDf.as("cp"), col("c.country") === col("cp.country") && col("c.reported_date") == col("cp.reported_date") , "left")
    .where("cp.country IS NULL")
    .select("c.*")

  //Save on Parquet format
  saveCovidCases(CovidUniqueDf, envProps.getString("dts.output.path"))

  // Validate incremental
  //covidParquetDf.createOrReplaceTempView("ParquetTable")
  //val sparkQuery = spark.sql("select count(*) from ParquetTable ")
  //sparkQuery.show()

  //End Process
  spark.close()

}
