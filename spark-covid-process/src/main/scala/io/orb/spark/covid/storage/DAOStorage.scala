package io.orb.spark.covid.storage
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DAOStorage {

  def readCSVFile(spark: SparkSession, srcPath: String): DataFrame = {
    spark.read
      .option("header", true)
      .options(Map("delimiter" -> ","))
      .csv(srcPath)
  }

  def readParquetFile(spark: SparkSession, srcPath: String): DataFrame = {
    spark.read
      .parquet(srcPath)
  }

  def saveCovidCases(df: DataFrame, dstPath: String): Unit = {
    df.write
      .mode("append")
      .partitionBy("country", "reported_date")
      .parquet(dstPath)
  }

}
