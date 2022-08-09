package io.orb.spark.covid
import org.apache.spark.sql.SparkSession
import io.orb.spark.covid.commons.Util

trait SparkJob {
  def sparkSessionCore(): SparkSession = {
    SparkSession
      .builder()
      .appName(appName)
      .master("local[1]") //comentar al desplegar en spark
      .config("spark.scheduler.mode", "FAIR")
      .getOrCreate()
  }

  val spark = sparkSessionCore()

  def appName: String = Util.NAME_APP
}
