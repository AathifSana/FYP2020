package common

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Environment {

  lazy val conf = new SparkConf().setAppName(APP_NM)

  lazy val sparkSession = SparkSession.builder()
      .config(conf)
      .appName(APP_NM)
      .enableHiveSupport()
      .getOrCreate()

  var APP_NM = "SparkJob"
  val LOCAL = "local"
}
