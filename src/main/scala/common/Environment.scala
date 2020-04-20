package common

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Environment {

  lazy val conf = new SparkConf().setAppName("SparkJob")

  lazy val sparkSession = SparkSession.builder()
      .config(conf)
      .appName("SparkJob")
      .enableHiveSupport()
      .getOrCreate()

}
