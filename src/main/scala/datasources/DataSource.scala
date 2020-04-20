package datasources

import common.Environment
import common.Common._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode}


object DataSource {

  lazy val sparkSession = Environment.sparkSession
  import sparkSession.implicits._

  def getTSVDataFrame(path: String, delimiter: String = TAB, header: String = STR_BOOL_FALSE) : DataFrame = {

    sparkSession
      .read
      .format(CSV_FORMAT)
      .option("delimiter", delimiter)
      .option("header", header)
      .load(path)
  }

  def getTSVDataFrameWithSchema(path: String, schema: StructType, delimiter: String = TAB) : DataFrame = {

    sparkSession
      .read
      .schema(schema)
      .format(CSV_FORMAT)
      .option("delimiter", delimiter)
      .option("header", STR_BOOL_FALSE)
      .load(path)
  }

  def saveDataFrameAsTSV(df: DataFrame, path: String, delimiter: String = TAB, header: String = STR_BOOL_FALSE): Unit = {
    df.write.mode(SaveMode.Overwrite)
      .format(CSV_FORMAT)
      .option("delimiter", delimiter)
      .option("header", header)
      .save(path)
  }

  //Database loading is for demonstration purpose
  def saveDataFrameToDatabase(df: DataFrame, table: String): Unit ={
    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", "root")
    prop.setProperty("password", "")

    val url = "jdbc:mysql://localhost/FYP"
    df.write.mode("overwrite").jdbc(url, table, prop)
  }
}
