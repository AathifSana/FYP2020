package jobs

import com.twitter.scalding.Args
import common.Common.{STR_BOOL_TRUE, atLeastOneNumberUDF, validateIsNumbersUDF}
import common.Environment
import datasources.DataSource
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{FloatType, IntegerType}

/*
  Filters out the invalid data.
 */
object DataPreprocessingJob {

  def main(args: Array[String]): Unit = {

    val params = Args(args)

    implicit val sc = Environment.sparkSession.sparkContext
    sc.setLogLevel("ERROR")

    val inputPath = params.required("input")
    val writePath = params.required("output")

    val transactions = DataSource.getTSVDataFrame(inputPath, header = STR_BOOL_TRUE)

    val validTransactions = transactions
      .withColumn(QUANTITY, col(QUANTITY).cast(IntegerType))
      .withColumn(PRICE, col(PRICE).cast(FloatType))
      .filter(
        col(STOCK_CODE).isNotNull && col(STOCK_CODE) =!= StringUtils.EMPTY &&
        col(DATE_TIME).isNotNull && col(DATE_TIME)=!= StringUtils.EMPTY &&
        col(CUSTOMER_ID).isNotNull && col(CUSTOMER_ID) =!= StringUtils.EMPTY
      )

    val filteredTransactions = validTransactions
      .filter(
        validateIsNumbersUDF(col(CUSTOMER_ID))
        && validateIsNumbersUDF(col(INVOICE_NO))
        && col(QUANTITY) > 0 && col(PRICE) > 0
        && atLeastOneNumberUDF(col(STOCK_CODE))
      )

    DataSource.saveDataFrameAsTSV(
      filteredTransactions.repartition(1),
      writePath
    )

  }

  val INVOICE_NO = "InvoiceNo"
  val STOCK_CODE = "StockCode"
  val PRODUCT_NAME = "Description"
  val QUANTITY = "Quantity"
  val DATE_TIME = "InvoiceDate"
  val PRICE = "UnitPrice"
  val CUSTOMER_ID = "CustomerID"
  val COUNTRY = "Country"

  val COUNT = "count"
}
