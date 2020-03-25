package jobs

import com.twitter.scalding.Args
import common.Common.{STR_BOOL_TRUE, atLeastOneNumberUDF, validateIsNumbersUDF}
import common.Environment
import datasources.DataSource
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, collect_list, struct, udf}
import org.apache.spark.sql.types.{FloatType, IntegerType}

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

    val mostFreqPName = udf { list: Seq[Row] =>
      list.maxBy(_.getLong(1)).getString(0)
    }

    val productNames = filteredTransactions.groupBy(STOCK_CODE, PRODUCT_NAME).count()
        .groupBy(STOCK_CODE).agg(mostFreqPName(collect_list(struct(PRODUCT_NAME, COUNT))) as PRODUCT_NAME)


    val mostFreqPrice = udf { list: Seq[Row] =>
      list.maxBy(_.getLong(1)).getFloat(0)
    }

    val productPrices = filteredTransactions.groupBy(STOCK_CODE, PRICE).count()
        .groupBy(STOCK_CODE).agg(mostFreqPrice(collect_list(struct(PRICE, COUNT))) as PRICE)


    val preProcessed = filteredTransactions.drop(PRODUCT_NAME, PRICE)
        .join(productNames, STOCK_CODE)
        .join(productPrices, STOCK_CODE)
        .select(INVOICE_NO,STOCK_CODE,PRODUCT_NAME,QUANTITY,DATE_TIME,PRICE,CUSTOMER_ID,COUNTRY)


    DataSource.saveDataFrameAsTSV(
      preProcessed.repartition(1),
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
