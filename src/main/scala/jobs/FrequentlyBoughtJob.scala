package jobs

import com.twitter.scalding.Args
import common.Environment
import datasources.DataSource
import common.Constants._
import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object FrequentlyBoughtJob {

  def main(args: Array[String]): Unit = {

    val params = Args(args)

    implicit val sc = Environment.sparkSession.sparkContext
    sc.setLogLevel("ERROR")

    val inputPathSeg = params.getOrElse("inputSeg" , "")
    val inputPath = params.required("input")
    val writePath = params.required("output")
    val minSupport = params.getOrElse("support" , "0.01").toDouble
    val minConfidence = params.getOrElse("confidence" , "0.2").toDouble

//    val customerSegments = DataSource.getTSVDataFrame(inputPathSeg, header = STR_BOOL_TRUE)
    val transactions = DataSource.getTSVDataFrameWithSchema(inputPath, schema)

    val itemsPerPerchases = transactions
      .groupBy(INVOICE_NO, DATE_TIME, CUSTOMER_ID, COUNTRY)
      .agg(collect_set(STOCK_CODE) as ITEMS)

    transactions.show(5)
    println(transactions.count())
    itemsPerPerchases.show(5, false)
    println(itemsPerPerchases.count())

    val fpgrowth = new FPGrowth().setItemsCol(ITEMS).setMinSupport(minSupport).setMinConfidence(minConfidence)
    val model = fpgrowth.fit(itemsPerPerchases)

    model.freqItemsets.show()
    model.associationRules.show()
    model.associationRules.agg(max("confidence"), min("confidence"), avg("confidence")).show()

  }

  val INVOICE_NO = "InvoiceNo"
  val STOCK_CODE = "StockCode"
  val PRODUCT_NAME = "Description"
  val QUANTITY = "Quantity"
  val DATE_TIME = "InvoiceDate"
  val PRICE = "UnitPrice"
  val CUSTOMER_ID = "CustomerID"
  val COUNTRY = "Country"
  val ALL = "*"
  val SEGMENT = "segment"
  val ITEMS = "items"

  //Per Perchase-----------------------
  val PRICE_PER_PURCHASE = "pricePerPurchase"
  val PRODUCT_QUANTITY_RATIO_PER_PURCHASE = "productQuantityRatioPerPurchase"


  //Per Customer-----------------------
  val PURCHASES_PER_CUSTOMER = "purchasesPerCustomer"
  val AVG_PRICE_PER_PURCHASE_PER_CUSTOMER = "avgPricePerPurchasePerCustomer"
  val AVG_PRODUCT_QUANTITY_RATIO_PER_PURCHASE_PER_CUSTOMER = "avgProductQuantityRatioPerPurchasePerCustomer"


  //Avg -------------------------------
  val AVG_PURCHASES_PER_CUSTOMER = "avgPurchasesPerCustomer"
  val AVG_PRICE_PER_PURCHASE = "avgPricePerPurchase"
  val AVG_PRODUCT_QUANTITY_RATIO_PER_PURCHASE = "avgProductQuantityRatioPerPurchase"


  val TOTAL_REVENUE = "totalRevenue"
  val TOTAL_PURCHASES = "totalPurchases"
  val TOTAL_CUSTOMERS = "totalCustomers"

  val NO_OF_CUSTOMERS = "noOfCustomers"

  val schema = StructType(Array(
    StructField(INVOICE_NO, StringType, true),
    StructField(STOCK_CODE, StringType, true),
    StructField(PRODUCT_NAME, StringType, true),
    StructField(QUANTITY, IntegerType, true),
    StructField(DATE_TIME, StringType, true),
    StructField(PRICE, FloatType, true),
    StructField(CUSTOMER_ID, StringType, true),
    StructField(COUNTRY, StringType, true)
  ))

}



