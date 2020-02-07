package jobs

import com.twitter.scalding.Args
import common.{Environment, FPGrowthCalculations}
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


    val fpgrowth = new FPGrowth().setItemsCol(ITEMS)
      .setMinSupport(minSupport)
      .setMinConfidence(minConfidence)
      .setPredictionCol(RECS)

    val model = fpgrowth.fit(itemsPerPerchases)

    val products = transactions.select(STOCK_CODE, PRODUCT_NAME, PRICE)
      .withColumnRenamed(STOCK_CODE, ITEMS)
      .distinct()
      .withColumn(ITEMS, array(ITEMS))

    val predictions = FPGrowthCalculations(products)
        .transform(model)
        .getPredictedDataset
        .withColumnRenamed(ITEMS, STOCK_CODE)
        .withColumn(STOCK_CODE, concat_ws("",col(STOCK_CODE)))



    predictions.show()


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
  val RECS = "recs"

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


