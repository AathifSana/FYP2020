package jobs

import com.twitter.scalding.Args
import common.Environment
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.types._
import common.Constants._
import datasources.DataSource
import org.apache.spark.sql.functions._


object CustomerSegmentationJob {

  def main(args: Array[String]): Unit = {

    val params = Args(args)

    implicit val sc = Environment.sparkSession.sparkContext
    sc.setLogLevel("ERROR")

    val inputPath = params.required("input")
    val writePath = params.required("output")

    val transactions = DataSource.getTSVDataFrameWithSchema(inputPath, schema)

    val perPerchase = transactions.groupBy(INVOICE_NO, DATE_TIME, CUSTOMER_ID, COUNTRY)
      .agg(sum(col(PRICE) * col(QUANTITY)) as PRICE_PER_PURCHASE,
        (countDistinct(STOCK_CODE) / sum(QUANTITY)) as PRODUCT_QUANTITY_RATIO_PER_PURCHASE)

    val avgPerPerchase = perPerchase.agg(sum(PRICE_PER_PURCHASE) as TOTAL_REVENUE,
      avg(PRICE_PER_PURCHASE) as AVG_PRICE_PER_PURCHASE,
      count(PRICE_PER_PURCHASE) as TOTAL_PURCHASES,
      avg(PRODUCT_QUANTITY_RATIO_PER_PURCHASE) as AVG_PRODUCT_QUANTITY_RATIO_PER_PURCHASE)

    val perCustomer = perPerchase.groupBy(CUSTOMER_ID)
        .agg(count(ALL) as PURCHASES_PER_CUSTOMER,
        avg(PRICE_PER_PURCHASE) as AVG_PRICE_PER_PURCHASE_PER_CUSTOMER,
        avg(PRODUCT_QUANTITY_RATIO_PER_PURCHASE) as AVG_PRODUCT_QUANTITY_RATIO_PER_PURCHASE_PER_CUSTOMER)

    val avgPerCustomer = perCustomer.agg(avg(PURCHASES_PER_CUSTOMER) as AVG_PURCHASES_PER_CUSTOMER)


    val avgPurchasesPerCustomer = avgPerCustomer.head().getDouble(0)
    val avgPricePerPurchase = avgPerPerchase.head().getDouble(1)
    val avgProductQuantityRatioPerPurchase = avgPerPerchase.head().getDouble(3)


    val segmentDf = perCustomer.withColumn(SEGMENT, {

      val segUDF = udf(getSegmentFunc)

      segUDF(col(PURCHASES_PER_CUSTOMER),
      col(AVG_PRICE_PER_PURCHASE_PER_CUSTOMER),
      col(AVG_PRODUCT_QUANTITY_RATIO_PER_PURCHASE_PER_CUSTOMER),
      lit(avgPurchasesPerCustomer), lit(avgPricePerPurchase), lit(avgProductQuantityRatioPerPurchase))

    })


    val segmentStats = segmentDf.groupBy(SEGMENT)
      .agg(count(ALL).as(NO_OF_CUSTOMERS),
      max(PURCHASES_PER_CUSTOMER),
      min(PURCHASES_PER_CUSTOMER),
      avg(PURCHASES_PER_CUSTOMER),
      max(AVG_PRICE_PER_PURCHASE_PER_CUSTOMER),
      min(AVG_PRICE_PER_PURCHASE_PER_CUSTOMER),
      avg(AVG_PRICE_PER_PURCHASE_PER_CUSTOMER),
      max(AVG_PRODUCT_QUANTITY_RATIO_PER_PURCHASE_PER_CUSTOMER),
      min(AVG_PRODUCT_QUANTITY_RATIO_PER_PURCHASE_PER_CUSTOMER),
      avg(AVG_PRODUCT_QUANTITY_RATIO_PER_PURCHASE_PER_CUSTOMER))


    DataSource.saveDataFrameAsTSV(
      segmentDf.repartition(1),
      writePath+"/customer_segments",
      header = STR_BOOL_TRUE
    )

    DataSource.saveDataFrameAsTSV(
      segmentStats.repartition(1),
      writePath+"/segment_statistics",
      header = STR_BOOL_TRUE
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
  val ALL = "*"
  val SEGMENT = "segment"

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


  val getSegmentFunc : ((Double,Double,Double,Double,Double,Double) => String) =
    (purchasePerCustomer : Double, pricePerPurchase :Double, productQuantityRatio:Double,
     avgPurchasePerCustomer:Double,avgPricePerPurchase :Double, avgProductQuantityRatio:Double) => {

      var segment = StringUtils.EMPTY

      if (purchasePerCustomer >= avgPurchasePerCustomer && pricePerPurchase >= avgPricePerPurchase && productQuantityRatio < avgProductQuantityRatio){
        segment = "1"
      }else if(purchasePerCustomer >= avgPurchasePerCustomer && pricePerPurchase >= avgPricePerPurchase && productQuantityRatio >= avgProductQuantityRatio){
        segment = "2"
      }else if(purchasePerCustomer >= avgPurchasePerCustomer && pricePerPurchase < avgPricePerPurchase && productQuantityRatio < avgProductQuantityRatio){
        segment = "3"
      }else if(purchasePerCustomer >= avgPurchasePerCustomer && pricePerPurchase < avgPricePerPurchase && productQuantityRatio >= avgProductQuantityRatio){
        segment = "4"
      }else if(purchasePerCustomer < avgPurchasePerCustomer && pricePerPurchase >= avgPricePerPurchase && productQuantityRatio < avgProductQuantityRatio){
        segment = "5"
      }else if(purchasePerCustomer < avgPurchasePerCustomer && pricePerPurchase >= avgPricePerPurchase && productQuantityRatio >= avgProductQuantityRatio){
        segment = "6"
      }else if(purchasePerCustomer < avgPurchasePerCustomer && pricePerPurchase < avgPricePerPurchase && productQuantityRatio < avgProductQuantityRatio){
        segment = "7"
      }else if(purchasePerCustomer < avgPurchasePerCustomer && pricePerPurchase < avgPricePerPurchase && productQuantityRatio >= avgProductQuantityRatio){
        segment = "8"
      }

      segment
    }
}
