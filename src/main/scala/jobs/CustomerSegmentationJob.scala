package jobs

import com.twitter.scalding.Args
import common.Environment
import org.apache.spark.sql.types._
import common.Common._
import datasources.DataSource
import org.apache.spark.sql.functions._

/**
  * Segments the customers according to the similarity value achieved
  * Updates the previous segment of customers
  */
object CustomerSegmentationJob {

  def main(args: Array[String]): Unit = {

    val params = Args(args)

    implicit val sc = Environment.sparkSession.sparkContext
    sc.setLogLevel("ERROR")

    val inputPath = params.required("input")
    val writePath = params.required("output")
    val customersPath = params.required("customers")

    val maxR = 100.0
    val minR = 1.0

    val transactions = DataSource.getTSVDataFrameWithSchema(inputPath, schema)

    val perPerchase = transactions.groupBy(INVOICE_NO, DATE_TIME, CUSTOMER_ID, COUNTRY)
      .agg(sum(col(PRICE) * col(QUANTITY)) as PRICE_PER_PURCHASE,
        countDistinct(STOCK_CODE) as PRODUCTS_PER_PURCHASE,
        sum(QUANTITY) as QUANTITY_PER_PURCHASE)

    val perCustomer = perPerchase.groupBy(CUSTOMER_ID)
      .agg(count(ALL) as PURCHASES_PER_CUSTOMER,
        avg(PRICE_PER_PURCHASE) as AVG_PRICE_PER_PURCHASE_PER_CUSTOMER,
        avg(PRODUCTS_PER_PURCHASE) as AVG_PRODUCTS_PER_PURCHASE_PER_CUSTOMER,
        avg(QUANTITY_PER_PURCHASE) as AVG_QUANTITY_PER_PURCHASE_PER_CUSTOMER)


    val minMaxPerCustomer = perCustomer.agg(
      max(PURCHASES_PER_CUSTOMER) as MAX_PURCHASES_PER_CUSTOMER,
      min(PURCHASES_PER_CUSTOMER) as MIN_PURCHASES_PER_CUSTOMER,

      max(AVG_PRICE_PER_PURCHASE_PER_CUSTOMER) as MAX_AVG_PRICE_PER_PURCHASE_PER_CUSTOMER,
      min(AVG_PRICE_PER_PURCHASE_PER_CUSTOMER) as MIN_AVG_PRICE_PER_PURCHASE_PER_CUSTOMER,

      max(AVG_PRODUCTS_PER_PURCHASE_PER_CUSTOMER) as MAX_AVG_PRODUCTS_PER_PURCHASE_PER_CUSTOMER,
      min(AVG_PRODUCTS_PER_PURCHASE_PER_CUSTOMER) as MIN_AVG_PRODUCTS_PER_PURCHASE_PER_CUSTOMER,

      max(AVG_QUANTITY_PER_PURCHASE_PER_CUSTOMER) as MAX_AVG_QUANTITY_PER_PURCHASE_PER_CUSTOMER,
      min(AVG_QUANTITY_PER_PURCHASE_PER_CUSTOMER) as MIN_AVG_QUANTITY_PER_PURCHASE_PER_CUSTOMER
    )

    val perCustomerScaled = perCustomer.crossJoin(minMaxPerCustomer)
      .withColumn(SCALED_PURCHASES_PER_CUSTOMER,
        scaleUDF(col(PURCHASES_PER_CUSTOMER), col(MIN_PURCHASES_PER_CUSTOMER), col(MAX_PURCHASES_PER_CUSTOMER),
          lit(minR), lit(maxR)))
      .withColumn(SCALED_AVG_PRICE_PER_PURCHASE_PER_CUSTOMER,
        scaleUDF(col(AVG_PRICE_PER_PURCHASE_PER_CUSTOMER), col(MIN_AVG_PRICE_PER_PURCHASE_PER_CUSTOMER), col(MAX_AVG_PRICE_PER_PURCHASE_PER_CUSTOMER),
          lit(minR), lit(maxR)))
      .withColumn(SCALED_AVG_PRODUCTS_PER_PURCHASE_PER_CUSTOMER,
        scaleUDF(col(AVG_PRODUCTS_PER_PURCHASE_PER_CUSTOMER), col(MIN_AVG_PRODUCTS_PER_PURCHASE_PER_CUSTOMER), col(MAX_AVG_PRODUCTS_PER_PURCHASE_PER_CUSTOMER),
          lit(minR), lit(maxR)))
      .withColumn(SCALED_AVG_QUANTITY_PER_PURCHASE_PER_CUSTOMER,
        scaleUDF(col(AVG_QUANTITY_PER_PURCHASE_PER_CUSTOMER), col(MIN_AVG_QUANTITY_PER_PURCHASE_PER_CUSTOMER), col(MAX_AVG_QUANTITY_PER_PURCHASE_PER_CUSTOMER),
          lit(minR), lit(maxR)))
      .select(CUSTOMER_ID,
        SCALED_PURCHASES_PER_CUSTOMER,
        PURCHASES_PER_CUSTOMER,

        SCALED_AVG_PRICE_PER_PURCHASE_PER_CUSTOMER,
        AVG_PRICE_PER_PURCHASE_PER_CUSTOMER,

        SCALED_AVG_PRODUCTS_PER_PURCHASE_PER_CUSTOMER,
        AVG_PRODUCTS_PER_PURCHASE_PER_CUSTOMER,

        SCALED_AVG_QUANTITY_PER_PURCHASE_PER_CUSTOMER,
        AVG_QUANTITY_PER_PURCHASE_PER_CUSTOMER
      )


    val similarity = perCustomerScaled
      .withColumn(SIMILARITY, euclideanDistanceUDF(array(SCALED_PURCHASES_PER_CUSTOMER,
                                                      SCALED_AVG_PRICE_PER_PURCHASE_PER_CUSTOMER,
                                                      SCALED_AVG_PRODUCTS_PER_PURCHASE_PER_CUSTOMER,
                                                      SCALED_AVG_QUANTITY_PER_PURCHASE_PER_CUSTOMER),
      array(lit(minR),lit(minR), lit(minR), lit(minR))))
      .select(CUSTOMER_ID,SIMILARITY)


    val minMaxSimilarity = similarity.select(SIMILARITY).agg(max(SIMILARITY) as MAX_SIMILARITY,
      min(SIMILARITY) as MIN_SIMILARITY, avg(SIMILARITY) as AVG_SIMILARITY)


    val segments = similarity.crossJoin(minMaxSimilarity)
      .withColumn(SEGMENT, {
        val segmentUDF = udf((sim: Double, avg: Double)=>{
          if(sim < avg) "1" else "2"
        })

        segmentUDF(col(SIMILARITY), col(AVG_SIMILARITY))
      }).select(CUSTOMER_ID, SEGMENT)


    val segmentStats = segments.groupBy(SEGMENT)
      .agg(count(ALL).as(NO_OF_CUSTOMERS))


    val customers = DataSource.getTSVDataFrameWithSchema(customersPath,
      StructType(Array(
        StructField(CUSTOMER_ID, StringType, true),
        StructField(SEGMENT_OLD, StringType, true)
      )))

    val updateSegmentUDF = udf((old_seg: String, new_seg: String)=>{

      var seg = new_seg match {
        case null => old_seg
        case _ => new_seg
      }
      seg
    })

    val updatedCustomers = customers
      .join(segments, Seq(CUSTOMER_ID), "left")
      .withColumn(SEGMENT, updateSegmentUDF(col(SEGMENT_OLD), col(SEGMENT)))
      .select(CUSTOMER_ID, SEGMENT).distinct()


    DataSource.saveDataFrameAsTSV(
      similarity.repartition(1),
      customersPath+"/../debug/similarity",
      header = STR_BOOL_TRUE
    )

    DataSource.saveDataFrameAsTSV(
      segmentStats.repartition(1),
      customersPath+"/../debug/segment_stats",
      header = STR_BOOL_TRUE
    )

    DataSource.saveDataFrameAsTSV(
      perCustomerScaled,
      customersPath+"/../debug/perCustomerValues",
      header = STR_BOOL_TRUE
    )

    DataSource.saveDataFrameAsTSV(
      updatedCustomers.repartition(1),
      writePath
    )

    DataSource.saveDataFrameToDatabase(updatedCustomers, "customers")

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
  val SEGMENT_OLD = "segment_old"

  val PRICE_PER_PURCHASE = "pricePerPurchase"
  val PRODUCT_QUANTITY_RATIO_PER_PURCHASE = "productQuantityRatioPerPurchase"

  val PURCHASES_PER_CUSTOMER = "purchasesPerCustomer"
  val AVG_PRICE_PER_PURCHASE_PER_CUSTOMER = "avgPricePerPurchasePerCustomer"
  val AVG_PRODUCT_QUANTITY_RATIO_PER_PURCHASE_PER_CUSTOMER = "avgProductQuantityRatioPerPurchasePerCustomer"

  val AVG_PURCHASES_PER_CUSTOMER = "avgPurchasesPerCustomer"
  val AVG_PRICE_PER_PURCHASE = "avgPricePerPurchase"
  val AVG_PRODUCT_QUANTITY_RATIO_PER_PURCHASE = "avgProductQuantityRatioPerPurchase"

  val TOTAL_REVENUE = "totalRevenue"
  val TOTAL_PURCHASES = "totalPurchases"
  val TOTAL_CUSTOMERS = "totalCustomers"

  val NO_OF_CUSTOMERS = "noOfCustomers"

  val PRODUCTS_PER_PURCHASE = "productsPerPurchase"
  val QUANTITY_PER_PURCHASE = "quantityPerPurchase"

  val AVG_PRODUCTS_PER_PURCHASE_PER_CUSTOMER = "avgProductsPerPurchasePerCustomer"
  val AVG_QUANTITY_PER_PURCHASE_PER_CUSTOMER = "avgQuantityPerPurchasePerCustomer"

  val SCALED_PURCHASES_PER_CUSTOMER = "scaled_purchasesPerCustomer"
  val SCALED_AVG_PRODUCTS_PER_PURCHASE_PER_CUSTOMER = "scaled_avgProductsPerPurchasePerCustomer"
  val SCALED_AVG_QUANTITY_PER_PURCHASE_PER_CUSTOMER = "scaled_avgQuantityPerPurchasePerCustomer"
  val SCALED_AVG_PRICE_PER_PURCHASE_PER_CUSTOMER = "scaled_avgPricePerPurchasePerCustomer"

  val MAX_PURCHASES_PER_CUSTOMER = "max_purchasesPerCustomer"
  val MIN_PURCHASES_PER_CUSTOMER = "min_purchasesPerCustomer"

  val MAX_AVG_PRODUCTS_PER_PURCHASE_PER_CUSTOMER = "max_avgProductsPerPurchasePerCustomer"
  val MIN_AVG_PRODUCTS_PER_PURCHASE_PER_CUSTOMER = "min_avgProductsPerPurchasePerCustomer"

  val MAX_AVG_QUANTITY_PER_PURCHASE_PER_CUSTOMER = "max_avgQuantityPerPurchasePerCustomer"
  val MIN_AVG_QUANTITY_PER_PURCHASE_PER_CUSTOMER = "min_avgQuantityPerPurchasePerCustomer"

  val MAX_AVG_PRICE_PER_PURCHASE_PER_CUSTOMER = "max_avgPricePerPurchasePerCustomer"
  val MIN_AVG_PRICE_PER_PURCHASE_PER_CUSTOMER = "min_avgPricePerPurchasePerCustomer"

  val SIMILARITY = "similarity"
  val MAX_SIMILARITY = "max_similarity"
  val MIN_SIMILARITY = "min_similarity"
  val AVG_SIMILARITY = "avg_similarity"

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
