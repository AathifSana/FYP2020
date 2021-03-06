package utils

import org.apache.spark.ml.fpm.FPGrowthModel
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

import scala.language.existentials

/*Since the FP Growth of the Spark ml library does not sort the predictions according to its confidence,
  This class intends to fullfil that.
  predictWithConfidence method follows the same steps as the library, but confidence is also included
  getSortedRecs method sorts accoording to the confidence and displays only the consequents
  dropEmptyRecommendations drops the records with empty predictions
*/
class FPGrowthCalculations(dataset: DataFrame){


  private def dropEmptyRecommendations(model: FPGrowthModel) = {
    val filteredDataset = dataset.filter(_.getAs[Seq[Any]](model.getPredictionCol).nonEmpty)
    FPGrowthCalculations(filteredDataset)
  }


  private def getSortedRecs(model: FPGrowthModel)= {
    val previousDatatype = dataset.schema(model.getItemsCol).dataType

    val sortedPredictions = dataset.withColumn(model.getPredictionCol,{
      val sortResultUdf = udf((res: Seq[Row])=>{
        res.sortBy(- _.getDouble(1)).map(_.getString(0)).distinct
      })
      sortResultUdf(col(model.getPredictionCol))
    })

    FPGrowthCalculations(sortedPredictions)
  }

  private def predictWithConfidence(model : FPGrowthModel) = {
    model.transformSchema(dataset.schema)

    val changedRules = model.associationRules.withColumn("consequent", {
      val concatSeq = udf((consequent : Seq[Any] , confidence: Double)=> {
        consequent.map{p => (p.toString,confidence)}
      })
      concatSeq(col("consequent"),col("confidence"))
    })

    val rules: Array[(Seq[Any], Seq[Any])] = changedRules.select("antecedent", "consequent")
      .rdd.map(r => (r.getSeq(0), r.getSeq(1)))
      .collect().asInstanceOf[Array[(Seq[Any], Seq[Any])]]
    val brRules = dataset.sparkSession.sparkContext.broadcast(rules)

    val predictUDF = udf((items: Seq[Any]) => {
      if (items != null) {
        val itemset = items.map(item => item.toString).toSet
        brRules.value.flatMap(rule =>
          if (items != null && rule._1.forall(item => itemset.contains(item.toString))) {
            rule._2.filter { cons =>
              cons match {
                case Row(consequent: String, confidence: Double) => !itemset.contains(consequent)
              }
            }
          } else {
            Seq.empty
          }).distinct
      } else {
        Seq.empty
      }},ArrayType.apply(StructType.apply(Seq(StructField.apply("consequent",StringType),StructField.apply("confidence",DoubleType)))))

    val predictions = dataset.withColumn(model.getPredictionCol, predictUDF(col(model.getItemsCol)))

    FPGrowthCalculations(predictions)
  }


  def transform(model: FPGrowthModel)= {
    predictWithConfidence(model)
      .getSortedRecs(model)
      .dropEmptyRecommendations(model)
  }

  def getPredictedDataset = dataset
}


object FPGrowthCalculations {

  def apply(dataset: DataFrame) : FPGrowthCalculations = {
    new FPGrowthCalculations(dataset)
  }


}
