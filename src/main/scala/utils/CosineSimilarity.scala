package utils

import scala.language.postfixOps

object CosineSimilarity {

  /*
   * This method takes 2 equal length arrays of integers
   * It returns a double representing similarity of the 2 arrays
   * 0.9925 would be 99.25% similar
   * (x dot y)/||X|| ||Y||
   */
  def cosineSimilarity(x: Array[Int], y: Array[Int]): Double = {
    require(x.size == y.size)
    if(magnitude(x) == 0.0){
      0.0
    }else{
    dotProduct(x, y)/(magnitude(x) * magnitude(y))
    }
  }

  /*
   * Return the dot product of the 2 arrays
   * e.g. (a[0]*b[0])+(a[1]*a[2])
   */
  def dotProduct(x: Array[Int], y: Array[Int]): Int = {
    (for((a, b) <- x zip y) yield a * b) sum
  }

  /*
   * Return the magnitude of an array
   * We multiply each element, sum it, then square root the result.
   */
  def magnitude(x: Array[Int]): Double = {
    math.sqrt(x map(i => i*i) sum)
  }


}
