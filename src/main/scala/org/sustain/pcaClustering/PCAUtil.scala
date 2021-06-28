package org.sustain.pcaClustering

import org.apache.spark.ml.feature.PCAModel
import org.apache.spark.ml.linalg.DenseVector

object PCAUtil {
  /**
   * Determines the minimum number of principal components needed to capture a target variance between observations.
   *
   * @param pca            The Spark PCA model that has been fit to the data.
   * @param targetVariance The target variance [0, 1.0] (suggested: 0.95) we wish to capture.
   * @return The number (K) of principle components which capture, for example, 95% of the variance between observations.
   */
  def getNoPrincipalComponentsByVariance(pca: PCAModel, targetVariance: Double): Int = {
    var n: Int = -1
    var varianceSum = 0.0
    val explainedVariance: DenseVector = pca.explainedVariance
    explainedVariance.foreachActive((index, variance) => {
      n = index + 1
      if (n >= pca.getK) {
        return n
      }
      varianceSum += variance
      if (varianceSum >= targetVariance) {
        return n
      }
    })

    pca.getK
  }

}
