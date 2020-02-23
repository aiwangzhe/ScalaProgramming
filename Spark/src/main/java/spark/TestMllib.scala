package spark

import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.{Matrices, Vectors}

object TestMllib {

  def main(args: Array[String]): Unit = {
    val denseVector = Vectors.dense(Array(1.0, 2.0, 3.0));
    print(denseVector)
    val labelPoint = new LabeledPoint(2, denseVector);
    print(labelPoint)

  }
}
