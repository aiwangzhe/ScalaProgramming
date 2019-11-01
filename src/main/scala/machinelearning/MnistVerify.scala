package machinelearning

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object MnistVerify {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[4]").appName("MnistVerify").getOrCreate()
    val dataFrame = sparkSession.read.format("libsvm").load("train-data")

    // Split the data into train and test
    val splits = dataFrame.randomSplit(Array[Double](0.9, 0.1), 1234L)
    val train = splits(0)
    val test = splits(1)

    // specify layers for the neural network:
    // input layer of size 4 (features), two intermediate of size 5 and 4
    // and output of size 3 (classes)
    val layers = Array[Int](784, 800, 400, 10)

    // create the trainer and set its parameters
    val trainer = new MultilayerPerceptronClassifier().
      setLayers(layers).setBlockSize(32).
      setSolver("gd").
      setStepSize(0.1).
      setSeed(1234L).setMaxIter(20)

    // train the model
    val model = trainer.fit(train)

    // compute accuracy on the test set
    val result = model.transform(test)

    val predictionAndLabels = result.select("prediction", "label")
    predictionAndLabels.foreach(row => {
      System.out.println("row: " + row)
    })
    val evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy")

    System.out.println("Test set accuracy = " + evaluator.evaluate(predictionAndLabels))
    // $example off$

    sparkSession.stop()
  }
}
