// spark/examples/ml

// scalastyle:off println

import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
// $example off$
import org.apache.spark.sql.{DataFrame, SQLContext}

// $example on$
// Crates a DataFrame
val dataset: DataFrame = sqlContext.createDataFrame(Seq(
  (1, Vectors.dense(0.0, 0.0, 0.0)),
  (2, Vectors.dense(0.1, 0.1, 0.1)),
  (3, Vectors.dense(0.2, 0.2, 0.2)),
  (4, Vectors.dense(9.0, 9.0, 9.0)),
  (5, Vectors.dense(9.1, 9.1, 9.1)),
  (6, Vectors.dense(9.2, 9.2, 9.2))
)).toDF("id", "features")

// Trains a k-means model
val kmeans = new KMeans().setK(2).setFeaturesCol("features").setPredictionCol("prediction")
val model = kmeans.fit(dataset)

// Shows the result
println("Final Centers: ")
model.clusterCenters.foreach(println)
