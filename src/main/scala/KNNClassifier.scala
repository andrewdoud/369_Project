import scala.math._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object KNNClassifier {

    def euclidean_distance(row1: List[Double], row2: List[Double]): Double = {
        val zippedRDD = row1 zip row2
        val zippedResidualsSquared = zippedRDD.map(x => math.pow((x._1 - x._2), 2))
        val distance = sqrt(zippedResidualsSquared.sum)
        return distance
    }

    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        val conf = new SparkConf().setAppName("DataPreprocess").setMaster("local[4]")
        val sc = new SparkContext(conf)

        val trainFile = sc.textFile("src/main/scala/train/part-00000")
        val testFile = sc.textFile("src/main/scala/test/part-00000")

        val trainLines = trainFile.map(x => x.split(",")).map(x => x.map(y => y.toDouble)).map(
            x => List(x(0).toInt, x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12).toInt))
        val testLines = testFile.map(x => x.split(",")).map(x => x.map(y => y.toDouble)).map(
          x => List(x(0).toInt, x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12).toInt))

        val NUM_NEIGHBORS = 50

        val groundTruth = testFile.map(x => x.split(",")).map(x => (x(0).toDouble, x(12).toDouble))
        println("Ground truth")
        groundTruth.foreach(println)

        // Generating matrix of all tuples
        val allLines = testLines.cartesian(trainLines)
        println("All Lines generated")

        // Calculating euclidean distances from the matrix
        val distances = allLines.map({case (x, y) =>
            (x(0), (y(12), euclidean_distance(x.slice(1,12), y.slice(1,12))))})

        // Finding k nearest neighbors for a tuple and returning their ground truth values
        val nearest_neighbors = distances.groupByKey()
            .map({case (k, v) => (k, v.toList.sortBy(_._2).take(NUM_NEIGHBORS))})
            .mapValues(v => v.map(_._1))

        // Finding the plurality of the ground truth values for a tuple
        val pluralities = nearest_neighbors.map({case (k, v) =>
            (k, v.groupBy(x => x).mapValues(_.size).maxBy(_._2)._1)})

        val merged = groundTruth.join(pluralities)
        val accuracy = merged.filter(x => x._2._1 == x._2._2).count().toDouble / merged.count().toDouble * 100
        println(f"Accuracy: $accuracy")
    }
}
