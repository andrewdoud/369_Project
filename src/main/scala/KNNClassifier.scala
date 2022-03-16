import scala.io._
import scala.math._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.collection._
import scala.collection.immutable.HashMap

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
        val num_neighbors = 1
        val trainLines = trainFile.map(x => x.split(",")).map(x => x.map(y => y.toDouble)).map(
            x => List(x(0).toInt, x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12).toInt))
        val groundTruth = trainFile.map(x => x.split(",")).map(x => List(x(0).toInt, x(12).toInt))
//        trainLines.foreach(println)
//        groundTruth.foreach(println)
        // Generating matrix of all tuples
        val allLines = trainLines.cartesian(trainLines).filter({case (tup1, tup2) => tup1(0) != tup2(0)})
        println("All Lines generated")

        // Calculating euclidean distances from the matrix
        val distances = allLines.map({case (x, y) => (x(0), (y(12), euclidean_distance(x.slice(1,12), y.slice(1,12))))})

        // Finding k nearest neighbors for a tuple and returning their ground truth values
        val nearest_neighbors = distances.groupByKey()
            .map({case (k, v) => (k, v.toList.sortBy(x => x._2)(Ordering[Double].reverse).take(num_neighbors))})
            .mapValues(v => v.map(x => x._1))

        nearest_neighbors.collect().foreach(println)

        // Finding the plurality of the ground truth values for a tuple
        val pluralities = nearest_neighbors.map({case (k, v) => (k, v.groupBy(x => x).mapValues(_.size).maxBy(_._2)._1)})
        pluralities.collect().foreach(println)

        // pluralities output:
        // (7, 0)
        // (54, 1)
        // (12, 1)
        // ...
        // (18782, 0)

        // Compare ground truth and pluralities output

    }
}
