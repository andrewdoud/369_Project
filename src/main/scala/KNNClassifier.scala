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
//        List((1,A), (2,B), (3,C), (4,D))

        val zippedResidualsSquared = zippedRDD.map(x => math.pow((x._1 - x._2), 2))
        val distance = sqrt(zippedResidualsSquared.sum)
        return distance
    }

    def get_nearest_neighbors(train: RDD[(List[Double], List[Double])], test_row: List[Double], num_neighbors: Int): Array[Double] = {
        println("in got nearest")
        val train_wo_test = train.filter({case (tup1, tup2) => tup1(0) != tup2(0)}) //.filter(x => x(0) != test_row(0))
        println("before distances")
        val distances = train_wo_test.map({case (x, y) => (x, euclidean_distance(x, y))})
        val nearest = distances.sortBy(x => x._2, false).take(num_neighbors)

        return nearest.map(x => x._1.head)
    }

    def predict_classification(ground_truth: RDD[(Double, Double)], nearest_neighbors: Array[Double])
//        (train: RDD[List[Double]], ground_truth: RDD[(Double, Double)], test_row: List[Double], num_neighbors: Int): RDD[Double] =
    {
        println("in predict classification")
//        val nearest_neighbors = get_nearest_neighbors(train, test_row, num_neighbors)
        println("Got nearest neighbors")

        val neighbor_classification = ground_truth.filter(x => nearest_neighbors.contains(x._1)).map(x => x._2)
//        val neighbor_classification = ground_truth.join(nearest_neighbors).map({case (k, (v1, v2)) => v1})

        neighbor_classification.foreach(println)
        println("Reached")
        neighbor_classification
//        val plurality_class = neighbor_classification
//        (plurality_class.keys)
//
    }

//    def predict_classification2(v)

//    def predict_classification(train, test_row, num_neighbors):
    //    neighbors = get_neighbors(train, test_row, num_neighbors)
    //    output_values = [row[-1] for row in neighbors]
    //    prediction = max(set(output_values), key=output_values.count)
    //    return prediction

    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        val conf = new SparkConf().setAppName("DataPreprocess").setMaster("local[4]")
        val sc = new SparkContext(conf)

        val trainFile = sc.textFile("src/main/scala/train/part-00000")
        val num_neighbors = 1

        val trainLines = trainFile.map(x => x.split(",")).map(x => List(x(0).toInt, x(1).toDouble, x(2).toDouble, x(3).toDouble,
            x(4).toDouble, x(5).toDouble, x(6).toDouble, x(7).toDouble, x(8).toDouble, x(9).toDouble, x(10).toDouble, x(11).toDouble))
        val groundTruth = trainFile.map(x => x.split(",")).map(x => (x(0).toInt, (x(11).toDouble)))
        trainLines.foreach(println)
        groundTruth.foreach(println)

        val allLines = trainLines.cartesian(trainLines).filter({case (tup1, tup2) => tup1(0) != tup2(0)})
        println("All Lines generated")

        val distances = allLines.map({case (x, y) => (x(0), (y(11), euclidean_distance(x.init, y.init)))})

        println("Done with calculating distances")
//        distances.collect().foreach(println)
//        (1, [(1, 82347.3), (0, 1231.3)])
//        (1, [(1, 1231.3)]
        val nearest_neighbors = distances.groupByKey()
            .map({case (k, v) => (k, v.toList.sortBy(x => x._2)(Ordering[Double].reverse).take(num_neighbors))})
            .mapValues(v => v.map(x => x._1))

        println("Done finding enarest neighbors")
        nearest_neighbors.collect().foreach(println)

//      (1, [1, 0, 1])
//      (1, [(13, 1), (12, 0), (15, 1)])

        val pluralities = nearest_neighbors.map({case (k, v) => (k, v.groupBy(x => x).mapValues(_.size).maxBy(_._2)._1)})
        pluralities.collect().foreach(println)

//        return nearest.map(x => x._1.head)

//        val distances = trainLines.map(x => trainLines.filter(y => x(0) != y(0)).map(y => euclidean_distance(x, y)))
//        distances.collect.foreach(println)
        
//        println(sc)
////        println(trainLines)
//        println(groundTruth)
////        val x = trainLines.map(x => predict_classification(trainLines, groundTruth, x, 3))
////        x.foreach(println)
////        trainLines.
//
//        val y = trainLines.map(x => get_nearest_neighbors(trainLines, x, num_neighbors = 3))
//          .map(x => predict_classification(groundTruth, x))
//
//        y.collect().foreach(println)

//        val euclidean = trainLines.map(x => )
    }
}
