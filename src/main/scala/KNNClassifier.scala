import scala.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.collection._
import scala.collection.immutable.HashMap
import scala.math.Ordered.orderingToOrdered

object KNNClassifier {
    def euclidean_distance(sc: SparkContext, row1: List[Double], row2: List[Double]): Double = {
        val rdd1 = sc.parallelize(row1)
        val rdd2 = sc.parallelize(row2)
        val zippedRDD = rdd1 zip rdd2
        val zippedResidualsSquared = zippedRDD.map(x => (0, (x._1 - x._2) * (x._1 - x._2)))
        val distance = zippedResidualsSquared.reduceByKey((a, b) => a + b).collect()(0)._2
        return distance
    }

    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        val conf = new SparkConf().setAppName("DataPreprocess").setMaster("local[4]")
        val sc = new SparkContext(conf)
        val trainLines = sc.textFile("src/main/scala/train/part-00000").map(x => x.split(",")).map(x => List(x(0).toDouble, x(1).toDouble, x(2).toDouble, x(3).toDouble,
            x(4).toDouble, x(5).toDouble, x(6).toDouble, x(7).toDouble, x(8).toDouble, x(9).toDouble, x(10).toDouble))
        trainLines.foreach(println)
    }
}
