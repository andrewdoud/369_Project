import org.apache.spark.SparkContext._

import scala.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.collection._
import scala.collection.immutable.HashMap
import scala.math.Ordered.orderingToOrdered

object DataPreprocess {
    def main(args: Array[String]) {
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        val conf = new SparkConf().setAppName("DataPreprocess").setMaster("local[4]")
        val sc = new SparkContext(conf)
        var encodingMap = HashMap("Male"->"0", "Female"->"1", "No"->"0", "Yes"->"0", "Other"->"-1", "Private"->"0",
            "Self-employed"->"1", "children"->"2", "Govt_job"->"3", "Never_worked"->"4", "never smoked"->"0",
            "Unknown"->"1", "formerly smoked"->"2", "smokes"->"3", "Urban"->"0", "Rural"->"1")

        // id,gender,age,hypertension,heart_disease,ever_married,work_type,Residence_type,avg_glucose_level,bmi,smoking_status,stroke
        val lines = sc.textFile("src/main/scala/healthcare-dataset-stroke-data.csv").map(x => x.split(",")).filter(_.head != "id").filter(x => x(9) != "N/A")
        val linesEncoded = lines.map(x => (x(0).toInt, encodingMap(x(1)), x(2).toDouble.toInt, x(3).toInt,
                                            x(4).toInt, encodingMap(x(5)), encodingMap(x(6)), encodingMap(x(7)),
                                            x(8).toDouble, x(9).toDouble, encodingMap(x(10))))
        val trainTest = linesEncoded.randomSplit(Array(0.8, 0.2)) // 80/20 train test split
        val train = trainTest(0).map(x => x.toString().slice(1, x.toString().length-1))
        val test = trainTest(1).map(x => x.toString().slice(1, x.toString().length-1))
        train.coalesce(1).saveAsTextFile("src/main/scala/train")
        test.coalesce(1).saveAsTextFile("src/main/scala/test")
    }
}
