
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashMap

object DataPreprocess {
    def main(args: Array[String]) {
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        val conf = new SparkConf().setAppName("DataPreprocess").setMaster("local[4]")
        val sc = new SparkContext(conf)
        var encodingMap = HashMap("Male"->"0", "Female"->"1", "No"->"0", "Yes"->"1", "Other"->"-1", "Private"->"0",
            "Self-employed"->"1", "children"->"2", "Govt_job"->"3", "Never_worked"->"4", "never smoked"->"0",
            "Unknown"->"1", "formerly smoked"->"2", "smokes"->"3", "Urban"->"0", "Rural"->"1")

        // id,gender,age,hypertension,heart_disease,ever_married,work_type,Residence_type,avg_glucose_level,bmi,smoking_status,stroke
        val lines = sc.textFile("src/main/scala/winequality-white.csv").map(x => x.split(";")).filter(_.head != "\"fixed acidity\"")
        val linesEncoded = lines.map(x => x.map(y => y.toDouble)).map(x => (x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11)))

        val col1Max = linesEncoded.map(x => x._1).max()
        val col2Max = linesEncoded.map(x => x._2).max()
        val col3Max = linesEncoded.map(x => x._3).max()
        val col4Max = linesEncoded.map(x => x._4).max()
        val col5Max = linesEncoded.map(x => x._5).max()
        val col6Max = linesEncoded.map(x => x._6).max()
        val col7Max = linesEncoded.map(x => x._7).max()
        val col8Max = linesEncoded.map(x => x._8).max()
        val col9Max = linesEncoded.map(x => x._9).max()
        val col10Max = linesEncoded.map(x => x._10).max()
        val col11Max = linesEncoded.map(x => x._11).max()

        val linesScaled = linesEncoded.map(x => (x._1/col1Max, x._2/col2Max, x._3/col3Max, x._4/col4Max, x._5/col5Max,
            x._6/col6Max, x._7/col7Max, x._8/col8Max, x._9/col9Max, x._10/col10Max, x._11/col11Max, x._12.toInt))
        linesScaled.foreach(println)

        val trainTest = linesEncoded.randomSplit(Array(0.8, 0.2)) // 80/20 train test split
        val train = trainTest(0).map(x => x.toString().slice(1, x.toString().length-1))
        val test = trainTest(1).map(x => x.toString().slice(1, x.toString().length-1))
        train.coalesce(1).saveAsTextFile("src/main/scala/train")
        test.coalesce(1).saveAsTextFile("src/main/scala/test")
    }
}
