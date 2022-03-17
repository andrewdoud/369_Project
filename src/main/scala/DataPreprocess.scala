
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashMap

object DataPreprocess {
    def main(args: Array[String]) {
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        val conf = new SparkConf().setAppName("DataPreprocess").setMaster("local[4]")
        val sc = new SparkContext(conf)

        // id,gender,age,hypertension,heart_disease,ever_married,work_type,Residence_type,avg_glucose_level,bmi,smoking_status,stroke
        val lines = sc.textFile("src/main/scala/winequality-white.csv").map(x => x.split(";")).filter(_.head != "\"fixed acidity\"")
        val r = 1 to lines.count().toInt
        val idLines = r zip lines.collect()
        val linesEncoded = sc.parallelize(idLines).map(x => (x._1.toInt, x._2(0).toDouble, x._2(1).toDouble, x._2(2).toDouble,
            x._2(3).toDouble, x._2(4).toDouble, x._2(5).toDouble, x._2(6).toDouble, x._2(7).toDouble, x._2(8).toDouble,
            x._2(9).toDouble, x._2(10).toDouble, x._2(11).toInt))

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
        val col12Max = linesEncoded.map(x => x._12).max()

        val linesScaled = linesEncoded.map(x => (x._1, x._2/col2Max, x._3/col3Max, x._4/col4Max, x._5/col5Max,
            x._6/col6Max, x._7/col7Max, x._8/col8Max, x._9/col9Max, x._10/col10Max, x._11/col11Max, x._12/col12Max, x._13))
        linesScaled.foreach(println)

        val trainTest = linesScaled.randomSplit(Array(0.8, 0.2)) // 80/20 train test split
        val train = trainTest(0).map(x => x.toString().slice(1, x.toString().length-1))
        val test = trainTest(1).map(x => x.toString().slice(1, x.toString().length-1))
        train.coalesce(1).saveAsTextFile("src/main/scala/train")
        test.coalesce(1).saveAsTextFile("src/main/scala/test")
    }
}
