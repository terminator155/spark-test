import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object TestApp {
	def main(args: Array[String]) {
		val logFine = "~/spark-2.0.0/README.md"
		val conf = new SparkConf().setAppName("Test Spark App")
		val sc = new SparkContext(conf)
		val logData = sc.textFile(logFile, 2).cache()
		val numAs = logData.filter(line => line.contains("a")).count()
		val numBs = logData.filter(line => line.contains("b")).count()
		
		println("Lines with a: %s, lines with b: %s".format(numAs, numBs))
	}
}