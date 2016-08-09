import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.lang.Math

object TestApp {
	def main(args: Array[String]) {
		val logFile = "/home/ubuntu/spark-2.0.0/README.md"
		val conf = new SparkConf().setAppName("Test Spark App")
		val sc = new SparkContext(conf)
		val logData = sc.textFile(logFile, 2).cache()
		val numAs = logData.filter(line => line.contains("a")).count()
		val numBs = logData.filter(line => line.contains("b")).count()
		
		println("Lines with a: %s, lines with b: %s".format(numAs, numBs))
		
		val wordCount = logFile.map(line => line.split(" ").size)
		wordCount = wordCount.reduce((a, b) => Math.max(a, b))
		
		println("Max word count of line: %s".format(wordCount)))
		
		val allWordCount = logFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
		println("All word Count of document: %s".format(allWordCount))
	}
}