package umich.cci.spark.sparkmonkey

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf;
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
object main {
  def main(args: Array[String]) {
    println(util.Properties.versionString)
    val conf = new SparkConf().setAppName("Spark Monkey").setMaster("local")
    val sc = new SparkContext(conf)
    val text_file = sc.textFile(this.getClass.getClassLoader.getResource("README.md").getPath, 2)
    val ret = text_file.flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey((a, b) => a+ b).collect()
    ret.foreach(wc => println("[" + wc._1 + "," + wc._2 + "]"))
  }
}